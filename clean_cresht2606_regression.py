#!/usr/bin/env python3
import argparse
import json
from pathlib import Path
from typing import Dict, List, Tuple

import numpy as np
import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.linear_model import Ridge
from sklearn.metrics import mean_absolute_error, mean_squared_error
from sklearn.model_selection import KFold, cross_val_predict
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder


TARGET_COL = "price_million_vnd"
NUMERIC_FEATURES = ["area_m2", "bedrooms", "bathrooms", "floors", "timeline_hours"]
CATEGORICAL_FEATURES = ["location", "frontage"]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Clean cresht2606 raw data using regression residual-based outlier flags."
    )
    parser.add_argument(
        "--input-dir",
        default="data/raw/cresht2606_vietnam-real-estate-datasets-catalyst",
        help="Directory containing cresht2606 CSV files.",
    )
    parser.add_argument(
        "--output-dir",
        default="data/processed",
        help="Directory to save cleaned outputs.",
    )
    parser.add_argument(
        "--n-splits",
        type=int,
        default=5,
        help="KFold splits for cross-validated predictions.",
    )
    parser.add_argument(
        "--z-threshold",
        type=float,
        default=4.0,
        help="Absolute robust-z threshold on log-price residuals.",
    )
    parser.add_argument(
        "--ape-threshold",
        type=float,
        default=0.75,
        help="Absolute percentage error threshold for model outlier flag.",
    )
    parser.add_argument(
        "--min-train-rows",
        type=int,
        default=1000,
        help="Minimum valid rows to run model-based outlier detection.",
    )
    parser.add_argument(
        "--min-category-frequency",
        type=float,
        default=0.005,
        help="Minimum category frequency for OneHotEncoder.",
    )
    parser.add_argument(
        "--random-state",
        type=int,
        default=42,
        help="Random seed.",
    )
    return parser.parse_args()


def load_csv(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path, encoding="utf-8-sig")
    renamed = {}
    for col in df.columns:
        if col.startswith("\ufeff"):
            renamed[col] = col.replace("\ufeff", "")
    if renamed:
        df = df.rename(columns=renamed)

    df["source_file"] = path.name
    return df


def to_numeric_columns(df: pd.DataFrame, columns: List[str]) -> pd.DataFrame:
    out = df.copy()
    for col in columns:
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce")
    return out


def build_model_pipeline(
    numeric_cols: List[str], categorical_cols: List[str], min_category_frequency: float
) -> Pipeline:
    preprocess = ColumnTransformer(
        transformers=[
            (
                "num",
                Pipeline(
                    steps=[
                        ("imputer", SimpleImputer(strategy="median")),
                    ]
                ),
                numeric_cols,
            ),
            (
                "cat",
                Pipeline(
                    steps=[
                        (
                            "onehot",
                            OneHotEncoder(
                                handle_unknown="ignore",
                                min_frequency=min_category_frequency,
                                sparse_output=True,
                            ),
                        ),
                    ]
                ),
                categorical_cols,
            ),
        ],
        remainder="drop",
    )

    model = Ridge(alpha=1.0)
    return Pipeline(steps=[("preprocess", preprocess), ("model", model)])


def compute_model_outliers(
    df: pd.DataFrame,
    n_splits: int,
    z_threshold: float,
    ape_threshold: float,
    min_train_rows: int,
    min_category_frequency: float,
    random_state: int,
) -> Tuple[pd.DataFrame, Dict[str, float]]:
    out = df.copy()
    out["cv_pred_price_million_vnd"] = np.nan
    out["residual_log_price"] = np.nan
    out["robust_z_log_residual"] = np.nan
    out["ape"] = np.nan
    out["flag_model_outlier"] = False

    if TARGET_COL not in out.columns:
        return out, {
            "model_applied": False,
            "reason": f"missing target column: {TARGET_COL}",
        }

    target = out[TARGET_COL]
    valid_target_mask = target.notna() & (target > 0)

    numeric_cols = [col for col in NUMERIC_FEATURES if col in out.columns]
    categorical_cols = [col for col in CATEGORICAL_FEATURES if col in out.columns]
    feature_cols = numeric_cols + categorical_cols

    if not feature_cols:
        return out, {
            "model_applied": False,
            "reason": "no supported feature columns found",
        }

    train_idx = out.index[valid_target_mask]
    if len(train_idx) < max(min_train_rows, 50):
        return out, {
            "model_applied": False,
            "reason": f"insufficient valid target rows: {len(train_idx)}",
        }

    X_train = out.loc[train_idx, feature_cols]
    if categorical_cols:
        X_train = X_train.copy()
        for col in categorical_cols:
            X_train[col] = X_train[col].astype("string").fillna("__MISSING__")

    y_train = out.loc[train_idx, TARGET_COL].astype(float).to_numpy()
    y_train_log = np.log1p(y_train)

    splits = min(max(2, n_splits), len(train_idx))
    cv = KFold(n_splits=splits, shuffle=True, random_state=random_state)
    pipeline = build_model_pipeline(
        numeric_cols=numeric_cols,
        categorical_cols=categorical_cols,
        min_category_frequency=min_category_frequency,
    )

    pred_log = cross_val_predict(pipeline, X_train, y_train_log, cv=cv, n_jobs=-1, method="predict")
    pred_price = np.expm1(pred_log).clip(min=0)
    residual_log = y_train_log - pred_log

    residual_median = float(np.median(residual_log))
    mad = float(np.median(np.abs(residual_log - residual_median)))
    if mad <= 1e-12:
        robust_z = np.zeros_like(residual_log)
    else:
        robust_z = 0.6745 * (residual_log - residual_median) / mad

    ape = np.abs(pred_price - y_train) / np.maximum(y_train, 1.0)
    model_outlier_mask = (np.abs(robust_z) > z_threshold) & (ape > ape_threshold)

    out.loc[train_idx, "cv_pred_price_million_vnd"] = pred_price
    out.loc[train_idx, "residual_log_price"] = residual_log
    out.loc[train_idx, "robust_z_log_residual"] = robust_z
    out.loc[train_idx, "ape"] = ape
    out.loc[train_idx, "flag_model_outlier"] = model_outlier_mask

    stats = {
        "model_applied": True,
        "n_train_rows": int(len(train_idx)),
        "n_features": int(len(feature_cols)),
        "n_numeric_features": int(len(numeric_cols)),
        "n_categorical_features": int(len(categorical_cols)),
        "cv_splits": int(splits),
        "mae_log_price": float(mean_absolute_error(y_train_log, pred_log)),
        "rmse_log_price": float(np.sqrt(mean_squared_error(y_train_log, pred_log))),
        "mean_ape": float(np.mean(ape)),
        "median_ape": float(np.median(ape)),
        "mad_log_residual": float(mad),
        "residual_median": float(residual_median),
        "model_outlier_count": int(np.sum(model_outlier_mask)),
        "model_outlier_rate_pct": float(np.mean(model_outlier_mask) * 100.0),
    }
    return out, stats


def add_rule_flags(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()

    if TARGET_COL in out.columns:
        out["flag_price_missing_or_nonpositive"] = out[TARGET_COL].isna() | (out[TARGET_COL] <= 0)
    else:
        out["flag_price_missing_or_nonpositive"] = True

    if "area_m2" in out.columns:
        out["flag_area_nonpositive"] = out["area_m2"].notna() & (out["area_m2"] <= 0)
    else:
        out["flag_area_nonpositive"] = False

    room_flags = []
    for col in ["bedrooms", "bathrooms", "floors"]:
        if col in out.columns:
            flag_col = f"flag_{col}_negative"
            out[flag_col] = out[col].notna() & (out[col] < 0)
            room_flags.append(flag_col)

    if room_flags:
        out["flag_rooms_negative_any"] = out[room_flags].any(axis=1)
    else:
        out["flag_rooms_negative_any"] = False

    out["flag_invalid_basic"] = (
        out["flag_price_missing_or_nonpositive"]
        | out["flag_area_nonpositive"]
        | out["flag_rooms_negative_any"]
    )
    out["flag_drop"] = out["flag_invalid_basic"] | out["flag_model_outlier"]
    out["is_clean"] = ~out["flag_drop"]
    return out


def summarize_file(df: pd.DataFrame, model_stats: Dict[str, float], source_file: str) -> Dict[str, object]:
    total_rows = int(len(df))
    clean_rows = int(df["is_clean"].sum())
    dropped_rows = total_rows - clean_rows

    summary = {
        "source_file": source_file,
        "total_rows": total_rows,
        "clean_rows": clean_rows,
        "dropped_rows": dropped_rows,
        "dropped_rate_pct": float((dropped_rows * 100.0 / total_rows) if total_rows else 0.0),
        "drop_reason_counts": {
            "invalid_basic": int(df["flag_invalid_basic"].sum()),
            "model_outlier": int(df["flag_model_outlier"].sum()),
            "price_missing_or_nonpositive": int(df["flag_price_missing_or_nonpositive"].sum()),
            "area_nonpositive": int(df["flag_area_nonpositive"].sum()),
            "rooms_negative_any": int(df["flag_rooms_negative_any"].sum()),
        },
        "model_stats": model_stats,
    }
    return summary


def main() -> int:
    args = parse_args()
    input_dir = Path(args.input_dir).resolve()
    output_dir = Path(args.output_dir).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    csv_files = sorted(input_dir.glob("*.csv"))
    if not csv_files:
        raise FileNotFoundError(f"No CSV files found in: {input_dir}")

    flagged_parts = []
    cleaned_parts = []
    file_summaries = []

    for csv_path in csv_files:
        raw_df = load_csv(csv_path)
        working_df = to_numeric_columns(raw_df, NUMERIC_FEATURES + [TARGET_COL])

        flagged_df, model_stats = compute_model_outliers(
            df=working_df,
            n_splits=args.n_splits,
            z_threshold=args.z_threshold,
            ape_threshold=args.ape_threshold,
            min_train_rows=args.min_train_rows,
            min_category_frequency=args.min_category_frequency,
            random_state=args.random_state,
        )
        flagged_df = add_rule_flags(flagged_df)

        clean_df = flagged_df[flagged_df["is_clean"]].copy()

        base_cols = [col for col in raw_df.columns if col in flagged_df.columns]
        clean_export_cols = list(dict.fromkeys(base_cols + ["source_file"]))

        clean_path = output_dir / f"{csv_path.stem}.cleaned.csv"
        flags_path = output_dir / f"{csv_path.stem}.flags.csv"
        clean_df.to_csv(clean_path, index=False, columns=clean_export_cols)
        flagged_df.to_csv(flags_path, index=False)

        flagged_parts.append(flagged_df)
        cleaned_parts.append(clean_df[clean_export_cols].copy())
        file_summaries.append(summarize_file(flagged_df, model_stats, csv_path.name))

    all_flags = pd.concat(flagged_parts, ignore_index=True)
    all_clean = pd.concat(cleaned_parts, ignore_index=True)

    combined_flags_path = output_dir / "cresht2606_regression_flags.csv"
    combined_clean_path = output_dir / "cresht2606_regression_cleaned.csv"
    summary_path = output_dir / "cresht2606_regression_cleaning_summary.json"

    all_flags.to_csv(combined_flags_path, index=False)
    all_clean.to_csv(combined_clean_path, index=False)

    overall_total = int(len(all_flags))
    overall_clean = int(all_flags["is_clean"].sum())
    overall_dropped = overall_total - overall_clean

    summary = {
        "input_dir": str(input_dir),
        "output_dir": str(output_dir),
        "config": {
            "n_splits": args.n_splits,
            "z_threshold": args.z_threshold,
            "ape_threshold": args.ape_threshold,
            "min_train_rows": args.min_train_rows,
            "min_category_frequency": args.min_category_frequency,
            "random_state": args.random_state,
        },
        "overall": {
            "total_rows": overall_total,
            "clean_rows": overall_clean,
            "dropped_rows": overall_dropped,
            "dropped_rate_pct": float((overall_dropped * 100.0 / overall_total) if overall_total else 0.0),
            "drop_reason_counts": {
                "invalid_basic": int(all_flags["flag_invalid_basic"].sum()),
                "model_outlier": int(all_flags["flag_model_outlier"].sum()),
                "price_missing_or_nonpositive": int(all_flags["flag_price_missing_or_nonpositive"].sum()),
                "area_nonpositive": int(all_flags["flag_area_nonpositive"].sum()),
                "rooms_negative_any": int(all_flags["flag_rooms_negative_any"].sum()),
            },
        },
        "files": file_summaries,
        "outputs": {
            "combined_cleaned_csv": str(combined_clean_path),
            "combined_flags_csv": str(combined_flags_path),
            "per_file_cleaned_csv_pattern": str(output_dir / "*.cleaned.csv"),
            "per_file_flags_csv_pattern": str(output_dir / "*.flags.csv"),
        },
    }

    with open(summary_path, "w", encoding="utf-8") as file_obj:
        json.dump(summary, file_obj, ensure_ascii=False, indent=2)

    print("Regression-based cleaning completed.")
    print(f"- Combined cleaned: {combined_clean_path}")
    print(f"- Combined flags:   {combined_flags_path}")
    print(f"- Summary JSON:     {summary_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
