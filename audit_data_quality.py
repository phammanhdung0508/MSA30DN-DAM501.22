#!/usr/bin/env python3
import argparse
import csv
import json
import math
import os
import re
from collections import Counter
from statistics import mean
from typing import Dict, List, Optional, Set, Tuple


CANONICAL_COLUMNS = [
    "id",
    "detail_url",
    "title",
    "location",
    "timeline_hours",
    "area_m2",
    "bedrooms",
    "bathrooms",
    "floors",
    "frontage",
    "price_million_vnd",
]

NUMERIC_COLUMNS = [
    "timeline_hours",
    "area_m2",
    "bedrooms",
    "bathrooms",
    "floors",
    "price_million_vnd",
]

OUTLIER_RULES = {
    "timeline_gt_1y": {
        "field": "timeline_hours",
        "check": lambda value, stale_hours: value is not None and value > stale_hours,
    },
    "area_gt_10000": {
        "field": "area_m2",
        "check": lambda value, stale_hours: value is not None and value > 10000,
    },
    "bedrooms_gt_20": {
        "field": "bedrooms",
        "check": lambda value, stale_hours: value is not None and value > 20,
    },
    "bathrooms_gt_20": {
        "field": "bathrooms",
        "check": lambda value, stale_hours: value is not None and value > 20,
    },
    "floors_gt_30": {
        "field": "floors",
        "check": lambda value, stale_hours: value is not None and value > 30,
    },
    "price_gt_200000": {
        "field": "price_million_vnd",
        "check": lambda value, stale_hours: value is not None and value > 200000,
    },
    "price_eq_0": {
        "field": "price_million_vnd",
        "check": lambda value, stale_hours: value is not None and value == 0,
    },
}

COMMERCIAL_PATTERNS = [
    r"\bkho\b",
    r"xưởng",
    r"\bxuong\b",
    r"văn phòng",
    r"van phong",
    r"mặt bằng",
    r"\bmbkd\b",
    r"nhà xưởng",
    r"nha xuong",
]

UNKNOWN_PRICE_KEYWORDS = [
    "thỏa thuận",
    "thoả thuận",
    "thoa thuan",
    "liên hệ",
    "lien he",
    "xem giá",
    "xem gia",
    "đang cập nhật",
    "dang cap nhat",
]

STATUS_ORDER = {
    "pass": 0,
    "partial": 1,
    "fail": 2,
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Audit CSV real-estate data quality across key criteria."
    )
    parser.add_argument(
        "--input-dir",
        default="data/raw/cresht2606_vietnam-real-estate-datasets-catalyst",
        help="Directory that contains CSV files to audit.",
    )
    parser.add_argument(
        "--stale-hours",
        type=int,
        default=8760,
        help="Threshold for old records in hours (default: 1 year).",
    )
    parser.add_argument(
        "--sample-limit",
        type=int,
        default=3,
        help="How many sample rows to print for each outlier rule.",
    )
    parser.add_argument(
        "--output-json",
        default="",
        help="Optional path to save full audit report as JSON.",
    )
    return parser.parse_args()


def status_from_rate(value_pct: float, pass_max: float, partial_max: float) -> str:
    if value_pct <= pass_max:
        return "pass"
    if value_pct <= partial_max:
        return "partial"
    return "fail"


def combine_status(current: str, candidate: str) -> str:
    if STATUS_ORDER[candidate] > STATUS_ORDER[current]:
        return candidate
    return current


def clean_text(value: object) -> str:
    text = "" if value is None else str(value)
    text = text.replace("\ufeff", "")
    text = text.replace("\r", " ").replace("\n", " ")
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def clean_location(value: object) -> str:
    text = clean_text(value)
    text = text.replace("·", " ")
    text = re.sub(r"\s+", " ", text)
    text = re.sub(r"\s*,\s*", ", ", text)
    text = text.strip(" ,")
    return text


def to_float(value: object) -> Optional[float]:
    text = clean_text(value)
    if text == "" or text.lower() == "nan":
        return None
    try:
        number = float(text)
    except ValueError:
        return None
    if math.isnan(number):
        return None
    return number


def parse_locale_number(text: str) -> Optional[float]:
    raw = clean_text(text)
    if not raw:
        return None

    number = re.sub(r"[^0-9,\.\-]", "", raw)
    if number in {"", "-", ".", ","}:
        return None

    if "." in number and "," in number:
        if number.rfind(",") > number.rfind("."):
            number = number.replace(".", "")
            number = number.replace(",", ".")
        else:
            number = number.replace(",", "")
    elif "," in number:
        if number.count(",") > 1:
            number = number.replace(",", "")
        else:
            left, right = number.split(",", 1)
            if len(right) in {1, 2}:
                number = f"{left}.{right}"
            elif len(right) == 3 and len(left) >= 1:
                number = f"{left}{right}"
            else:
                number = f"{left}{right}"
    elif "." in number:
        if number.count(".") > 1:
            number = number.replace(".", "")
        else:
            left, right = number.split(".", 1)
            if len(right) == 3 and len(left) >= 1:
                number = f"{left}{right}"

    try:
        return float(number)
    except ValueError:
        return None


def parse_area_text_to_m2(text: str) -> Optional[float]:
    raw = clean_text(text).lower()
    if not raw:
        return None

    hectare_match = re.search(r"([0-9][0-9\.,]*)\s*ha\b", raw)
    if hectare_match:
        value = parse_locale_number(hectare_match.group(1))
        if value is not None:
            return value * 10000.0

    dims_match = re.search(r"([0-9][0-9\.,]*)\s*[x\*]\s*([0-9][0-9\.,]*)", raw)
    if dims_match:
        side_a = parse_locale_number(dims_match.group(1))
        side_b = parse_locale_number(dims_match.group(2))
        if side_a is not None and side_b is not None:
            return side_a * side_b

    number_match = re.search(r"([0-9][0-9\.,]*)", raw)
    if not number_match:
        return None
    return parse_locale_number(number_match.group(1))


def parse_price_text_to_million_vnd(text: str) -> Optional[float]:
    raw = clean_text(text).lower()
    if not raw:
        return None

    if any(keyword in raw for keyword in UNKNOWN_PRICE_KEYWORDS):
        return None

    total = 0.0
    found = False
    unit_patterns = [
        (r"([0-9][0-9\.,]*)\s*(?:tỷ|ty)\b", 1000.0),
        (r"([0-9][0-9\.,]*)\s*(?:triệu|trieu)\b", 1.0),
        (r"([0-9][0-9\.,]*)\s*(?:nghìn|nghin|ngàn|ngan)\b", 0.001),
    ]

    for pattern, multiplier in unit_patterns:
        for match in re.finditer(pattern, raw):
            value = parse_locale_number(match.group(1))
            if value is None:
                continue
            total += value * multiplier
            found = True

    if found:
        return total

    number_match = re.search(r"([0-9][0-9\.,]*)", raw)
    if not number_match:
        return None

    value = parse_locale_number(number_match.group(1))
    if value is None:
        return None

    if any(token in raw for token in ["vnd", "vnđ", "đồng", "dong", "đ"]):
        if value >= 10000:
            return value / 1_000_000.0

    return value


def percentile(sorted_values: List[float], p: float) -> float:
    if not sorted_values:
        return float("nan")
    idx = int((len(sorted_values) - 1) * p)
    return sorted_values[idx]


def has_bom(path: str) -> bool:
    with open(path, "rb") as file_obj:
        return file_obj.read(3) == b"\xef\xbb\xbf"


def infer_schema(raw_columns: List[str]) -> str:
    cols = set(raw_columns)
    if {"id", "location", "area_m2", "price_million_vnd"}.issubset(cols):
        return "cresht2606_like"
    if {"product_id", "address", "price", "area"}.issubset(cols):
        return "andyvo1009_like"
    return "generic"


def empty_canonical_row() -> Dict[str, str]:
    return {col: "" for col in CANONICAL_COLUMNS}


def normalize_rows(
    raw_rows: List[Dict[str, str]],
    raw_columns: List[str],
    schema_name: str,
) -> Tuple[List[Dict[str, str]], Set[str], Dict[str, int]]:
    raw_col_set = set(raw_columns)
    parse_fail_counts: Dict[str, int] = {
        "area_m2": 0,
        "price_million_vnd": 0,
    }

    normalized_rows: List[Dict[str, str]] = []

    if schema_name == "cresht2606_like":
        available_fields = {col for col in CANONICAL_COLUMNS if col in raw_col_set}
        for row in raw_rows:
            normalized = empty_canonical_row()
            for col in available_fields:
                value = clean_text(row.get(col, ""))
                if col == "location":
                    value = clean_location(value)
                normalized[col] = value
            normalized_rows.append(normalized)
        return normalized_rows, available_fields, parse_fail_counts

    if schema_name == "andyvo1009_like":
        available_fields = {
            "id",
            "location",
            "area_m2",
            "bedrooms",
            "bathrooms",
            "price_million_vnd",
        }
        for row in raw_rows:
            normalized = empty_canonical_row()
            normalized["id"] = clean_text(row.get("product_id", ""))
            normalized["location"] = clean_location(row.get("address", ""))
            normalized["bedrooms"] = clean_text(row.get("bedrooms_num", ""))
            normalized["bathrooms"] = clean_text(row.get("bathrooms_num", ""))

            area_raw = clean_text(row.get("area", ""))
            area_value = parse_area_text_to_m2(area_raw)
            if area_raw and area_value is None:
                parse_fail_counts["area_m2"] += 1
            if area_value is not None:
                normalized["area_m2"] = str(area_value)

            price_raw = clean_text(row.get("price", ""))
            price_value = parse_price_text_to_million_vnd(price_raw)
            if price_raw and price_value is None:
                parse_fail_counts["price_million_vnd"] += 1
            if price_value is not None:
                normalized["price_million_vnd"] = str(price_value)

            normalized_rows.append(normalized)
        return normalized_rows, available_fields, parse_fail_counts

    available_fields: Set[str] = set()
    if "id" in raw_col_set or "product_id" in raw_col_set:
        available_fields.add("id")
    if "detail_url" in raw_col_set or "url" in raw_col_set or "link" in raw_col_set:
        available_fields.add("detail_url")
    if "title" in raw_col_set:
        available_fields.add("title")
    if "location" in raw_col_set or "address" in raw_col_set:
        available_fields.add("location")
    if "timeline_hours" in raw_col_set:
        available_fields.add("timeline_hours")
    if "area_m2" in raw_col_set or "area" in raw_col_set:
        available_fields.add("area_m2")
    if "bedrooms" in raw_col_set or "bedrooms_num" in raw_col_set:
        available_fields.add("bedrooms")
    if "bathrooms" in raw_col_set or "bathrooms_num" in raw_col_set:
        available_fields.add("bathrooms")
    if "floors" in raw_col_set:
        available_fields.add("floors")
    if "frontage" in raw_col_set:
        available_fields.add("frontage")
    if "price_million_vnd" in raw_col_set or "price" in raw_col_set:
        available_fields.add("price_million_vnd")

    for row in raw_rows:
        normalized = empty_canonical_row()
        normalized["id"] = clean_text(row.get("id", "") or row.get("product_id", ""))
        normalized["detail_url"] = clean_text(
            row.get("detail_url", "") or row.get("url", "") or row.get("link", "")
        )
        normalized["title"] = clean_text(row.get("title", ""))
        normalized["location"] = clean_location(row.get("location", "") or row.get("address", ""))
        normalized["timeline_hours"] = clean_text(row.get("timeline_hours", ""))
        normalized["bedrooms"] = clean_text(row.get("bedrooms", "") or row.get("bedrooms_num", ""))
        normalized["bathrooms"] = clean_text(row.get("bathrooms", "") or row.get("bathrooms_num", ""))
        normalized["floors"] = clean_text(row.get("floors", ""))
        normalized["frontage"] = clean_text(row.get("frontage", ""))

        area_raw = clean_text(row.get("area_m2", ""))
        if area_raw:
            normalized["area_m2"] = area_raw
        else:
            area_text = clean_text(row.get("area", ""))
            area_value = parse_area_text_to_m2(area_text)
            if area_text and area_value is None:
                parse_fail_counts["area_m2"] += 1
            if area_value is not None:
                normalized["area_m2"] = str(area_value)

        price_raw = clean_text(row.get("price_million_vnd", ""))
        if price_raw:
            normalized["price_million_vnd"] = price_raw
        else:
            price_text = clean_text(row.get("price", ""))
            price_value = parse_price_text_to_million_vnd(price_text)
            if price_text and price_value is None:
                parse_fail_counts["price_million_vnd"] += 1
            if price_value is not None:
                normalized["price_million_vnd"] = str(price_value)

        normalized_rows.append(normalized)

    return normalized_rows, available_fields, parse_fail_counts


def decimal_profile(
    rows: List[Dict[str, str]],
    column: str,
    available: bool,
) -> Dict[str, object]:
    if not available:
        return {
            "available": False,
            "count": 0,
            "int_like_pct": 0.0,
            "one_decimal_pct": 0.0,
            "gt1_decimal_pct": 0.0,
        }

    total = 0
    int_like = 0
    one_decimal = 0
    gt1_decimal = 0
    for row in rows:
        raw = clean_text(row.get(column, ""))
        if raw == "" or raw.lower() == "nan":
            continue
        total += 1
        if "." not in raw:
            int_like += 1
            continue
        frac = raw.split(".")[-1].rstrip("0")
        if frac == "":
            int_like += 1
        elif len(frac) == 1:
            one_decimal += 1
        else:
            gt1_decimal += 1

    if total == 0:
        return {
            "available": True,
            "count": 0,
            "int_like_pct": 0.0,
            "one_decimal_pct": 0.0,
            "gt1_decimal_pct": 0.0,
        }

    return {
        "available": True,
        "count": total,
        "int_like_pct": round(int_like * 100.0 / total, 2),
        "one_decimal_pct": round(one_decimal * 100.0 / total, 2),
        "gt1_decimal_pct": round(gt1_decimal * 100.0 / total, 2),
    }


def compute_missing(
    rows: List[Dict[str, str]],
    available_fields: Set[str],
) -> Tuple[Dict[str, Optional[int]], Dict[str, Optional[float]]]:
    row_count = len(rows)
    missing_counts: Dict[str, Optional[int]] = {}
    missing_pct: Dict[str, Optional[float]] = {}

    for col in CANONICAL_COLUMNS:
        if col not in available_fields:
            missing_counts[col] = None
            missing_pct[col] = None
            continue

        missing = 0
        for row in rows:
            value = clean_text(row.get(col, ""))
            if value == "" or value.lower() == "nan":
                missing += 1
        missing_counts[col] = missing
        missing_pct[col] = round(missing * 100.0 / row_count, 2) if row_count else 0.0

    return missing_counts, missing_pct


def compute_duplicate_stats(
    rows: List[Dict[str, str]],
    available_fields: Set[str],
) -> Dict[str, Dict[str, Optional[int]]]:
    duplicate_stats: Dict[str, Dict[str, Optional[int]]] = {}

    for key in ["id", "detail_url", "title"]:
        if key not in available_fields:
            duplicate_stats[key] = {
                "available": False,
                "duplicates": None,
                "unique_nonempty": None,
            }
            continue

        values = [clean_text(row.get(key, "")) for row in rows]
        counter = Counter(values)
        duplicate_count = sum(v - 1 for k, v in counter.items() if k != "" and v > 1)
        unique_nonempty = sum(1 for k in counter if k != "")
        duplicate_stats[key] = {
            "available": True,
            "duplicates": duplicate_count,
            "unique_nonempty": unique_nonempty,
        }

    return duplicate_stats


def compute_numeric_stats(
    rows: List[Dict[str, str]],
    available_fields: Set[str],
    parse_fail_counts: Dict[str, int],
) -> Dict[str, Dict[str, object]]:
    numeric_stats: Dict[str, Dict[str, object]] = {}

    for col in NUMERIC_COLUMNS:
        if col not in available_fields:
            numeric_stats[col] = {
                "available": False,
            }
            continue

        values: List[float] = []
        invalid_parse = parse_fail_counts.get(col, 0)
        zero_count = 0
        negative_count = 0

        for row in rows:
            raw = clean_text(row.get(col, ""))
            if raw == "" or raw.lower() == "nan":
                continue
            value = to_float(raw)
            if value is None:
                invalid_parse += 1
                continue
            values.append(value)
            if value == 0:
                zero_count += 1
            if value < 0:
                negative_count += 1

        values_sorted = sorted(values)
        if values_sorted:
            numeric_stats[col] = {
                "available": True,
                "count": len(values_sorted),
                "invalid_parse": invalid_parse,
                "min": values_sorted[0],
                "p50": percentile(values_sorted, 0.5),
                "p95": percentile(values_sorted, 0.95),
                "max": values_sorted[-1],
                "zero_count": zero_count,
                "negative_count": negative_count,
            }
        else:
            numeric_stats[col] = {
                "available": True,
                "count": 0,
                "invalid_parse": invalid_parse,
            }

    return numeric_stats


def evaluate_criteria(
    schema_name: str,
    row_count: int,
    stale_hours: int,
    available_fields: Set[str],
    missing_pct: Dict[str, Optional[float]],
    duplicate_stats: Dict[str, Dict[str, Optional[int]]],
    outlier_index_sets: Dict[str, set],
    outlier_pct: Dict[str, Optional[float]],
    outlier_applicable: Dict[str, bool],
    bom: bool,
    commercial_pct: Optional[float],
    location_stats: Dict[str, Optional[float]],
    decimal_area: Dict[str, object],
    decimal_price: Dict[str, object],
    parse_fail_counts: Dict[str, int],
) -> Dict[str, Dict[str, str]]:
    results: Dict[str, Dict[str, str]] = {}

    parse_fail_total = sum(parse_fail_counts.values())
    parse_fail_pct = (parse_fail_total * 100.0 / row_count) if row_count else 0.0

    if row_count > 0:
        results["Accessibility"] = {
            "status": "pass",
            "reason": f"CSV readable, {row_count} rows.",
        }
    else:
        results["Accessibility"] = {
            "status": "fail",
            "reason": "CSV empty or unreadable.",
        }

    essential_fields = {"id", "location", "price_million_vnd"}
    missing_essential = sorted(essential_fields - available_fields)
    if missing_essential:
        results["Relevance"] = {
            "status": "fail",
            "reason": f"Missing essential fields: {missing_essential}.",
        }
    elif commercial_pct is None:
        results["Relevance"] = {
            "status": "partial",
            "reason": "No title column; topic-noise check unavailable.",
        }
    else:
        results["Relevance"] = {
            "status": status_from_rate(commercial_pct, pass_max=10.0, partial_max=25.0),
            "reason": f"Commercial-like titles: {commercial_pct:.2f}%.",
        }

    interpret_status = "pass"
    interpret_reasons: List[str] = []
    if bom:
        interpret_status = combine_status(interpret_status, "partial")
        interpret_reasons.append("BOM detected; load with utf-8-sig.")
    if parse_fail_total > 0:
        if parse_fail_pct > 10.0:
            interpret_status = combine_status(interpret_status, "fail")
        else:
            interpret_status = combine_status(interpret_status, "partial")
        interpret_reasons.append(
            f"Text-to-number parse failures: {parse_fail_total} rows ({parse_fail_pct:.2f}%)."
        )
    if schema_name == "generic":
        interpret_status = combine_status(interpret_status, "partial")
        interpret_reasons.append("Generic schema mapping mode in use.")
    if not interpret_reasons:
        interpret_reasons.append("Columns parsed cleanly.")
    results["Interpretability"] = {
        "status": interpret_status,
        "reason": " ".join(interpret_reasons),
    }

    critical_fields = [
        field
        for field in ["area_m2", "bedrooms", "bathrooms", "floors", "price_million_vnd"]
        if field in available_fields and missing_pct.get(field) is not None
    ]
    if not critical_fields:
        reliability_status = "fail"
        reliability_reason = "No measurable critical fields (area/bed/bath/floors/price)."
    else:
        critical_missing_avg = mean(float(missing_pct[field]) for field in critical_fields)
        if critical_missing_avg > 35.0:
            reliability_status = "fail"
        elif critical_missing_avg > 15.0:
            reliability_status = "partial"
        else:
            reliability_status = "pass"
        if len(critical_fields) < 5:
            reliability_status = combine_status(reliability_status, "partial")
        reliability_reason = (
            f"Critical-field missing avg: {critical_missing_avg:.2f}% over {critical_fields}."
        )
    results["Reliability"] = {
        "status": reliability_status,
        "reason": reliability_reason,
    }

    if "timeline_hours" not in available_fields:
        timeliness_status = "partial"
        timeliness_reason = "timeline_hours not available in this schema."
    else:
        stale_pct = outlier_pct.get("timeline_gt_1y") or 0.0
        timeliness_status = status_from_rate(stale_pct, pass_max=10.0, partial_max=30.0)
        timeliness_reason = f"Rows older than {stale_hours}h: {stale_pct:.2f}%."
    results["Timeliness"] = {
        "status": timeliness_status,
        "reason": timeliness_reason,
    }

    suspicious_keys = ["area_gt_10000", "floors_gt_30", "price_gt_200000", "price_eq_0"]
    applied_suspicious = [key for key in suspicious_keys if outlier_applicable.get(key, False)]
    if not applied_suspicious:
        accuracy_status = "partial"
        accuracy_reason = "No applicable suspicious-value checks for available fields."
    else:
        suspicious_union = set()
        for key in applied_suspicious:
            suspicious_union.update(outlier_index_sets.get(key, set()))
        suspicious_pct = (len(suspicious_union) * 100.0 / row_count) if row_count else 0.0
        accuracy_status = status_from_rate(suspicious_pct, pass_max=1.0, partial_max=5.0)
        if parse_fail_pct > 5.0:
            accuracy_status = combine_status(accuracy_status, "partial")
        accuracy_reason = (
            f"Suspicious rows: {suspicious_pct:.2f}% using checks {applied_suspicious}."
        )
    results["Accuracy"] = {
        "status": accuracy_status,
        "reason": accuracy_reason,
    }

    consistency_status = "pass"
    consistency_reasons: List[str] = []
    if "id" not in available_fields:
        consistency_status = "fail"
        consistency_reasons.append("id field unavailable.")
    else:
        duplicate_id = duplicate_stats.get("id", {}).get("duplicates") or 0
        if duplicate_id > 0:
            consistency_status = combine_status(consistency_status, "fail")
        consistency_reasons.append(f"Duplicate id: {duplicate_id}.")

    location_one_comma_pct = location_stats.get("one_comma_pct")
    if location_one_comma_pct is None:
        consistency_status = combine_status(consistency_status, "partial")
        consistency_reasons.append("Location format check unavailable.")
    else:
        if location_one_comma_pct < 90.0:
            consistency_status = combine_status(consistency_status, "fail")
        elif location_one_comma_pct < 98.0:
            consistency_status = combine_status(consistency_status, "partial")
        consistency_reasons.append(f"Location one-comma format: {location_one_comma_pct:.2f}%.")

    if bom:
        consistency_status = combine_status(consistency_status, "partial")
    results["Consistency"] = {
        "status": consistency_status,
        "reason": " ".join(consistency_reasons),
    }

    have_area = "area_m2" in available_fields
    have_price = "price_million_vnd" in available_fields
    if not have_area and not have_price:
        precision_status = "fail"
        precision_reason = "Both area_m2 and price_million_vnd are unavailable."
    else:
        precision_status = "pass"
        precision_parts: List[str] = []

        if not have_area:
            precision_status = combine_status(precision_status, "partial")
            precision_parts.append("area_m2 unavailable")
        else:
            area_int = float(decimal_area.get("int_like_pct", 0.0))
            if area_int < 80.0:
                precision_status = combine_status(precision_status, "fail")
            elif area_int < 95.0:
                precision_status = combine_status(precision_status, "partial")
            precision_parts.append(f"area int-like: {area_int:.2f}%")

        if not have_price:
            precision_status = combine_status(precision_status, "partial")
            precision_parts.append("price_million_vnd unavailable")
        else:
            price_multi_dec = float(decimal_price.get("gt1_decimal_pct", 0.0))
            if price_multi_dec > 15.0:
                precision_status = combine_status(precision_status, "fail")
            elif price_multi_dec > 5.0:
                precision_status = combine_status(precision_status, "partial")
            precision_parts.append(f"price >1 decimal: {price_multi_dec:.2f}%")

        if parse_fail_total > 0:
            precision_status = combine_status(precision_status, "partial")
            precision_parts.append(
                f"parse failures area/price: {parse_fail_total} rows ({parse_fail_pct:.2f}%)"
            )

        precision_reason = ", ".join(precision_parts)

    results["Precision"] = {
        "status": precision_status,
        "reason": precision_reason,
    }

    granularity_core = {"id", "location", "price_million_vnd"}
    missing_gran_core = sorted(granularity_core - available_fields)
    if missing_gran_core:
        granularity_status = "fail"
        granularity_reason = f"Missing core row-level fields: {missing_gran_core}."
    else:
        optional_context = {"detail_url", "title", "timeline_hours"}
        missing_optional = sorted(optional_context - available_fields)
        if missing_optional:
            granularity_status = "partial"
            granularity_reason = (
                "Core row-level fields present; missing optional context fields: "
                f"{missing_optional}."
            )
        else:
            granularity_status = "pass"
            granularity_reason = "Has id/url/title/location/time/price row-level context."

    results["Granularity"] = {
        "status": granularity_status,
        "reason": granularity_reason,
    }

    return results


def build_file_report(
    path: str,
    stale_hours: int,
    sample_limit: int,
) -> Dict[str, object]:
    with open(path, "r", encoding="utf-8-sig", newline="") as file_obj:
        reader = csv.DictReader(file_obj)
        raw_columns = reader.fieldnames or []
        raw_rows = list(reader)

    schema_name = infer_schema(raw_columns)
    rows, available_fields, parse_fail_counts = normalize_rows(
        raw_rows=raw_rows,
        raw_columns=raw_columns,
        schema_name=schema_name,
    )

    row_count = len(rows)
    missing_counts, missing_pct = compute_missing(rows, available_fields)
    duplicate_stats = compute_duplicate_stats(rows, available_fields)
    numeric_stats = compute_numeric_stats(rows, available_fields, parse_fail_counts)

    parsed_rows: List[Dict[str, Optional[float]]] = []
    for row in rows:
        parsed_rows.append(
            {
                col: (to_float(row.get(col, "")) if col in available_fields else None)
                for col in NUMERIC_COLUMNS
            }
        )

    outlier_counts = {name: 0 for name in OUTLIER_RULES}
    outlier_samples = {name: [] for name in OUTLIER_RULES}
    outlier_index_sets = {name: set() for name in OUTLIER_RULES}
    outlier_applicable = {
        name: OUTLIER_RULES[name]["field"] in available_fields for name in OUTLIER_RULES
    }

    for idx, (row, parsed) in enumerate(zip(rows, parsed_rows)):
        for name, rule in OUTLIER_RULES.items():
            if not outlier_applicable[name]:
                continue
            field_name = str(rule["field"])
            field_value = parsed.get(field_name)
            if rule["check"](field_value, stale_hours):
                outlier_counts[name] += 1
                outlier_index_sets[name].add(idx)
                if len(outlier_samples[name]) < sample_limit:
                    outlier_samples[name].append(
                        {
                            "id": row.get("id", ""),
                            "location": row.get("location", ""),
                            "area_m2": row.get("area_m2", ""),
                            "bedrooms": row.get("bedrooms", ""),
                            "bathrooms": row.get("bathrooms", ""),
                            "floors": row.get("floors", ""),
                            "price_million_vnd": row.get("price_million_vnd", ""),
                            "timeline_hours": row.get("timeline_hours", ""),
                            "detail_url": row.get("detail_url", ""),
                        }
                    )

    outlier_pct: Dict[str, Optional[float]] = {}
    for key, count in outlier_counts.items():
        if not outlier_applicable[key]:
            outlier_pct[key] = None
        elif row_count == 0:
            outlier_pct[key] = 0.0
        else:
            outlier_pct[key] = round(count * 100.0 / row_count, 2)

    location_stats: Dict[str, Optional[float]] = {
        "unique_count": None,
        "one_comma_count": None,
        "zero_comma_count": None,
        "gt1_comma_count": None,
        "one_comma_pct": None,
        "top5": [],
    }
    if "location" in available_fields:
        location_counter = Counter(
            clean_text(row.get("location", ""))
            for row in rows
            if clean_text(row.get("location", ""))
        )
        one_comma = 0
        zero_comma = 0
        gt1_comma = 0
        for row in rows:
            location = clean_text(row.get("location", ""))
            comma_count = location.count(",")
            if comma_count == 1:
                one_comma += 1
            elif comma_count == 0:
                zero_comma += 1
            else:
                gt1_comma += 1

        location_stats = {
            "unique_count": len(location_counter),
            "one_comma_count": one_comma,
            "zero_comma_count": zero_comma,
            "gt1_comma_count": gt1_comma,
            "one_comma_pct": round(one_comma * 100.0 / row_count, 2) if row_count else 0.0,
            "top5": location_counter.most_common(5),
        }

    frontage_values_top: List[Tuple[str, int]] = []
    if "frontage" in available_fields:
        frontage_counter = Counter(clean_text(row.get("frontage", "")) for row in rows)
        frontage_values_top = frontage_counter.most_common(5)

    commercial_count: Optional[int] = None
    commercial_pct: Optional[float] = None
    if "title" in available_fields:
        commercial_re = [re.compile(pattern, flags=re.IGNORECASE) for pattern in COMMERCIAL_PATTERNS]
        count = 0
        for row in rows:
            title = clean_text(row.get("title", ""))
            if any(pattern.search(title) for pattern in commercial_re):
                count += 1
        commercial_count = count
        commercial_pct = round(count * 100.0 / row_count, 2) if row_count else 0.0

    decimal_area = decimal_profile(rows, "area_m2", "area_m2" in available_fields)
    decimal_price = decimal_profile(rows, "price_million_vnd", "price_million_vnd" in available_fields)

    criteria = evaluate_criteria(
        schema_name=schema_name,
        row_count=row_count,
        stale_hours=stale_hours,
        available_fields=available_fields,
        missing_pct=missing_pct,
        duplicate_stats=duplicate_stats,
        outlier_index_sets=outlier_index_sets,
        outlier_pct=outlier_pct,
        outlier_applicable=outlier_applicable,
        bom=has_bom(path),
        commercial_pct=commercial_pct,
        location_stats=location_stats,
        decimal_area=decimal_area,
        decimal_price=decimal_price,
        parse_fail_counts=parse_fail_counts,
    )

    return {
        "file": path,
        "filename": os.path.basename(path),
        "schema_name": schema_name,
        "bom": has_bom(path),
        "row_count": row_count,
        "raw_column_count": len(raw_columns),
        "raw_columns": raw_columns,
        "available_fields": sorted(available_fields),
        "missing_counts": missing_counts,
        "missing_pct": missing_pct,
        "duplicate_stats": duplicate_stats,
        "numeric_stats": numeric_stats,
        "normalization_parse_fail_counts": parse_fail_counts,
        "outlier_counts": outlier_counts,
        "outlier_pct": outlier_pct,
        "outlier_applicable": outlier_applicable,
        "outlier_samples": outlier_samples,
        "commercial_like_count": commercial_count,
        "commercial_like_pct": commercial_pct,
        "frontage_values_top": frontage_values_top,
        "location": location_stats,
        "decimal_profile": {
            "area_m2": decimal_area,
            "price_million_vnd": decimal_price,
        },
        "criteria": criteria,
    }


def overall_summary(reports: List[Dict[str, object]]) -> Dict[str, object]:
    total_rows = sum(int(report["row_count"]) for report in reports)
    if total_rows == 0:
        return {
            "total_files": len(reports),
            "total_rows": 0,
            "schema_consistent": False,
            "criteria_status_counts": {},
            "schema_names": sorted({str(report["schema_name"]) for report in reports}),
        }

    schema_signatures = {tuple(report["raw_columns"]) for report in reports}
    schema_consistent = len(schema_signatures) == 1

    criteria_status_counts: Dict[str, Dict[str, int]] = {}
    for report in reports:
        for criterion, detail in report["criteria"].items():
            criteria_status_counts.setdefault(
                criterion,
                {"pass": 0, "partial": 0, "fail": 0},
            )
            criteria_status_counts[criterion][detail["status"]] += 1

    return {
        "total_files": len(reports),
        "total_rows": total_rows,
        "schema_consistent": schema_consistent,
        "criteria_status_counts": criteria_status_counts,
        "schema_names": sorted({str(report["schema_name"]) for report in reports}),
    }


def format_pct(value: Optional[float]) -> str:
    if value is None:
        return "N/A"
    return f"{value}%"


def print_report(report: Dict[str, object]) -> None:
    print(f"\n=== {report['filename']} ===")
    print(
        "Rows: "
        f"{report['row_count']}, "
        f"Raw columns: {report['raw_column_count']}, "
        f"BOM: {report['bom']}, "
        f"Schema: {report['schema_name']}"
    )
    print(f"Available fields: {report['available_fields']}")

    parse_fail_counts = report.get("normalization_parse_fail_counts", {})
    parse_fail_total = sum(int(v) for v in parse_fail_counts.values())
    if parse_fail_total > 0:
        print(f"Normalization parse failures: {parse_fail_counts}")

    print("Criteria:")
    ordered = [
        "Relevance",
        "Accessibility",
        "Interpretability",
        "Reliability",
        "Timeliness",
        "Accuracy",
        "Consistency",
        "Precision",
        "Granularity",
    ]
    for criterion in ordered:
        detail = report["criteria"][criterion]
        print(f"- {criterion}: {detail['status'].upper()} | {detail['reason']}")

    print("Missing % (key fields):")
    for key in ["area_m2", "bedrooms", "bathrooms", "floors", "price_million_vnd"]:
        print(f"- {key}: {format_pct(report['missing_pct'].get(key))}")

    print("Outlier %:")
    for key in [
        "timeline_gt_1y",
        "area_gt_10000",
        "floors_gt_30",
        "price_gt_200000",
        "price_eq_0",
    ]:
        print(f"- {key}: {format_pct(report['outlier_pct'].get(key))}")


def main() -> int:
    args = parse_args()
    input_dir = os.path.abspath(args.input_dir)
    if not os.path.isdir(input_dir):
        print(f"Input directory not found: {input_dir}")
        return 1

    csv_files = sorted(
        os.path.join(input_dir, name)
        for name in os.listdir(input_dir)
        if name.lower().endswith(".csv")
    )
    if not csv_files:
        print(f"No CSV files found in: {input_dir}")
        return 1

    reports = [
        build_file_report(path=path, stale_hours=args.stale_hours, sample_limit=args.sample_limit)
        for path in csv_files
    ]
    summary = overall_summary(reports)

    print("DATA QUALITY AUDIT")
    print(f"Input directory: {input_dir}")
    print(f"CSV files: {len(csv_files)}")
    print(f"Total rows: {summary['total_rows']}")
    print(f"Schema consistent across files: {summary['schema_consistent']}")
    print(f"Detected schema types: {summary['schema_names']}")

    for report in reports:
        print_report(report)

    print("\nOverall criteria status counts:")
    for criterion, counts in summary["criteria_status_counts"].items():
        print(
            f"- {criterion}: "
            f"pass={counts['pass']}, "
            f"partial={counts['partial']}, "
            f"fail={counts['fail']}"
        )

    final = {
        "input_dir": input_dir,
        "summary": summary,
        "files": reports,
    }
    if args.output_json:
        output_path = os.path.abspath(args.output_json)
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        with open(output_path, "w", encoding="utf-8") as file_obj:
            json.dump(final, file_obj, ensure_ascii=False, indent=2)
        print(f"\nSaved JSON report: {output_path}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
