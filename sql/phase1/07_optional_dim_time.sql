-- Optional (Option B): infer posted date from dataset_snapshot_time - timeline_hours.
-- Keep this disabled unless you accept the synthetic timestamp assumption.

BEGIN;

CREATE TABLE IF NOT EXISTS warehouse.dim_time (
  date_id DATE PRIMARY KEY,
  year INT,
  month INT,
  week INT,
  quarter INT
);

-- Example loader (replace :start_date and :end_date in client code):
-- INSERT INTO warehouse.dim_time (date_id, year, month, week, quarter)
-- SELECT
--   d::DATE,
--   EXTRACT(YEAR FROM d)::INT,
--   EXTRACT(MONTH FROM d)::INT,
--   EXTRACT(WEEK FROM d)::INT,
--   EXTRACT(QUARTER FROM d)::INT
-- FROM generate_series(:start_date::DATE, :end_date::DATE, INTERVAL '1 day') AS d
-- ON CONFLICT (date_id) DO NOTHING;

COMMIT;
