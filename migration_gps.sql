-- =========================================================
-- CREATE GPS LOGS TABLE FOR SALESMAN TRACKING
-- =========================================================

CREATE TABLE IF NOT EXISTS gps_logs (
    id TEXT PRIMARY KEY,
    user_id TEXT,
    user_name TEXT,
    action TEXT,
    reference_id TEXT,
    lat NUMERIC,
    lng NUMERIC,
    timestamp TIMESTAMPTZ DEFAULT NOW(),
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Enable RLS (if you have it enabled on other tables)
ALTER TABLE gps_logs ENABLE ROW LEVEL SECURITY;

-- Create policy for all authenticated users to insert/select
DROP POLICY IF EXISTS "Enable all access for gps_logs" ON gps_logs;
CREATE POLICY "Enable all access for gps_logs" ON gps_logs FOR ALL USING (true) WITH CHECK (true);

-- Create simple index for filtering by user or date
CREATE INDEX IF NOT EXISTS idx_gps_logs_user ON gps_logs(user_name);
CREATE INDEX IF NOT EXISTS idx_gps_logs_time ON gps_logs(timestamp);

-- IMPORTANT: Add gps_logs to your local DB Table Cache mapping in app.js
