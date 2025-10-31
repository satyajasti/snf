
-- ===========================================
-- TEST SCRIPT: Basic Table Pair Validations
-- No stored procedure. No RUN_ID or JOB_NAME.
-- Includes:
--   1. ROW_COVERAGE
--   2. MINUS_SRC
--   3. MINUS_TGT
-- ===========================================

-- Example Inputs (REPLACE with your real values)
SET DOMAIN_NM = 'CLAIMS';
SET SRC_TABLE = 'SRC_DB.SRC_SCHEMA.SRC_TABLE';
SET TGT_TABLE = 'TGT_DB.TGT_SCHEMA.TGT_TABLE';
SET KEY_COLS = 'CLAIM_ID,PATIENT_ID';
SET OWNER_TEAM = 'DataTeam';

-- Pre-format key expression
SET KEY_EXPR = REPLACE($KEY_COLS, ',', ', ');

-- Create temp table for output
CREATE OR REPLACE TEMP TABLE TEMP_VALIDATION_RESULTS (
  batch_ts TIMESTAMP,
  domain_nm STRING,
  table_src STRING,
  table_tgt STRING,
  validation_type STRING,
  kpi_nm STRING,
  metric_value FLOAT,
  metric_target FLOAT,
  status STRING,
  severity STRING,
  src_count INT,
  tgt_count INT,
  only_in_src_cnt INT,
  only_in_tgt_cnt INT,
  owner_team STRING,
  notes_short STRING,
  sample_keys VARIANT
);

-- 1. ROW_COVERAGE
WITH src AS (
  SELECT COUNT(*) AS c FROM IDENTIFIER($SRC_TABLE)
),
tgt AS (
  SELECT COUNT(*) AS c FROM IDENTIFIER($TGT_TABLE)
)
INSERT INTO TEMP_VALIDATION_RESULTS (
  batch_ts, domain_nm, table_src, table_tgt,
  validation_type, kpi_nm, metric_value, metric_target,
  status, severity, src_count, tgt_count, owner_team, notes_short
)
SELECT
  CURRENT_TIMESTAMP(), $DOMAIN_NM, $SRC_TABLE, $TGT_TABLE,
  'ROW_COVERAGE', 'COMPLETENESS',
  IFF(s.c = 0, NULL, t.c / NULLIF(s.c, 0)), 0.99,
  CASE WHEN s.c = 0 THEN 'FAIL'
       WHEN t.c / NULLIF(s.c, 0) >= 0.99 THEN 'PASS'
       WHEN t.c / NULLIF(s.c, 0) >= 0.98 THEN 'WARN'
       ELSE 'FAIL' END,
  CASE WHEN s.c = 0 THEN 'ERROR'
       WHEN t.c / NULLIF(s.c, 0) >= 0.99 THEN 'INFO'
       WHEN t.c / NULLIF(s.c, 0) >= 0.98 THEN 'WARN'
       ELSE 'ERROR' END,
  s.c, t.c, $OWNER_TEAM, 'Row coverage (tgt/src)'
FROM src s, tgt t;

-- View results
SELECT * FROM TEMP_VALIDATION_RESULTS;
