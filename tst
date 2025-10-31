
-- ============================================================
-- MULTI-TABLE VALIDATOR - Snowflake Task-Based SQL Script
-- No procedures used - Pure SQL scripting
-- Date Generated: 20251031
-- ============================================================

-- STEP 1: Create control table to configure validations
CREATE OR REPLACE TABLE VALIDATION_CONTROL (
  RUN_ID STRING,
  JOB_NAME STRING,
  DOMAIN_NM STRING,
  SRC_TABLE STRING,
  TGT_TABLE STRING,
  KEY_COLS STRING,
  MANDATORY_COLS STRING,
  COMPARE_COLS_RULES STRING,
  TS_COLUMN STRING,
  OWNER_TEAM STRING,
  ENABLED BOOLEAN DEFAULT TRUE
);

-- STEP 2: Create validation result table
CREATE OR REPLACE TABLE VALIDATION_RESULTS (
  run_id STRING,
  batch_ts TIMESTAMP,
  job_name STRING,
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
  mismatch_cnt INT,
  null_pct_mandatory FLOAT,
  owner_team STRING,
  notes_short STRING,
  sample_keys VARIANT
);

-- STEP 3: Main validator script using SQL scripting
DECLARE job RECORD;
DECLARE c CURSOR FOR 
  SELECT * FROM VALIDATION_CONTROL WHERE ENABLED = TRUE;

BEGIN
  FOR job IN c DO

    LET run_id = job.RUN_ID;
    LET job_name = job.JOB_NAME;
    LET domain_nm = job.DOMAIN_NM;
    LET src_table = job.SRC_TABLE;
    LET tgt_table = job.TGT_TABLE;
    LET key_cols = job.KEY_COLS;
    LET mandatory_cols = job.MANDATORY_COLS;
    LET ts_col = job.TS_COLUMN;
    LET owner_team = job.OWNER_TEAM;

    LET key_expr = REPLACE(key_cols, ',', ', ');

    -- ROW COVERAGE
    EXECUTE IMMEDIATE '
      WITH src AS (SELECT COUNT(*) AS c FROM IDENTIFIER(''' || :src_table || ''')),
           tgt AS (SELECT COUNT(*) AS c FROM IDENTIFIER(''' || :tgt_table || '''))
      INSERT INTO VALIDATION_RESULTS (
        run_id, batch_ts, job_name, domain_nm, table_src, table_tgt,
        validation_type, kpi_nm, metric_value, metric_target, status, severity,
        src_count, tgt_count, owner_team, notes_short
      )
      SELECT
        ''' || :run_id || ''', CURRENT_TIMESTAMP(), ''' || :job_name || ''', ''' || :domain_nm || ''',
        ''' || :src_table || ''', ''' || :tgt_table || ''', 'ROW_COVERAGE', 'COMPLETENESS',
        IFF(s.c = 0, NULL, t.c / NULLIF(s.c, 0)), 0.99,
        CASE WHEN s.c = 0 THEN 'FAIL'
             WHEN t.c / NULLIF(s.c, 0) >= 0.99 THEN 'PASS'
             WHEN t.c / NULLIF(s.c, 0) >= 0.98 THEN 'WARN'
             ELSE 'FAIL' END,
        CASE WHEN s.c = 0 THEN 'ERROR'
             WHEN t.c / NULLIF(s.c, 0) >= 0.99 THEN 'INFO'
             WHEN t.c / NULLIF(s.c, 0) >= 0.98 THEN 'WARN'
             ELSE 'ERROR' END,
        s.c, t.c, ''' || :owner_team || ''', 'Row coverage (tgt/src)'
      FROM src s, tgt t;
    ';

    -- MINUS_SRC
    EXECUTE IMMEDIATE '
      WITH s AS (SELECT ' || :key_expr || ' FROM IDENTIFIER(''' || :src_table || ''')),
           t AS (SELECT ' || :key_expr || ' FROM IDENTIFIER(''' || :tgt_table || ''')),
           only_src AS (
             SELECT s.* FROM s LEFT JOIN t USING (' || :key_expr || ')
             WHERE ' || ARRAY_TO_STRING(ARRAY_AGG('t.' || SPLIT_PART(k, ' ', 1) || ' IS NULL'), ' AND ') || '
           )
      INSERT INTO VALIDATION_RESULTS (
        run_id, batch_ts, job_name, domain_nm, table_src, table_tgt,
        validation_type, kpi_nm, status, severity, only_in_src_cnt,
        owner_team, notes_short, sample_keys
      )
      SELECT
        ''' || :run_id || ''', CURRENT_TIMESTAMP(), ''' || :job_name || ''', ''' || :domain_nm || ''',
        ''' || :src_table || ''', ''' || :tgt_table || ''', 'MINUS_SRC', 'ACCURACY',
        IFF(COUNT(*) = 0, 'PASS', 'WARN'),
        IFF(COUNT(*) = 0, 'INFO', 'WARN'),
        COUNT(*), ''' || :owner_team || ''', 'Rows only in SRC',
        ARRAY_AGG(OBJECT_CONSTRUCT(*)) LIMIT 10
      FROM only_src;
    ';

    -- MINUS_TGT
    EXECUTE IMMEDIATE '
      WITH s AS (SELECT ' || :key_expr || ' FROM IDENTIFIER(''' || :src_table || ''')),
           t AS (SELECT ' || :key_expr || ' FROM IDENTIFIER(''' || :tgt_table || ''')),
           only_tgt AS (
             SELECT t.* FROM t LEFT JOIN s USING (' || :key_expr || ')
             WHERE ' || ARRAY_TO_STRING(ARRAY_AGG('s.' || SPLIT_PART(k, ' ', 1) || ' IS NULL'), ' AND ') || '
           )
      INSERT INTO VALIDATION_RESULTS (
        run_id, batch_ts, job_name, domain_nm, table_src, table_tgt,
        validation_type, kpi_nm, status, severity, only_in_tgt_cnt,
        owner_team, notes_short, sample_keys
      )
      SELECT
        ''' || :run_id || ''', CURRENT_TIMESTAMP(), ''' || :job_name || ''', ''' || :domain_nm || ''',
        ''' || :src_table || ''', ''' || :tgt_table || ''', 'MINUS_TGT', 'ACCURACY',
        IFF(COUNT(*) = 0, 'PASS', 'ERROR'),
        IFF(COUNT(*) = 0, 'INFO', 'ERROR'),
        COUNT(*), ''' || :owner_team || ''', 'Rows only in TGT',
        ARRAY_AGG(OBJECT_CONSTRUCT(*)) LIMIT 10
      FROM only_tgt;
    ';

    -- NULL_STATS
    EXECUTE IMMEDIATE '
      WITH tgt AS (SELECT * FROM IDENTIFIER(''' || :tgt_table || ''')),
           nulls AS (
             SELECT ' || ARRAY_TO_STRING(ARRAY_AGG('AVG(IFF(' || c || ' IS NULL OR TRIM(' || c || ') = '', 1, 0))'), ' + ') || ' / ' || CARDINALITY(ARRAY_AGG(c)) || ' AS null_pct
             FROM tgt
           )
      INSERT INTO VALIDATION_RESULTS (
        run_id, batch_ts, job_name, domain_nm, table_src, table_tgt,
        validation_type, kpi_nm, metric_value, metric_target,
        status, severity, null_pct_mandatory, owner_team, notes_short
      )
      SELECT
        ''' || :run_id || ''', CURRENT_TIMESTAMP(), ''' || :job_name || ''', ''' || :domain_nm || ''',
        ''' || :src_table || ''', ''' || :tgt_table || ''', 'NULL_STATS', 'COMPLETENESS',
        null_pct, 0.01,
        CASE WHEN null_pct <= 0.01 THEN 'PASS'
             WHEN null_pct <= 0.03 THEN 'WARN'
             ELSE 'FAIL' END,
        CASE WHEN null_pct <= 0.01 THEN 'INFO'
             WHEN null_pct <= 0.03 THEN 'WARN'
             ELSE 'ERROR' END,
        null_pct, ''' || :owner_team || ''', 'Avg NULL% across mandatory columns'
      FROM nulls;
    ';
  END FOR;
END;
