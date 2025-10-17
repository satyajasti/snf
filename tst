CREATE SCHEMA IF NOT EXISTS HOSCDA_RPT;

CREATE TABLE IF NOT EXISTS HOSCDA_RPT.FACT_INGEST_VOLUME (
  -- Time grain (windowed)
  window_start_ts   TIMESTAMP_NTZ          COMMENT='Start of the reporting window (24h or sub-daily)',
  window_end_ts     TIMESTAMP_NTZ          COMMENT='End of the reporting window',
  load_dt           DATE                   COMMENT='Partition date for filters/rollups (usually window_start date)',

  -- Slicing dimensions
  stage_nm          STRING                 COMMENT='Pipeline stage: HOSCDA_SIG/HOSCDA/HOSCDA_EXTRACT/…',
  src_nm            STRING                 COMMENT='Source code (e.g., RA code)',
  src_display_nm    STRING                 COMMENT='Source display name (e.g., RA Name)',
  doc_type_cd       STRING                 COMMENT='Document type: CCDA/ADT/CCR/PDF/…',
  doc_subtype_cd    STRING                 COMMENT='Document subtype: e.g., ADT message A01/A02/A03; blank when N/A',
  prcs_nm           STRING                 COMMENT='Process family: MRR/YRDC/Risk Accuracy/ADT/ChartHub/CCR',
  env_nm            STRING                 COMMENT='Environment identifier: DEV/QA/PROD',
  lob_cd            STRING                 COMMENT='(Optional) Line of Business: MA/COMM/…',
  load_type         STRING                 COMMENT='(Optional) FULL / INCR',

  -- Measures (volumes & quality)
  file_cnt          NUMBER(38,0)           COMMENT='Files in window',
  record_cnt        NUMBER(38,0)           COMMENT='Records/messages in window',
  member_cnt        NUMBER(38,0)           COMMENT='Distinct members/patients in window',
  encounter_cnt     NUMBER(38,0)           COMMENT='Distinct encounters in window',
  byte_cnt          NUMBER(38,0)           COMMENT='Total bytes processed (approx is fine)',
  success_cnt       NUMBER(38,0)           COMMENT='Successful records',
  error_cnt         NUMBER(38,0)           COMMENT='Errored records',
  retry_cnt         NUMBER(38,0)           COMMENT='Retried records',

  -- Timeliness & latency
  first_event_ts    TIMESTAMP_NTZ          COMMENT='Earliest event_ts observed in window',
  last_event_ts     TIMESTAMP_NTZ          COMMENT='Latest event_ts observed in window',
  avg_latency_sec   NUMBER(18,3)           COMMENT='Avg(event_ts -> landed_ts) seconds',
  p95_latency_sec   NUMBER(18,3)           COMMENT='P95(event_ts -> landed_ts) seconds',

  -- Lineage / Ops
  edl_run_id        STRING                 COMMENT='Batch/run identifier when applicable',
  batch_id          STRING                 COMMENT='Optional batch grouping',
  created_ts        TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP COMMENT='Insert audit time',

  -- Rollup helpers
  load_yyyy         NUMBER(4,0)            COMMENT='Year of load_dt',
  load_mm           NUMBER(2,0)            COMMENT='Month of load_dt',
  load_yyyy_mm      STRING                 COMMENT='Year-month (YYYY-MM) for fast BI rollups',

  CONSTRAINT PK_FACT_INGEST_VOLUME
    PRIMARY KEY (window_start_ts, stage_nm, src_nm, doc_type_cd, COALESCE(doc_subtype_cd,'~'))
)
-- Choose a cluster key that matches common filters (date + major slicers)
CLUSTER BY (load_dt, stage_nm, src_nm, doc_type_cd);



CREATE TABLE IF NOT EXISTS HOSCDA_RPT.EVENT_INGEST_LOG (
  event_ts        TIMESTAMP_NTZ    COMMENT='Source/system event time',
  landed_ts       TIMESTAMP_NTZ    COMMENT='When the event/file landed in platform',
  stage_nm        STRING           COMMENT='Pipeline stage',
  src_nm          STRING           COMMENT='Source code',
  doc_type_cd     STRING           COMMENT='Document type',
  doc_subtype_cd  STRING           COMMENT='Document subtype',
  prcs_nm         STRING           COMMENT='Process family',
  env_nm          STRING           COMMENT='Environment',
  lob_cd          STRING           COMMENT='(Optional) Line of business',

  file_id         STRING           COMMENT='File identifier',
  file_nm         STRING           COMMENT='(Optional) File name',
  record_id       STRING           COMMENT='Record/message id (if available)',
  member_id       STRING           COMMENT='Member/patient id (if available)',
  encounter_id    STRING           COMMENT='Encounter id (if available)',

  status_cd       STRING           COMMENT='SUCCESS/ERROR/RETRY',
  error_code      STRING           COMMENT='Optional error code',
  error_msg       STRING           COMMENT='Optional error text',
  byte_len        NUMBER(38,0)     COMMENT='Record size (approx ok)',
  edl_run_id      STRING           COMMENT='Batch/run identifier',
  batch_id        STRING           COMMENT='Optional batch group',

  created_ts      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP
)
CLUSTER BY (stage_nm, src_nm, doc_type_cd, TO_DATE(event_ts));


CREATE TABLE IF NOT EXISTS HOSCDA_RPT.DIM_SOURCE (
  src_nm          STRING PRIMARY KEY,
  src_display_nm  STRING,
  ra_code         STRING,
  ra_name         STRING,
  lob_cd          STRING,
  owner_team      STRING,
  is_active       BOOLEAN,
  created_ts      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS HOSCDA_RPT.DIM_DOCTYPE (
  doc_type_cd     STRING,
  doc_subtype_cd  STRING,
  description     STRING,
  is_active       BOOLEAN,
  PRIMARY KEY (doc_type_cd, COALESCE(doc_subtype_cd,'~'))
);

CREATE TABLE IF NOT EXISTS HOSCDA_RPT.DIM_STAGE (
  stage_nm        STRING PRIMARY KEY,
  description     STRING
);



CREATE OR REPLACE VIEW HOSCDA_RPT.VW_FACT_INGEST_VOLUME AS
SELECT
  *,
  CASE WHEN record_cnt > 0 THEN ROUND(error_cnt  * 100.0 / record_cnt, 3) ELSE 0 END AS error_rate_pct,
  CASE WHEN record_cnt > 0 THEN ROUND(success_cnt* 100.0 / record_cnt, 3) ELSE 0 END AS success_rate_pct
FROM HOSCDA_RPT.FACT_INGEST_VOLUME;





| Column                                        | Type                               | Why / maps to                                                               |
| --------------------------------------------- | ---------------------------------- | --------------------------------------------------------------------------- |
| `window_start_ts`, `window_end_ts`, `load_dt` | TIMESTAMP_NTZ, TIMESTAMP_NTZ, DATE | Daily / 24h rolling or sub-daily windows; requirement 3.1/3.2               |
| `stage_nm`                                    | STRING                             | Slice by stage (HOSCDA_SIG/HOSCDA/HOSCDA_EXTRACT) – requirement 4           |
| `src_nm`, `src_display_nm`                    | STRING, STRING                     | Slice by source (RA Code / RA Name) – requirement 1.2                       |
| `doc_type_cd`, `doc_subtype_cd`               | STRING, STRING                     | Volume by doc type/subtype (e.g., ADT A01/A02/A03) – requirements 1.1 & 1.3 |
| `prcs_nm`                                     | STRING                             | Processes (MRR, YRDC, Risk Accuracy, ADT, ChartHub, CCR) – req 3.2          |
| `env_nm`                                      | STRING                             | Environment (PROD/QA/DEV) – ops separation                                  |
| `lob_cd`                                      | STRING                             | Optional: line of business slicing (MA, etc.)                               |
| `load_type`                                   | STRING                             | Optional: FULL vs INCR                                                      |
| `file_cnt`, `record_cnt`                      | NUMBER                             | Total volume & files – requirement 1.1                                      |
| `member_cnt`, `encounter_cnt`                 | NUMBER                             | Member/encounter volumes – requirement 1.4                                  |
| `success_cnt`, `error_cnt`, `retry_cnt`       | NUMBER                             | Pipeline health KPIs                                                        |
| `byte_cnt`                                    | NUMBER                             | Throughput/capacity planning                                                |
| `first_event_ts`, `last_event_ts`             | TIMESTAMP_NTZ                      | Timeliness for the window                                                   |
| `avg_latency_sec`, `p95_latency_sec`          | NUMBER(18,3)                       | Latency metrics for SLAs                                                    |
| `edl_run_id`, `batch_id`                      | STRING                             | Lineage & batch traceability                                                |
| `created_ts`                                  | TIMESTAMP_NTZ                      | Audit                                                                       |
| `load_yyyy`, `load_mm`, `load_yyyy_mm`        | NUMBER, NUMBER, STRING             | Month rollups – requirement 2.3                                             |
