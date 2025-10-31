INSERT INTO REPORTING.VALIDATION_RESULTS
(run_id,batch_ts,job_name,domain_nm,table_src,table_tgt,validation_type,kpi_nm,
 metric_value,metric_target,status,severity,src_count,tgt_count,owner_team,notes_short)
WITH
latest AS (
  SELECT COALESCE(
    (SELECT MAX(EDL_RUN_ID) FROM HOSCDA_EXTRACT.HLTH_OS_CDA_PATIENT),
    (SELECT MAX(EDL_RUN_ID) FROM HOSCDA.HLTH_OS_CDA_PATIENT)
  ) AS rid
),
src AS (
  SELECT COUNT(*) c FROM HOSCDA.HLTH_OS_CDA_PATIENT s, latest
  WHERE s.EDL_RUN_ID = latest.rid
),
tgt AS (
  SELECT COUNT(*) c FROM HOSCDA_EXTRACT.HLTH_OS_CDA_PATIENT t, latest
  WHERE t.EDL_RUN_ID = latest.rid
)
SELECT
  TO_VARCHAR(CURRENT_TIMESTAMP()), CURRENT_TIMESTAMP(), 'Static PATIENT run', 'Patient',
  'HOSCDA.HLTH_OS_CDA_PATIENT','HOSCDA_EXTRACT.HLTH_OS_CDA_PATIENT','ROW_COVERAGE','COMPLETENESS',
  IFF(s.c=0,NULL,t.c/NULLIF(s.c,0)) AS metric_value,
  0.99 AS metric_target,
  CASE WHEN s.c=0 THEN 'FAIL'
       WHEN t.c/NULLIF(s.c,0) >= 0.99 THEN 'PASS'
       WHEN t.c/NULLIF(s.c,0) >= 0.98 THEN 'WARN'
       ELSE 'FAIL' END AS status,
  CASE WHEN s.c=0 THEN 'ERROR'
       WHEN t.c/NULLIF(s.c,0) >= 0.99 THEN 'INFO'
       WHEN t.c/NULLIF(s.c,0) >= 0.98 THEN 'WARN'
       ELSE 'ERROR' END AS severity,
  s.c, t.c, 'Data Eng',
  'Row coverage (tgt/src) for latest run'
FROM src s, tgt t;



-- Only in SRC
INSERT INTO REPORTING.VALIDATION_RESULTS
(run_id,batch_ts,job_name,domain_nm,table_src,table_tgt,validation_type,kpi_nm,
 status,severity,only_in_src_cnt,owner_team,notes_short,sample_keys)
WITH latest AS (
  SELECT COALESCE(
    (SELECT MAX(EDL_RUN_ID) FROM HOSCDA_EXTRACT.HLTH_OS_CDA_PATIENT),
    (SELECT MAX(EDL_RUN_ID) FROM HOSCDA.HLTH_OS_CDA_PATIENT)
  ) AS rid
),
s AS (SELECT PAT_GUID FROM HOSCDA.HLTH_OS_CDA_PATIENT s, latest WHERE s.EDL_RUN_ID = latest.rid),
t AS (SELECT PAT_GUID FROM HOSCDA_EXTRACT.HLTH_OS_CDA_PATIENT t, latest WHERE t.EDL_RUN_ID = latest.rid),
only_src AS (
  SELECT s.PAT_GUID FROM s LEFT JOIN t USING (PAT_GUID) WHERE t.PAT_GUID IS NULL
),
samp AS (SELECT ARRAY_AGG(OBJECT_CONSTRUCT('PAT_GUID',PAT_GUID))[:10] a FROM only_src)
SELECT
  TO_VARCHAR(CURRENT_TIMESTAMP()), CURRENT_TIMESTAMP(), 'Static PATIENT run','Patient',
  'HOSCDA.HLTH_OS_CDA_PATIENT','HOSCDA_EXTRACT.HLTH_OS_CDA_PATIENT','MINUS_SRC','ACCURACY',
  IFF(COUNT(*)=0,'PASS','WARN'), IFF(COUNT(*)=0,'INFO','WARN'),
  COUNT(*),'Data Eng','Rows only in SRC',
  (SELECT TO_JSON(a) FROM samp)
FROM only_src;

-- Only in TGT
INSERT INTO REPORTING.VALIDATION_RESULTS
(run_id,batch_ts,job_name,domain_nm,table_src,table_tgt,validation_type,kpi_nm,
 status,severity,only_in_tgt_cnt,owner_team,notes_short,sample_keys)
WITH latest AS (
  SELECT COALESCE(
    (SELECT MAX(EDL_RUN_ID) FROM HOSCDA_EXTRACT.HLTH_OS_CDA_PATIENT),
    (SELECT MAX(EDL_RUN_ID) FROM HOSCDA.HLTH_OS_CDA_PATIENT)
  ) AS rid
),
s AS (SELECT PAT_GUID FROM HOSCDA.HLTH_OS_CDA_PATIENT s, latest WHERE s.EDL_RUN_ID = latest.rid),
t AS (SELECT PAT_GUID FROM HOSCDA_EXTRACT.HLTH_OS_CDA_PATIENT t, latest WHERE t.EDL_RUN_ID = latest.rid),
only_tgt AS (
  SELECT t.PAT_GUID FROM t LEFT JOIN s USING (PAT_GUID) WHERE s.PAT_GUID IS NULL
),
samp AS (SELECT ARRAY_AGG(OBJECT_CONSTRUCT('PAT_GUID',PAT_GUID))[:10] a FROM only_tgt)
SELECT
  TO_VARCHAR(CURRENT_TIMESTAMP()), CURRENT_TIMESTAMP(), 'Static PATIENT run','Patient',
  'HOSCDA.HLTH_OS_CDA_PATIENT','HOSCDA_EXTRACT.HLTH_OS_CDA_PATIENT','MINUS_TGT','ACCURACY',
  IFF(COUNT(*)=0,'PASS','ERROR'), IFF(COUNT(*)=0,'INFO','ERROR'),
  COUNT(*),'Data Eng','Rows only in TGT',
  (SELECT TO_JSON(a) FROM samp)
FROM only_tgt;




INSERT INTO REPORTING.VALIDATION_RESULTS
(run_id,batch_ts,job_name,domain_nm,table_src,table_tgt,validation_type,kpi_nm,
 metric_value,metric_target,status,severity,mismatch_cnt,owner_team,notes_short)
WITH latest AS (
  SELECT COALESCE(
    (SELECT MAX(EDL_RUN_ID) FROM HOSCDA_EXTRACT.HLTH_OS_CDA_PATIENT),
    (SELECT MAX(EDL_RUN_ID) FROM HOSCDA.HLTH_OS_CDA_PATIENT)
  ) AS rid
),
j AS (
  SELECT s.PAT_GUID,
         UPPER(TRIM(s.MEMBER_ID))  AS s_MEMBER_ID,
         UPPER(TRIM(t.MEMBER_ID))  AS t_MEMBER_ID,
         TO_VARCHAR(DATE_TRUNC('SECOND',TRY_TO_TIMESTAMP_NTZ(s.BIRTH_DT))) AS s_BIRTH_DT,
         TO_VARCHAR(DATE_TRUNC('SECOND',TRY_TO_TIMESTAMP_NTZ(t.BIRTH_DT))) AS t_BIRTH_DT,
         UPPER(TRIM(s.GENDER_CD))  AS s_GENDER_CD,
         UPPER(TRIM(t.GENDER_CD))  AS t_GENDER_CD
  FROM HOSCDA.HLTH_OS_CDA_PATIENT s
  JOIN HOSCDA_EXTRACT.HLTH_OS_CDA_PATIENT t USING (PAT_GUID)
  JOIN latest
  WHERE s.EDL_RUN_ID = latest.rid AND t.EDL_RUN_ID = latest.rid
),
tot AS (SELECT COUNT(*) c FROM j),
bad AS (
  SELECT COUNT(*) c
  FROM j
  WHERE NOT (
    (s_MEMBER_ID IS NULL AND t_MEMBER_ID IS NULL OR s_MEMBER_ID = t_MEMBER_ID)
    AND (s_BIRTH_DT  IS NULL AND t_BIRTH_DT  IS NULL OR s_BIRTH_DT  = t_BIRTH_DT)
    AND (s_GENDER_CD IS NULL AND t_GENDER_CD IS NULL OR s_GENDER_CD = t_GENDER_CD)
  )
)
SELECT
  TO_VARCHAR(CURRENT_TIMESTAMP()), CURRENT_TIMESTAMP(), 'Static PATIENT run','Patient',
  'HOSCDA.HLTH_OS_CDA_PATIENT','HOSCDA_EXTRACT.HLTH_OS_CDA_PATIENT','VALUE_MISMATCH','ACCURACY',
  IFF(tot.c=0,0,bad.c/NULLIF(tot.c,0)) AS mismatch_rate,
  0.005,
  CASE WHEN tot.c=0 THEN 'PASS'
       WHEN bad.c/NULLIF(tot.c,0) <= 0.005 THEN 'PASS'
       WHEN bad.c/NULLIF(tot.c,0) <= 0.01  THEN 'WARN'
       ELSE 'FAIL' END,
  CASE WHEN tot.c=0 THEN 'INFO'
       WHEN bad.c/NULLIF(tot.c,0) <= 0.005 THEN 'INFO'
       WHEN bad.c/NULLIF(tot.c,0) <= 0.01  THEN 'WARN'
       ELSE 'ERROR' END,
  bad.c,
  'Data Eng',
  'Member/Birth/Gender parity (normalized)'
FROM tot, bad;


INSERT INTO REPORTING.VALIDATION_RESULTS
(run_id,batch_ts,job_name,domain_nm,table_src,table_tgt,validation_type,kpi_nm,
 metric_value,metric_target,status,severity,mismatch_cnt,owner_team,notes_short)
WITH latest AS (
  SELECT COALESCE(
    (SELECT MAX(EDL_RUN_ID) FROM HOSCDA_EXTRACT.HLTH_OS_CDA_PATIENT),
    (SELECT MAX(EDL_RUN_ID) FROM HOSCDA.HLTH_OS_CDA_PATIENT)
  ) AS rid
),
j AS (
  SELECT s.PAT_GUID,
         UPPER(TRIM(s.MEMBER_ID))  AS s_MEMBER_ID,
         UPPER(TRIM(t.MEMBER_ID))  AS t_MEMBER_ID,
         TO_VARCHAR(DATE_TRUNC('SECOND',TRY_TO_TIMESTAMP_NTZ(s.BIRTH_DT))) AS s_BIRTH_DT,
         TO_VARCHAR(DATE_TRUNC('SECOND',TRY_TO_TIMESTAMP_NTZ(t.BIRTH_DT))) AS t_BIRTH_DT,
         UPPER(TRIM(s.GENDER_CD))  AS s_GENDER_CD,
         UPPER(TRIM(t.GENDER_CD))  AS t_GENDER_CD
  FROM HOSCDA.HLTH_OS_CDA_PATIENT s
  JOIN HOSCDA_EXTRACT.HLTH_OS_CDA_PATIENT t USING (PAT_GUID)
  JOIN latest
  WHERE s.EDL_RUN_ID = latest.rid AND t.EDL_RUN_ID = latest.rid
),
tot AS (SELECT COUNT(*) c FROM j),
bad AS (
  SELECT COUNT(*) c
  FROM j
  WHERE NOT (
    (s_MEMBER_ID IS NULL AND t_MEMBER_ID IS NULL OR s_MEMBER_ID = t_MEMBER_ID)
    AND (s_BIRTH_DT  IS NULL AND t_BIRTH_DT  IS NULL OR s_BIRTH_DT  = t_BIRTH_DT)
    AND (s_GENDER_CD IS NULL AND t_GENDER_CD IS NULL OR s_GENDER_CD = t_GENDER_CD)
  )
)
SELECT
  TO_VARCHAR(CURRENT_TIMESTAMP()), CURRENT_TIMESTAMP(), 'Static PATIENT run','Patient',
  'HOSCDA.HLTH_OS_CDA_PATIENT','HOSCDA_EXTRACT.HLTH_OS_CDA_PATIENT','VALUE_MISMATCH','ACCURACY',
  IFF(tot.c=0,0,bad.c/NULLIF(tot.c,0)) AS mismatch_rate,
  0.005,
  CASE WHEN tot.c=0 THEN 'PASS'
       WHEN bad.c/NULLIF(tot.c,0) <= 0.005 THEN 'PASS'
       WHEN bad.c/NULLIF(tot.c,0) <= 0.01  THEN 'WARN'
       ELSE 'FAIL' END,
  CASE WHEN tot.c=0 THEN 'INFO'
       WHEN bad.c/NULLIF(tot.c,0) <= 0.005 THEN 'INFO'
       WHEN bad.c/NULLIF(tot.c,0) <= 0.01  THEN 'WARN'
       ELSE 'ERROR' END,
  bad.c,
  'Data Eng',
  'Member/Birth/Gender parity (normalized)'
FROM tot, bad;



INSERT INTO REPORTING.VALIDATION_RESULTS
(run_id,batch_ts,job_name,domain_nm,table_src,table_tgt,validation_type,kpi_nm,
 metric_value,metric_target,status,severity,null_pct_mandatory,owner_team,notes_short)
WITH latest AS (
  SELECT COALESCE(
    (SELECT MAX(EDL_RUN_ID) FROM HOSCDA_EXTRACT.HLTH_OS_CDA_PATIENT),
    (SELECT MAX(EDL_RUN_ID) FROM HOSCDA.HLTH_OS_CDA_PATIENT)
  ) AS rid
),
scoped AS (
  SELECT * FROM HOSCDA_EXTRACT.HLTH_OS_CDA_PATIENT t, latest
  WHERE t.EDL_RUN_ID = latest.rid
),
m AS (
  SELECT AVG(IFF(TRIM(t.MEMBER_ID) IS NULL OR TRIM(t.MEMBER_ID)='',1,0)) AS null_pct
  FROM scoped t
)
SELECT
  TO_VARCHAR(CURRENT_TIMESTAMP()), CURRENT_TIMESTAMP(), 'Static PATIENT run','Patient',
  'HOSCDA.HLTH_OS_CDA_PATIENT','HOSCDA_EXTRACT.HLTH_OS_CDA_PATIENT','NULL_STATS','COMPLETENESS',
  m.null_pct, 0.01,
  CASE WHEN m.null_pct <= 0.01 THEN 'PASS'
       WHEN m.null_pct <= 0.03 THEN 'WARN'
       ELSE 'FAIL' END,
  CASE WHEN m.null_pct <= 0.01 THEN 'INFO'
       WHEN m.null_pct <= 0.03 THEN 'WARN'
       ELSE 'ERROR' END,
  m.null_pct, 'Data Eng',
  'Mandatory NULL% (MEMBER_ID)'
FROM m;



INSERT INTO REPORTING.VALIDATION_RESULTS
(run_id,batch_ts,job_name,domain_nm,table_src,table_tgt,validation_type,kpi_nm,
 metric_value,metric_target,status,severity,owner_team,notes_short)
WITH last_dt AS (
  SELECT DATEDIFF('minute', MAX(EDL_INCRMNTL_LOAD_DTM), CURRENT_TIMESTAMP()) AS mins
  FROM HOSCDA_EXTRACT.HLTH_OS_CDA_PATIENT
)
SELECT
  TO_VARCHAR(CURRENT_TIMESTAMP()), CURRENT_TIMESTAMP(), 'Static PATIENT run','Patient',
  'HOSCDA.HLTH_OS_CDA_PATIENT','HOSCDA_EXTRACT.HLTH_OS_CDA_PATIENT','FRESHNESS','TIMELINESS',
  last_dt.mins, 60,
  CASE WHEN mins <= 60 THEN 'PASS' WHEN mins <= 120 THEN 'WARN' ELSE 'FAIL' END,
  CASE WHEN mins <= 60 THEN 'INFO' WHEN mins <= 120 THEN 'WARN' ELSE 'ERROR' END,
  'Data Eng','Minutes since last TGT load'
FROM last_dt;







INSERT INTO REPORTING.VALIDATION_RESULTS
(run_id,batch_ts,job_name,domain_nm,table_src,table_tgt,validation_type,kpi_nm,
 metric_value,metric_target,status,severity,src_count,tgt_count,owner_team,notes_short)
WITH latest AS (
  SELECT COALESCE(
    (SELECT MAX(EDL_RUN_ID) FROM HOSCDA_EXTRACT.HLTH_OS_CDA_ENCNTR),
    (SELECT MAX(EDL_RUN_ID) FROM HOSCDA.HLTH_OS_CDA_ENCNTR)
  ) AS rid
),
src AS (
  SELECT COUNT(*) c FROM HOSCDA.HLTH_OS_CDA_ENCNTR s, latest
  WHERE s.EDL_RUN_ID = latest.rid
),
tgt AS (
  SELECT COUNT(*) c FROM HOSCDA_EXTRACT.HLTH_OS_CDA_ENCNTR t, latest
  WHERE t.EDL_RUN_ID = latest.rid
)
SELECT
  TO_VARCHAR(CURRENT_TIMESTAMP()), CURRENT_TIMESTAMP(), 'Static ENCOUNTER run', 'Encounter',
  'HOSCDA.HLTH_OS_CDA_ENCNTR','HOSCDA_EXTRACT.HLTH_OS_CDA_ENCNTR','ROW_COVERAGE','COMPLETENESS',
  IFF(s.c=0,NULL,t.c/NULLIF(s.c,0)) AS metric_value,
  0.99, 
  CASE WHEN s.c=0 THEN 'FAIL'
       WHEN t.c/NULLIF(s.c,0) >= 0.99 THEN 'PASS'
       WHEN t.c/NULLIF(s.c,0) >= 0.98 THEN 'WARN'
       ELSE 'FAIL' END,
  CASE WHEN s.c=0 THEN 'ERROR'
       WHEN t.c/NULLIF(s.c,0) >= 0.99 THEN 'INFO'
       WHEN t.c/NULLIF(s.c,0) >= 0.98 THEN 'WARN'
       ELSE 'ERROR' END,
  s.c, t.c, 'Ingestion', 'Row coverage (tgt/src) for latest run'
FROM src s, tgt t;


-- Only in SRC
INSERT INTO REPORTING.VALIDATION_RESULTS
(run_id,batch_ts,job_name,domain_nm,table_src,table_tgt,validation_type,kpi_nm,
 status,severity,only_in_src_cnt,owner_team,notes_short,sample_keys)
WITH latest AS (
  SELECT COALESCE(
    (SELECT MAX(EDL_RUN_ID) FROM HOSCDA_EXTRACT.HLTH_OS_CDA_ENCNTR),
    (SELECT MAX(EDL_RUN_ID) FROM HOSCDA.HLTH_OS_CDA_ENCNTR)
  ) AS rid
),
s AS (SELECT ENCNTR_ID FROM HOSCDA.HLTH_OS_CDA_ENCNTR s, latest WHERE s.EDL_RUN_ID = latest.rid),
t AS (SELECT ENCNTR_ID FROM HOSCDA_EXTRACT.HLTH_OS_CDA_ENCNTR t, latest WHERE t.EDL_RUN_ID = latest.rid),
only_src AS (SELECT s.ENCNTR_ID FROM s LEFT JOIN t USING (ENCNTR_ID) WHERE t.ENCNTR_ID IS NULL),
samp AS (SELECT ARRAY_AGG(OBJECT_CONSTRUCT('ENCNTR_ID',ENCNTR_ID))[:10] a FROM only_src)
SELECT
  TO_VARCHAR(CURRENT_TIMESTAMP()), CURRENT_TIMESTAMP(), 'Static ENCOUNTER run','Encounter',
  'HOSCDA.HLTH_OS_CDA_ENCNTR','HOSCDA_EXTRACT.HLTH_OS_CDA_ENCNTR','MINUS_SRC','ACCURACY',
  IFF(COUNT(*)=0,'PASS','WARN'), IFF(COUNT(*)=0,'INFO','WARN'),
  COUNT(*),'Ingestion','Rows only in SRC',(SELECT TO_JSON(a) FROM samp)
FROM only_src;

-- Only in TGT
INSERT INTO REPORTING.VALIDATION_RESULTS
(run_id,batch_ts,job_name,domain_nm,table_src,table_tgt,validation_type,kpi_nm,
 status,severity,only_in_tgt_cnt,owner_team,notes_short,sample_keys)
WITH latest AS (
  SELECT COALESCE(
    (SELECT MAX(EDL_RUN_ID) FROM HOSCDA_EXTRACT.HLTH_OS_CDA_ENCNTR),
    (SELECT MAX(EDL_RUN_ID) FROM HOSCDA.HLTH_OS_CDA_ENCNTR)
  ) AS rid
),
s AS (SELECT ENCNTR_ID FROM HOSCDA.HLTH_OS_CDA_ENCNTR s, latest WHERE s.EDL_RUN_ID = latest.rid),
t AS (SELECT ENCNTR_ID FROM HOSCDA_EXTRACT.HLTH_OS_CDA_ENCNTR t, latest WHERE t.EDL_RUN_ID = latest.rid),
only_tgt AS (SELECT t.ENCNTR_ID FROM t LEFT JOIN s USING (ENCNTR_ID) WHERE s.ENCNTR_ID IS NULL),
samp AS (SELECT ARRAY_AGG(OBJECT_CONSTRUCT('ENCNTR_ID',ENCNTR_ID))[:10] a FROM only_tgt)
SELECT
  TO_VARCHAR(CURRENT_TIMESTAMP()), CURRENT_TIMESTAMP(), 'Static ENCOUNTER run','Encounter',
  'HOSCDA.HLTH_OS_CDA_ENCNTR','HOSCDA_EXTRACT.HLTH_OS_CDA_ENCNTR','MINUS_TGT','ACCURACY',
  IFF(COUNT(*)=0,'PASS','ERROR'), IFF(COUNT(*)=0,'INFO','ERROR'),
  COUNT(*),'Ingestion','Rows only in TGT',(SELECT TO_JSON(a) FROM samp)
FROM only_tgt;



INSERT INTO REPORTING.VALIDATION_RESULTS
(run_id,batch_ts,job_name,domain_nm,table_src,table_tgt,validation_type,kpi_nm,
 metric_value,metric_target,status,severity,mismatch_cnt,owner_team,notes_short)
WITH latest AS (
  SELECT COALESCE(
    (SELECT MAX(EDL_RUN_ID) FROM HOSCDA_EXTRACT.HLTH_OS_CDA_ENCNTR),
    (SELECT MAX(EDL_RUN_ID) FROM HOSCDA.HLTH_OS_CDA_ENCNTR)
  ) AS rid
),
j AS (
  SELECT s.ENCNTR_ID,
         s.PAT_GUID AS s_PAT_GUID, t.PAT_GUID AS t_PAT_GUID,
         TO_VARCHAR(DATE_TRUNC('SECOND',TRY_TO_TIMESTAMP_NTZ(s.ENCOUNTER_DT))) AS s_ENCOUNTER_DT,
         TO_VARCHAR(DATE_TRUNC('SECOND',TRY_TO_TIMESTAMP_NTZ(t.ENCOUNTER_DT))) AS t_ENCOUNTER_DT,
         UPPER(TRIM(s.ENCNTR_TYPE_CD)) AS s_ENCNTR_TYPE_CD,
         UPPER(TRIM(t.ENCNTR_TYPE_CD)) AS t_ENCNTR_TYPE_CD
  FROM HOSCDA.HLTH_OS_CDA_ENCNTR s
  JOIN HOSCDA_EXTRACT.HLTH_OS_CDA_ENCNTR t USING (ENCNTR_ID)
  JOIN latest
  WHERE s.EDL_RUN_ID = latest.rid AND t.EDL_RUN_ID = latest.rid
),
tot AS (SELECT COUNT(*) c FROM j),
bad AS (
  SELECT COUNT(*) c
  FROM j
  WHERE NOT (
    (s_PAT_GUID IS NULL AND t_PAT_GUID IS NULL OR s_PAT_GUID = t_PAT_GUID)
    AND (s_ENCOUNTER_DT IS NULL AND t_ENCOUNTER_DT IS NULL OR s_ENCOUNTER_DT = t_ENCOUNTER_DT)
    AND (s_ENCNTR_TYPE_CD IS NULL AND t_ENCNTR_TYPE_CD IS NULL OR s_ENCNTR_TYPE_CD = t_ENCNTR_TYPE_CD)
  )
)
SELECT
  TO_VARCHAR(CURRENT_TIMESTAMP()), CURRENT_TIMESTAMP(), 'Static ENCOUNTER run','Encounter',
  'HOSCDA.HLTH_OS_CDA_ENCNTR','HOSCDA_EXTRACT.HLTH_OS_CDA_ENCNTR','VALUE_MISMATCH','ACCURACY',
  IFF(tot.c=0,0,bad.c/NULLIF(tot.c,0)), 0.005,
  CASE WHEN tot.c=0 THEN 'PASS'
       WHEN bad.c/NULLIF(tot.c,0) <= 0.005 THEN 'PASS'
       WHEN bad.c/NULLIF(tot.c,0) <= 0.01  THEN 'WARN'
       ELSE 'FAIL' END,
  CASE WHEN tot.c=0 THEN 'INFO'
       WHEN bad.c/NULLIF(tot.c,0) <= 0.005 THEN 'INFO'
       WHEN bad.c/NULLIF(tot.c,0) <= 0.01  THEN 'WARN'
       ELSE 'ERROR' END,
  bad.c,'Ingestion','Encounter value parity (normalized)'
FROM tot, bad;



INSERT INTO REPORTING.VALIDATION_RESULTS
(run_id,batch_ts,job_name,domain_nm,table_src,table_tgt,validation_type,kpi_nm,
 metric_value,metric_target,status,severity,null_pct_mandatory,owner_team,notes_short)
WITH latest AS (
  SELECT COALESCE(
    (SELECT MAX(EDL_RUN_ID) FROM HOSCDA_EXTRACT.HLTH_OS_CDA_ENCNTR),
    (SELECT MAX(EDL_RUN_ID) FROM HOSCDA.HLTH_OS_CDA_ENCNTR)
  ) AS rid
),
scoped AS (
  SELECT * FROM HOSCDA_EXTRACT.HLTH_OS_CDA_ENCNTR t, latest
  WHERE t.EDL_RUN_ID = latest.rid
),
m AS (
  SELECT AVG(IFF(TRIM(t.PAT_GUID) IS NULL OR TRIM(t.PAT_GUID)='',1,0)) AS null_pct
  FROM scoped t
)
SELECT
  TO_VARCHAR(CURRENT_TIMESTAMP()), CURRENT_TIMESTAMP(), 'Static ENCOUNTER run','Encounter',
  'HOSCDA.HLTH_OS_CDA_ENCNTR','HOSCDA_EXTRACT.HLTH_OS_CDA_ENCNTR','NULL_STATS','COMPLETENESS',
  m.null_pct, 0.01,
  CASE WHEN m.null_pct <= 0.01 THEN 'PASS'
       WHEN m.null_pct <= 0.03 THEN 'WARN'
       ELSE 'FAIL' END,
  CASE WHEN m.null_pct <= 0.01 THEN 'INFO'
       WHEN m.null_pct <= 0.03 THEN 'WARN'
       ELSE 'ERROR' END,
  m.null_pct,'Ingestion','Mandatory NULL% (PAT_GUID)'
FROM m;


INSERT INTO REPORTING.VALIDATION_RESULTS
(run_id,batch_ts,job_name,domain_nm,table_src,table_tgt,validation_type,kpi_nm,
 metric_value,metric_target,status,severity,owner_team,notes_short)
WITH last_dt AS (
  SELECT DATEDIFF('minute', MAX(EDL_INCRMNTL_LOAD_DTM), CURRENT_TIMESTAMP()) AS mins
  FROM HOSCDA_EXTRACT.HLTH_OS_CDA_ENCNTR
)
SELECT
  TO_VARCHAR(CURRENT_TIMESTAMP()), CURRENT_TIMESTAMP(), 'Static ENCOUNTER run','Encounter',
  'HOSCDA.HLTH_OS_CDA_ENCNTR','HOSCDA_EXTRACT.HLTH_OS_CDA_ENCNTR','FRESHNESS','TIMELINESS',
  mins, 60,
  CASE WHEN mins <= 60 THEN 'PASS' WHEN mins <= 120 THEN 'WARN' ELSE 'FAIL' END,
  CASE WHEN mins <= 60 THEN 'INFO' WHEN mins <= 120 THEN 'WARN' ELSE 'ERROR' END,
  'Ingestion','Minutes since last TGT load'
FROM last_dt;
