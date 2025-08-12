-- Optional: limit to one run while testing
-- SET test_run := 123456789;

WITH src AS (
  SELECT
    e.encntr_guid,
    e.edl_run_id,
    /* raw fields for inspection */
    TYP.ENCNTR_TYPE_DSPLY_TXT,
    TYP.ORGNL_TXT,
    /* which source is used */
    CASE WHEN NULLIF(TRIM(TYP.ENCNTR_TYPE_DSPLY_TXT), '') IS NOT NULL
         THEN 'DISPLAY' ELSE 'ORIGINAL' END AS source_used,
    /* recomputed value per your rule */
    COALESCE(NULLIF(TRIM(TYP.ENCNTR_TYPE_DSPLY_TXT), ''), TYP.ORGNL_TXT) AS src_pat_type_nm
  FROM hoscda.hlth_os_cda_encntr e
  LEFT JOIN hoscda.hlth_os_cda_encntr_type TYP
    ON e.encntr_id = TYP.rsrc_id
   AND e.edl_run_id = TYP.edl_run_id
  WHERE TYP.TRNSLTN_IND = '0'
)
SELECT
  tgt.edl_run_id,
  COUNT(*)                                      AS total_rows,
  SUM(CASE WHEN s.source_used='DISPLAY'  THEN 1 ELSE 0 END) AS used_display,
  SUM(CASE WHEN s.source_used='ORIGINAL' THEN 1 ELSE 0 END) AS used_original,
  SUM(CASE WHEN tgt.PAT_TYPE_NM = s.src_pat_type_nm THEN 1 ELSE 0 END) AS matches,
  SUM(CASE WHEN (tgt.PAT_TYPE_NM <> s.src_pat_type_nm)
                 OR (tgt.PAT_TYPE_NM IS NULL) <> (s.src_pat_type_nm IS NULL)
           THEN 1 ELSE 0 END) AS mismatches
FROM temp_db.temp_schema.HLTH_OS_RSKADJMNT_CHRT_GNRTN_HOSCDA_TEMP tgt
JOIN src s
  ON tgt.encntr_guid = s.encntr_guid
 AND tgt.edl_run_id  = s.edl_run_id
-- AND tgt.edl_run_id = $test_run
GROUP BY 1
ORDER BY 1;




WITH src AS (
  SELECT e.encntr_guid, e.edl_run_id,
         TYP.ENCNTR_TYPE_DSPLY_TXT, TYP.ORGNL_TXT,
         COALESCE(NULLIF(TRIM(TYP.ENCNTR_TYPE_DSPLY_TXT), ''), TYP.ORGNL_TXT) AS src_pat_type_nm
  FROM hoscda.hlth_os_cda_encntr e
  LEFT JOIN hoscda.hlth_os_cda_encntr_type TYP
    ON e.encntr_id = TYP.rsrc_id AND e.edl_run_id = TYP.edl_run_id
  WHERE TYP.TRNSLTN_IND = '0'
    AND NULLIF(TRIM(TYP.ENCNTR_TYPE_DSPLY_TXT), '') IS NOT NULL
)
SELECT
  tgt.edl_run_id,
  tgt.encntr_guid,
  s.ENCNTR_TYPE_DSPLY_TXT,
  s.ORGNL_TXT,
  s.src_pat_type_nm   AS recomputed,
  tgt.PAT_TYPE_NM     AS in_target
FROM temp_db.temp_schema.HLTH_OS_RSKADJMNT_CHRT_GNRTN_HOSCDA_TEMP tgt
JOIN src s
  ON tgt.encntr_guid = s.encntr_guid
 AND tgt.edl_run_id  = s.edl_run_id
-- AND tgt.edl_run_id = $test_run
LIMIT 50;



WITH src AS (
  SELECT e.encntr_guid, e.edl_run_id,
         TYP.ENCNTR_TYPE_DSPLY_TXT, TYP.ORGNL_TXT,
         COALESCE(NULLIF(TRIM(TYP.ENCNTR_TYPE_DSPLY_TXT), ''), TYP.ORGNL_TXT) AS src_pat_type_nm
  FROM hoscda.hlth_os_cda_encntr e
  LEFT JOIN hoscda.hlth_os_cda_encntr_type TYP
    ON e.encntr_id = TYP.rsrc_id AND e.edl_run_id = TYP.edl_run_id
  WHERE TYP.TRNSLTN_IND = '0'
    AND NULLIF(TRIM(TYP.ENCNTR_TYPE_DSPLY_TXT), '') IS NULL
)
SELECT
  tgt.edl_run_id,
  tgt.encntr_guid,
  s.ENCNTR_TYPE_DSPLY_TXT AS display_blank,
  s.ORGNL_TXT,
  s.src_pat_type_nm       AS recomputed,
  tgt.PAT_TYPE_NM         AS in_target
FROM temp_db.temp_schema.HLTH_OS_RSKADJMNT_CHRT_GNRTN_HOSCDA_TEMP tgt
JOIN src s
  ON tgt.encntr_guid = s.encntr_guid
 AND tgt.edl_run_id  = s.edl_run_id
-- AND tgt.edl_run_id = $test_run
LIMIT 50;



WITH src AS (
  SELECT e.encntr_guid, e.edl_run_id,
         TYP.ENCNTR_TYPE_DSPLY_TXT, TYP.ORGNL_TXT,
         COALESCE(NULLIF(TRIM(TYP.ENCNTR_TYPE_DSPLY_TXT), ''), TYP.ORGNL_TXT) AS src_pat_type_nm
  FROM hoscda.hlth_os_cda_encntr e
  LEFT JOIN hoscda.hlth_os_cda_encntr_type TYP
    ON e.encntr_id = TYP.rsrc_id AND e.edl_run_id = TYP.edl_run_id
  WHERE TYP.TRNSLTN_IND = '0'
)
SELECT
  tgt.edl_run_id,
  tgt.encntr_guid,
  s.ENCNTR_TYPE_DSPLY_TXT,
  s.ORGNL_TXT,
  s.src_pat_type_nm   AS recomputed,
  tgt.PAT_TYPE_NM     AS in_target
FROM temp_db.temp_schema.HLTH_OS_RSKADJMNT_CHRT_GNRTN_HOSCDA_TEMP tgt
JOIN src s
  ON tgt.encntr_guid = s.encntr_guid
 AND tgt.edl_run_id  = s.edl_run_id
WHERE (tgt.PAT_TYPE_NM <> s.src_pat_type_nm)
   OR ((tgt.PAT_TYPE_NM IS NULL) <> (s.src_pat_type_nm IS NULL))
-- AND tgt.edl_run_id = $test_run
ORDER BY tgt.edl_run_id
LIMIT 100;


SELECT
  COUNT(*) AS total_typ_rows,
  SUM(CASE WHEN NULLIF(TRIM(ENCNTR_TYPE_DSPLY_TXT),'') IS NULL THEN 1 ELSE 0 END) AS display_blank,
  SUM(CASE WHEN NULLIF(TRIM(ENCNTR_TYPE_DSPLY_TXT),'') IS NOT NULL THEN 1 ELSE 0 END) AS display_present
FROM hoscda.hlth_os_cda_encntr_type
WHERE TRNSLTN_IND='0';

