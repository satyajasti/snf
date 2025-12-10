-- ============================================================
-- VALIDATION 1:
-- RECORDS PRESENT IN ISSUE_LOG BUT NOT PRESENT IN CLNCL_ENCNTR
-- ============================================================

SELECT 
      LOG.JOIN_KEY_FLD_NM AS ri_log_key,
      LOG.EDL_RUN_ID,
      LOG.EDL_LOAD_DTM
FROM S01_HOSCDA.HOSCDA_ALLPHI_NOGBD.HLTH_OS_CDA_RI_ISSU_LOG LOG
LEFT JOIN CLNCL_ENCNTR C
       ON C.ENCNTR_GUID = LOG.JOIN_KEY_FLD_NM
      AND C.EDL_RUN_ID  = LOG.EDL_RUN_ID
WHERE C.ENCNTR_GUID IS NULL
-- Optional date filter
-- AND TO_CHAR(LOG.EDL_LOAD_DTM,'YYYY-MM-DD') = '2025-12-09'
;

-- ============================================================
-- VALIDATION 2:
-- RECORDS PRESENT IN CLNCL_ENCNTR BUT NOT PRESENT IN ISSUE_LOG
-- ============================================================

SELECT
      C.ENCNTR_GUID AS encounter_guid,
      C.EDL_RUN_ID,
      C.EDL_LOAD_DTM
FROM CLNCL_ENCNTR C
LEFT JOIN S01_HOSCDA.HOSCDA_ALLPHI_NOGBD.HLTH_OS_CDA_RI_ISSU_LOG LOG
       ON LOG.JOIN_KEY_FLD_NM = C.ENCNTR_GUID
      AND LOG.EDL_RUN_ID      = C.EDL_RUN_ID
WHERE LOG.JOIN_KEY_FLD_NM IS NULL
-- Optional date filter
-- AND TO_CHAR(C.EDL_LOAD_DTM,'YYYY-MM-DD') = '2025-12-09'
;