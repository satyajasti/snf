PROC AS (
    SELECT 
        proc_cd.PROC_CD,
        proc_cda.EDL_RUN_ID,
        proc_cda.PROC_ID,
        proc_cda.SRC_NM
    FROM HLTH_OS_CDA_OBSRVTN OBSRVTN
    JOIN HLTH_OS_CDA_PROC proc_cda
        ON OBSRVTN.EDL_RUN_ID = proc_cda.EDL_RUN_ID
       AND OBSRVTN.SRC_NM = proc_cda.SRC_NM_TXT
    JOIN HLTH_OS_CDA_PROC_CD_RFRNC proc_cd
        ON proc_cda.EDL_RUN_ID = proc_cd.EDL_RUN_ID
       AND proc_cda.PROC_ID = proc_cd.RSRC_ID
)

, SAMPLE_ACCOUNTS AS (
    SELECT DISTINCT MCID
    FROM P01_CDA.CDA_ALLPHI.OBSRVTN
    LIMIT 10
)
WITH

-- ============================================================
--   10 MCIDs ONLY (Option 1)
-- ============================================================
SAMPLE_ACCOUNTS AS (
    SELECT DISTINCT MCID
    FROM P01_CDA.CDA_ALLPHI.OBSRVTN
    LIMIT 10
),

-- ============================================================
--   HOSCDA EXTRACT (TARGET)
-- ============================================================
HOSCDA AS (
    SELECT 
          OBSRVTN.OBSRVTN_VAL_TXT             AS HOSCDA_OBSRVTN_VAL_TXT,
          OBSRVTN.obsrvtn_val_type_nm,
          VAL_CD.OBSRVTN_DSPLY_TXT            AS HOSCDA_DISPLAY_TXT,
          VAL_CD.ORGLN_TXT                    AS HOSCDA_ORIGINAL_TXT,
          ENCNTR_ID.val_txt                   AS SRC_ENCNTR_ID,
          PAT.MCID,
          PAT.RCRD_AUTHRTY_NM,
          VAL_CD.CD_TRNSLTNG_ID               AS HOSCDA_CODE,
          OBSRVTN.EDL_RUN_ID                  AS HOSCDA_EDL_RUN_ID,
          OBSRVTN.SRC_NM                      AS HOSCDA_SRC_NM
    FROM HOSCDA_STG.HLTH_OS_CDA_OBSRVTN_STG OBSRVTN
    INNER JOIN HOSCDA_STG.HLTH_OS_CDA_OBSRVTN_CD_RFRNC_STG OBSRVTN_CD
          ON OBSRVTN.obsrvtn_id = OBSRVTN_CD.rsrc_id
         AND OBSRVTN.EDL_RUN_ID = OBSRVTN_CD.EDL_RUN_ID
    INNER JOIN HOSCDA_STG.HLTH_OS_CDA_ENCNTR_ID_RFRNC_STG ENCNTR_ID
          ON SPLIT_PART(OBSRVTN.ENCNTR_RFRNC_TXT,'^',2) = ENCNTR_ID.RSRC_ID
         AND OBSRVTN.EDL_RUN_ID = ENCNTR_ID.EDL_RUN_ID
    INNER JOIN HOSCDA_STG.HLTH_OS_CDA_OBSRVTN_CD_RFRNC_STG VAL_CD
          ON OBSRVTN.obsrvtn_id = VAL_CD.rsrc_id
         AND OBSRVTN.EDL_RUN_ID = VAL_CD.EDL_RUN_ID
    INNER JOIN HOSCDA_EXTRACT.PAT_ANTHM_ID PAT
          ON SPLIT_PART(OBSRVTN.subj_rfrnc_txt, '^', 2) = PAT.PAT_GUID
    WHERE TRIM(OBSRVTN.OBSRVTN_VAL_TXT) <> ''
      AND OBSRVTN.obsrvtn_val_type_nm = 'CD'
      AND PAT.MCID IN (SELECT MCID FROM SAMPLE_ACCOUNTS)
    GROUP BY ALL
),

-- ============================================================
--   CDA EXTRACT (SOURCE)
-- ============================================================
CDA AS (
    SELECT
          OBSRVTN.SRC_ENCNTR_ID,
          P01_protegrity.scrty_acs_cntrl.ANTM_AES256_DETOK(
                OBSRVTN.OBSRVTN_VAL_TXT
          )                               AS CDA_OBSRVTN_VAL_TXT,
          PAT.MCID,
          PAT.RCRD_AUTHRTY_NM,
          OBSRVTN.CODE                     AS CDA_CODE,
          OBSRVTN.EDL_RUN_ID               AS CDA_EDL_RUN_ID,
          OBSRVTN.SRC_NM                   AS CDA_SRC_NM
    FROM P01_CDA.CDA_ALLPHI.OBSRVTN OBSRVTN
    INNER JOIN P01_CDA.CDA_ALLPHI.PAT PAT
          ON PAT.PAT_GUID = OBSRVTN.PAT_GUID
    WHERE PAT.MCID IN (SELECT MCID FROM SAMPLE_ACCOUNTS)
    GROUP BY ALL
),

-- ============================================================
--   PROCEDURE CODE JOIN
-- ============================================================
PROC AS (
    SELECT 
        proc_cd.PROC_CD,
        proc_cda.EDL_RUN_ID,
        proc_cda.PROC_ID,
        proc_cda.SRC_NM_TXT
    FROM HLTH_OS_CDA_PROC proc_cda
    JOIN HLTH_OS_CDA_PROC_CD_RFRNC proc_cd
        ON proc_cda.EDL_RUN_ID = proc_cd.EDL_RUN_ID
       AND proc_cda.PROC_ID = proc_cd.RSRC_ID
)

-- ============================================================
--   FINAL COMPARISON WITH PROC CODE
-- ============================================================
SELECT 
    COALESCE(CDA.SRC_ENCNTR_ID, HOSCDA.SRC_ENCNTR_ID) AS SRC_ENCNTR_ID,
    COALESCE(CDA.MCID, HOSCDA.MCID) AS MCID,

    -- CDA
    CDA.CDA_OBSRVTN_VAL_TXT,
    CDA.CDA_CODE,

    -- HOSCDA
    HOSCDA.HOSCDA_OBSRVTN_VAL_TXT,
    HOSCDA.HOSCDA_CODE,
    HOSCDA.HOSCDA_DISPLAY_TXT,
    HOSCDA.HOSCDA_ORIGINAL_TXT,

    -- PROC CODE
    PROC.PROC_CD AS PROCEDURE_CODE,

    -- COMPARISON FLAGS
    CASE 
        WHEN CDA.CDA_OBSRVTN_VAL_TXT = HOSCDA.HOSCDA_OBSRVTN_VAL_TXT 
        THEN 'MATCH' ELSE 'MISMATCH' END AS VALUE_MATCH,

    CASE WHEN CDA.CDA_CODE = HOSCDA.HOSCDA_CODE 
        THEN 'MATCH' ELSE 'MISMATCH' END AS CODE_MATCH

FROM CDA
FULL OUTER JOIN HOSCDA
      ON CDA.SRC_ENCNTR_ID = HOSCDA.SRC_ENCNTR_ID
     AND CDA.MCID = HOSCDA.MCID
     AND CDA.CDA_CODE = HOSCDA.HOSCDA_CODE
LEFT JOIN PROC
     ON HOSCDA.HOSCDA_EDL_RUN_ID = PROC.EDL_RUN_ID
    AND HOSCDA.HOSCDA_SRC_NM = PROC.SRC_NM_TXT

ORDER BY SRC_ENCNTR_ID, MCID;
