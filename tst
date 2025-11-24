-- ============================================================
--   HOSCDA EXTRACT (TARGET)
-- ============================================================
WITH HOSCDA AS (
    SELECT 
          OBSRVTN.OBSRVTN_VAL_TXT             AS HOSCDA_OBSRVTN_VAL_TXT,
          OBSRVTN.obsrvtn_val_type_nm,
          VAL_CD.OBSRVTN_DSPLY_TXT            AS HOSCDA_DISPLAY_TXT,
          VAL_CD.ORGLN_TXT                    AS HOSCDA_ORIGINAL_TXT,
          ENCNTR_ID.val_txt                   AS SRC_ENCNTR_ID,
          PAT.MCID,
          PAT.RCRD_AUTHRTY_NM,
          VAL_CD.CD_TRNSLTNG_ID               AS HOSCDA_CODE
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
          ON SPLIT_PART(OBSRVTN.subj_rfrnc_txt,'^',2) = PAT.PAT_GUID
    WHERE TRIM(OBSRVTN.OBSRVTN_VAL_TXT) <> ''
      AND OBSRVTN.obsrvtn_val_type_nm = 'CD'
      AND UPPER(OBSRVTN_CD.SYS_NM) NOT LIKE ANY ('{''LOINC%'',''SNOMED%'',''CVX%''}')
      AND LOWER(OBSRVTN_CD.trnsltng_id) LIKE ANY ('{''%C%'',''%D%''}')
      AND NOT (OBSRVTN_CD.trnsltng_id ILIKE ANY ('{''0%'',''1%'',''%CX%'',''%DX%''}'))
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
          OBSRVTN.CODE                     AS CDA_CODE
    FROM P01_CDA.CDA_ALLPHI.OBSRVTN OBSRVTN
    INNER JOIN P01_CDA.CDA_ALLPHI.PAT PAT
          ON PAT.PAT_GUID = OBSRVTN.PAT_GUID
    GROUP BY ALL
)

-- ============================================================
-- FINAL DYNAMIC MULTI-RECORD COMPARISON
-- ============================================================
SELECT 
    COALESCE(CDA.SRC_ENCNTR_ID, HOSCDA.SRC_ENCNTR_ID)   AS SRC_ENCNTR_ID,
    COALESCE(CDA.MCID, HOSCDA.MCID)                     AS MCID,

    -- CDA VALUES
    CDA.CDA_OBSRVTN_VAL_TXT,
    CDA.CDA_CODE,

    -- HOSCDA VALUES
    HOSCDA.HOSCDA_OBSRVTN_VAL_TXT,
    HOSCDA.HOSCDA_CODE,
    HOSCDA.HOSCDA_DISPLAY_TXT,
    HOSCDA.HOSCDA_ORIGINAL_TXT,

    -- MATCH LOGIC
    CASE WHEN CDA.CDA_OBSRVTN_VAL_TXT = HOSCDA.HOSCDA_OBSRVTN_VAL_TXT
         THEN 'MATCH' ELSE 'MISMATCH' END AS VALUE_MATCH,

    CASE WHEN CDA.CDA_CODE = HOSCDA.HOSCDA_CODE
         THEN 'MATCH' ELSE 'MISMATCH' END AS CODE_MATCH,

    CASE WHEN CDA.SRC_ENCNTR_ID = HOSCDA.SRC_ENCNTR_ID
          AND CDA.MCID = HOSCDA.MCID
         THEN 'YES' ELSE 'NO' END AS SAME_PATIENT_ENCOUNTER

FROM CDA
FULL OUTER JOIN HOSCDA
      ON CDA.SRC_ENCNTR_ID = HOSCDA.SRC_ENCNTR_ID
     AND CDA.MCID = HOSCDA.MCID
     AND CDA.CDA_CODE = HOSCDA.HOSCDA_CODE
ORDER BY SRC_ENCNTR_ID, MCID;