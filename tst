
-----------------------------------------------------------------------
-- AUTO-DETECT DATE WINDOW BASED ON RAWZ/STG/TGT LOAD DATES
-- We take LEAST(last RAWZ load, last STG load, last TGT load)
-- so the comparison window is consistent across all 3 layers.
-----------------------------------------------------------------------
DECLARE
    START_TS           TIMESTAMP_NTZ;
    END_TS             TIMESTAMP_NTZ;

    RAWZ_LAST_LOAD     TIMESTAMP_NTZ;
    STG_LAST_LOAD      TIMESTAMP_NTZ;
    TGT_LAST_LOAD      TIMESTAMP_NTZ;
BEGIN

    -- Latest RAWZ load timestamp
    SELECT MAX(LOAD_DTM)
    INTO RAWZ_LAST_LOAD
    FROM HOSCDA_RAWZ.HLTH_OS_CDA_PRVNANCE_RAWZ;

    -- Latest STG load timestamp
    SELECT MAX(LOAD_DTM)
    INTO STG_LAST_LOAD
    FROM HOSCDA_STG.HLTH_OS_CDA_PRVNANCE_STG;

    -- Latest TARGET load timestamp
    SELECT MAX(LOAD_DTM)
    INTO TGT_LAST_LOAD
    FROM HOSCDA.HLTH_OS_CDA_PRVNANCE;

    -- Final comparison window (smallest of the 3)
    END_TS := LEAST(
        RAWZ_LAST_LOAD::DATE,
        STG_LAST_LOAD::DATE,
        TGT_LAST_LOAD::DATE
    );

    -- Compare only 1-day window prior to END_TS
    START_TS := DATEADD('DAY', -1, END_TS);


-----------------------------------------------------------------------
-- PARAMETER TABLE (RA_CODE ↔ RA_NAME Mapping)
-- This ensures we always map RA_MNEMONIC → Friendly Name
-----------------------------------------------------------------------
    CREATE OR REPLACE TEMP TABLE PARM AS
    SELECT 
        RA_CODE.VALUE AS RA_CODE,
        RA_NAME.VALUE AS RA_NAME
    FROM
        (SELECT RESOURCEID, VALUE
         FROM HOSCDA.HLTH_OS_CDA_PARM
         WHERE NAME = 'raMnemonic'
         GROUP BY ALL) RA_CODE
    JOIN
        (SELECT RESOURCEID, VALUE
         FROM HOSCDA.HLTH_OS_CDA_PARM
         WHERE NAME = 'raName'
         GROUP BY ALL) RA_NAME
    ON RA_CODE.RESOURCEID = RA_NAME.RESOURCEID
    GROUP BY ALL;


-----------------------------------------------------------------------
-- RAWZ DOCUMENT REPOSITORY
-- Extracts all documents from RAWZ layer within window.
-- NOT DEDUPED intentionally — every document counts.
-----------------------------------------------------------------------
    CREATE OR REPLACE TEMP TABLE DOC_REPO_RAWZ AS
    SELECT
        SPLIT_PART(DCMNT.EDL_RUN_ID, '^', 1) AS RA_CODE,

        -------------------------------------------------------------------
        -- Document Classification Rules
        -------------------------------------------------------------------
        CASE 
            WHEN DCMNT.EDL_RUN_ID ILIKE '%^CCR%' THEN 'CCR'
            WHEN DCMNT.DCMNT_TYPE_CD ILIKE ANY ('{MDM%','ORU^R01','VXU^V04}')
                 AND DCMNT_CNTNT.ATCHMNT_TTL_NM = 'HL7Vx' THEN '1-HL7-MSG'
            WHEN DCMNT.DCMNT_TYPE_CD ILIKE '%BINARY%' THEN '1-2-PDF-DOC'
            WHEN DCMNT.DCMNT_TYPE_CD = 'CCDA'
                 OR DCMNT_CNTNT.ATCHMNT_TTL_NM ILIKE ANY ('{CCDA','Continuity of Care Document%','B64}')
                 THEN '3-CCDA'
            WHEN DCMNT_CNTNT.ATCHMNT_TTL_NM ILIKE ANY ('{CCDA','B64}')
                 THEN '3-CCDA'
            WHEN DCMNT.DCMNT_TYPE_CD ILIKE ANY ('{ADT^A%','CUSTOM_EXPERIAN}')
                 THEN '4-ADT'
            ELSE '5-OTHER'
        END AS DCMNT_TYPE,

        DCMNT.DCMNT_TYPE_CD,
        DCMNT_CNTNT.ATCHMNT_TTL_NM,

        CAST(hoscda_appcode.CONVERT_STRING_TO_TIMESTAMP_NTZ(PRV.PRVNANCE_PRD_END_DTM) AS DATE) AS LOAD_DT

    FROM HOSCDA_RAWZ.HLTH_OS_CDA_DCMNT_RFRNC_RAWZ DCMNT
    JOIN HOSCDA_RAWZ.HLTH_OS_CDA_PRVNANCE_RAWZ PRV
        ON DCMNT.SRC_NM    = PRV.SRC_NM
       AND DCMNT.EDL_RUN_ID= PRV.EDL_RUN_ID
       AND PRV.LOAD_DTM   >= :START_TS
       AND PRV.LOAD_DTM   <  :END_TS

    JOIN HOSCDA_RAWZ.HLTH_OS_CDA_DCMNT_RFRNC_ID_RAWZ DCMNT_ID
        ON DCMNT.DCMNT_RFRNC_ID = DCMNT_ID.RSRC_ID
       AND DCMNT.EDL_RUN_ID     = DCMNT_ID.EDL_RUN_ID

    JOIN HOSCDA_RAWZ.HLTH_OS_CDA_DCMNT_RFRNC_CNTNT_RAWZ DCMNT_CNTNT
        ON DCMNT.DCMNT_RFRNC_ID = DCMNT_CNTNT.RSRC_ID
       AND DCMNT.EDL_RUN_ID     = DCMNT_CNTNT.EDL_RUN_ID
       AND DCMNT_CNTNT.ATCHMNT_TTL_NM NOT IN ('CODEX', 'DOCMETADATA', 'DOCASCII')

    WHERE DCMNT.DCMNT_TYPE_CD IS NOT NULL
      AND DCMNT.DCMNT_TYPE_CD <> 'CCDA';


-----------------------------------------------------------------------
-- STG DOCUMENT REPOSITORY
-- (Same classification as RAWZ)
-----------------------------------------------------------------------
    CREATE OR REPLACE TEMP TABLE DOC_REPO_STG AS
    SELECT
        SPLIT_PART(DCMNT.EDL_RUN_ID, '^', 1) AS RA_CODE,
        CASE 
            WHEN DCMNT.EDL_RUN_ID ILIKE '%^CCR%' THEN 'CCR'
            WHEN DCMNT.DCMNT_TYPE_CD ILIKE ANY ('{MDM%','ORU^R01','VXU^V04}')
                 AND DCMNT_CNTNT.ATCHMNT_TTL_NM = 'HL7Vx' THEN '1-HL7-MSG'
            WHEN DCMNT.DCMNT_TYPE_CD ILIKE '%BINARY%' THEN '1-2-PDF-DOC'
            WHEN DCMNT.DCMNT_TYPE_CD = 'CCDA'
                 OR DCMNT_CNTNT.ATCHMNT_TTL_NM ILIKE ANY ('{CCDA','Continuity of Care Document%','B64}')
                 THEN '3-CCDA'
            WHEN DCMNT_CNTNT.ATCHMNT_TTL_NM ILIKE ANY ('{CCDA','B64}') THEN '3-CCDA'
            WHEN DCMNT.DCMNT_TYPE_CD ILIKE ANY ('{ADT^A%','CUSTOM_EXPERIAN}') THEN '4-ADT'
            ELSE '5-OTHER'
        END AS DCMNT_TYPE,
        DCMNT.DCMNT_TYPE_CD,
        DCMNT_CNTNT.ATCHMNT_TTL_NM,
        CAST(hoscda_appcode.CONVERT_STRING_TO_TIMESTAMP_NTZ(PRV.PRVNANCE_PRD_END_DTM) AS DATE) AS LOAD_DT

    FROM HOSCDA_STG.HLTH_OS_CDA_DCMNT_RFRNC_STG DCMNT
    JOIN HOSCDA_STG.HLTH_OS_CDA_PRVNANCE_STG PRV
        ON DCMNT.SRC_NM = PRV.SRC_NM
       AND DCMNT.EDL_RUN_ID = PRV.EDL_RUN_ID
       AND PRV.LOAD_DTM >= :START_TS
       AND PRV.LOAD_DTM <  :END_TS

    JOIN HOSCDA_STG.HLTH_OS_CDA_DCMNT_RFRNC_ID_STG DCMNT_ID
        ON DCMNT.DCMNT_RFRNC_ID = DCMNT_ID.RSRC_ID
       AND DCMNT.EDL_RUN_ID     = DCMNT_ID.EDL_RUN_ID

    JOIN HOSCDA_STG.HLTH_OS_CDA_DCMNT_RFRNC_CNTNT_STG DCMNT_CNTNT
        ON DCMNT.DCMNT_RFRNC_ID = DCMNT_CNTNT.RSRC_ID
       AND DCMNT.EDL_RUN_ID     = DCMNT_CNTNT.EDL_RUN_ID
       AND DCMNT_CNTNT.ATCHMNT_TTL_NM NOT IN ('CODEX','DOCMETADATA','DOCASCII')

    WHERE DCMNT.DCMNT_TYPE_CD IS NOT NULL
      AND DCMNT.DCMNT_TYPE_CD <> 'CCDA';


-----------------------------------------------------------------------
-- TARGET (TGT) DOCUMENT REPOSITORY
-----------------------------------------------------------------------
    CREATE OR REPLACE TEMP TABLE DOC_REPO_TGT AS
    SELECT
        SPLIT_PART(DCMNT.EDL_RUN_ID, '^', 1) AS RA_CODE,
        CASE 
            WHEN DCMNT.EDL_RUN_ID ILIKE '%^CCR%' THEN 'CCR'
            WHEN DCMNT.DCMNT_TYPE_CD ILIKE ANY ('{MDM%','ORU^R01','VXU^V04}')
                 AND DCMNT_CNTNT.ATCHMNT_TTL_NM = 'HL7Vx' THEN '1-HL7-MSG'
            WHEN DCMNT.DCMNT_TYPE_CD ILIKE '%BINARY%' THEN '1-2-PDF-DOC'
            WHEN DCMNT.DCMNT_TYPE_CD = 'CCDA'
                 OR DCMNT_CNTNT.ATCHMNT_TTL_NM ILIKE ANY ('{CCDA','Continuity of Care Document%','B64}')
                 THEN '3-CCDA'
            WHEN DCMNT_CNTNT.ATCHMNT_TTL_NM ILIKE ANY ('{CCDA','B64}') THEN '3-CCDA'
            WHEN DCMNT.DCMNT_TYPE_CD ILIKE ANY ('{ADT^A%','CUSTOM_EXPERIAN}') THEN '4-ADT'
            ELSE '5-OTHER'
        END AS DCMNT_TYPE,
        DCMNT.DCMNT_TYPE_CD,
        DCMNT_CNTNT.ATCHMNT_TTL_NM,
        CAST(hoscda_appcode.CONVERT_STRING_TO_TIMESTAMP_NTZ(PRV.PRVNANCE_PRD_END_DTM) AS DATE) AS LOAD_DT

    FROM HOSCDA.HLTH_OS_CDA_DCMNT_RFRNC DCMNT
    JOIN HOSCDA.HLTH_OS_CDA_PRVNANCE PRV
        ON DCMNT.SRC_NM = PRV.SRC_NM
       AND DCMNT.EDL_RUN_ID = PRV.EDL_RUN_ID
       AND PRV.LOAD_DTM >= :START_TS
       AND PRV.LOAD_DTM <  :END_TS

    JOIN HOSCDA.HLTH_OS_CDA_DCMNT_RFRNC_ID DCMNT_ID
        ON DCMNT.DCMNT_RFRNC_ID = DCMNT_ID.RSRC_ID
       AND DCMNT.EDL_RUN_ID     = DCMNT_ID.EDL_RUN_ID

    JOIN HOSCDA.HLTH_OS_CDA_DCMNT_RFRNC_CNTNT DCMNT_CNTNT
        ON DCMNT.DCMNT_RFRNC_ID = DCMNT_CNTNT.RSRC_ID
       AND DCMNT.EDL_RUN_ID     = DCMNT_CNTNT.EDL_RUN_ID
       AND DCMNT_CNTNT.ATCHMNT_TTL_NM NOT IN ('CODEX','DOCMETADATA','DOCASCII')

    WHERE DCMNT.DCMNT_TYPE_CD IS NOT NULL
      AND DCMNT.DCMNT_TYPE_CD <> 'CCDA';


-----------------------------------------------------------------------
-- RAWZ / STG / TGT COUNTS (Grouped by RA + Date)
-----------------------------------------------------------------------
    CREATE OR REPLACE TEMP TABLE RAWZ_CNT AS
    SELECT RA_CODE, LOAD_DT, COUNT(*) AS RAWZ_RCRD_CNT
    FROM DOC_REPO_RAWZ
    GROUP BY RA_CODE, LOAD_DT;

    CREATE OR REPLACE TEMP TABLE STG_CNT AS
    SELECT RA_CODE, LOAD_DT, COUNT(*) AS STG_RCRD_CNT
    FROM DOC_REPO_STG
    GROUP BY RA_CODE, LOAD_DT;

    CREATE OR REPLACE TEMP TABLE TGT_CNT AS
    SELECT RA_CODE, LOAD_DT, COUNT(*) AS TRGT_RCRD_CNT
    FROM DOC_REPO_TGT
    GROUP BY RA_CODE, LOAD_DT;


-----------------------------------------------------------------------
-- DIMENSIONS (Unique RA, Type, Date)
-----------------------------------------------------------------------
    CREATE OR REPLACE TEMP TABLE RAWZ_DIM AS
    SELECT d.RA_CODE, p.RA_NAME, d.DCMNT_TYPE, d.DCMNT_TYPE_CD, d.LOAD_DT
    FROM DOC_REPO_RAWZ d
    JOIN PARM p ON d.RA_CODE = p.RA_CODE
    GROUP BY 1,2,3,4,5;

    CREATE OR REPLACE TEMP TABLE STG_DIM AS
    SELECT d.RA_CODE, p.RA_NAME, d.DCMNT_TYPE, d.DCMNT_TYPE_CD, d.LOAD_DT
    FROM DOC_REPO_STG d
    JOIN PARM p ON d.RA_CODE = p.RA_CODE
    GROUP BY 1,2,3,4,5;

    CREATE OR REPLACE TEMP TABLE TGT_DIM AS
    SELECT d.RA_CODE, p.RA_NAME, d.DCMNT_TYPE, d.DCMNT_TYPE_CD, d.LOAD_DT
    FROM DOC_REPO_TGT d
    JOIN PARM p ON d.RA_CODE = p.RA_CODE
    GROUP BY 1,2,3,4,5;


-----------------------------------------------------------------------
-- FINAL METRICS (Join RAWZ/STG/TGT counts to dimension)
-----------------------------------------------------------------------
    CREATE OR REPLACE TEMP TABLE FINAL_METRICS AS
    SELECT
        d.RA_CODE,
        d.RA_NAME,
        d.DCMNT_TYPE,
        d.DCMNT_TYPE_CD,
        d.LOAD_DT,
        COALESCE(r.RAWZ_RCRD_CNT, 0) AS RAWZ_RCRD_CNT,
        COALESCE(s.STG_RCRD_CNT, 0)  AS STG_RCRD_CNT,
        COALESCE(t.TRGT_RCRD_CNT, 0) AS TRGT_RCRD_CNT
    FROM RAWZ_DIM d
    LEFT JOIN RAWZ_CNT r ON d.RA_CODE = r.RA_CODE AND d.LOAD_DT = r.LOAD_DT
    LEFT JOIN STG_CNT s ON d.RA_CODE = s.RA_CODE AND d.LOAD_DT = s.LOAD_DT
    LEFT JOIN TGT_CNT t ON d.RA_CODE = t.RA_CODE AND d.LOAD_DT = t.LOAD_DT;


-----------------------------------------------------------------------
-- INSERT FINAL RESULT INTO METRICS TABLE
-----------------------------------------------------------------------
    INSERT INTO TEMP_DB.TEMP_SCHEMA.HLTH_OS_CDA_RECON_RPTG_MTRCS (
        EDL_LOAD_DTM, EDL_RUN_ID, EDL_SOR_CD, KF_TMS,
        EDL_SCRTY_LVL_CD, EDL_LOB_CD, EDL_EXTRNL_LOAD_CD,
        EDL_CREAT_DTM, EDL_INCRMNTL_LOAD_DTM,
        EDL_CLNT_ID, EDL_TNNT_ID, EDL_OFSHR_ACSBL_IND,
        RECNCLTN_UUID, RCRD_AUTHRTY_CD, RCRD_AUTHRTY_NM,
        DCMNT_TYPE_CD, LOAD_DTM,
        STG_RCRD_CNT, RAWZ_RCRD_CNT, TRGT_RCRD_CNT,
        ISSU_RCRD_CNT,
        SRC_RECNCLTN_1_CD, SRC_RECNCLTN_2_CD, SRC_RECNCLTN_3_CD, SRC_RECNCLTN_4_CD, SRC_RECNCLTN_5_CD
    )
    SELECT
        CURRENT_TIMESTAMP(),                     -- EDL_LOAD_DTM
        'NA',                                    -- EDL_RUN_ID
        'RA_CODE',                               -- EDL_SOR_CD
        CURRENT_TIMESTAMP(),                     -- KF_TMS
        'UNK',                                   -- EDL_SCRTY_LVL_CD
        'NA', 'NA',                              -- LOB / EXT_LOAD
        CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(),
        0, 0, 'N',
        UNIFORM(1, 9999999999999, RANDOM()),     -- UUID
        RA_CODE, RA_NAME, DCMNT_TYPE_CD, LOAD_DT,
        STG_RCRD_CNT, RAWZ_RCRD_CNT, TRGT_RCRD_CNT,
        0, NULL, NULL, NULL, NULL, NULL
    FROM FINAL_METRICS;

END;
