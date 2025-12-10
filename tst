-- ============================================================
-- MASTER PARENT–CHILD VALIDATION SCRIPT
-- Parent: HLTH_OS_CDA_ENCNTR
-- Child : HLTH_OS_CDA_PROC
-- Date  : 2025-12-09  (Change in v_date only)
-- ============================================================

-- Set date parameter
SET v_date = '2025-12-09';

-- ============================================================
-- COMMON CTEs FOR REUSE
-- ============================================================

WITH parent_tbl AS (
    SELECT *
    FROM S01_HOSCDA.HOSCDA_ALLPHI_NOGBD.HLTH_OS_CDA_ENCNTR
    WHERE TO_CHAR(EDL_LOAD_DTM,'YYYY-MM-DD') = $v_date
),

child_tbl AS (
    SELECT *
    FROM S01_HOSCDA.HOSCDA_ALLPHI_NOGBD.HLTH_OS_CDA_PROC
    WHERE TO_CHAR(EDL_LOAD_DTM,'YYYY-MM-DD') = $v_date
),

lkp AS (
    SELECT *
    FROM U01_HOSCDA.HOSCDA_ALLPHI_NOGBD.HLTH_OS_SRC_SYS_LKUP
    WHERE SRC_STTS_CD = 'Y'
      AND CNSMR_APLCTN_NM = 'CREM'
),

-- ============================================================
-- 1️⃣ CHILD PRESENT BUT PARENT MISSING (VALID LKP)
-- ============================================================
missing_parent AS (
    SELECT 
          p.*,
          L.RCRD_AUTHRTY_CD
    FROM child_tbl p
    LEFT JOIN parent_tbl e
           ON p.ENCNTR_ID = e.ENCNTR_ID
          AND p.EDL_RUN_ID = e.EDL_RUN_ID
    INNER JOIN lkp L
           ON UPPER(SPLIT_PART(p.EDL_RUN_ID, '^', 1)) = UPPER(L.RCRD_AUTHRTY_CD)
    WHERE e.ENCNTR_ID IS NULL
),

-- ============================================================
-- 2️⃣ PARENT PRESENT BUT CHILD MISSING
-- ============================================================
missing_child AS (
    SELECT 
          e.*
    FROM parent_tbl e
    LEFT JOIN child_tbl p
           ON e.ENCNTR_ID = p.ENCNTR_ID
          AND e.EDL_RUN_ID = p.EDL_RUN_ID
    WHERE p.ENCNTR_ID IS NULL
),

-- ============================================================
-- 3️⃣ PARENT–CHILD KEY MISMATCH (RUN_ID mismatch)
-- ============================================================
mismatch_keys AS (
    SELECT
          p.EDL_RUN_ID AS proc_run_id,
          e.EDL_RUN_ID AS encntr_run_id,
          p.ENCNTR_ID AS proc_encntr_id,
          e.ENCNTR_ID AS encntr_encntr_id,
          p.PROC_ID
    FROM child_tbl p
    JOIN parent_tbl e
         ON p.ENCNTR_ID = e.ENCNTR_ID
    WHERE p.EDL_RUN_ID <> e.EDL_RUN_ID
)

-- ============================================================
-- 4️⃣ PRINT RESULTS
-- ============================================================

-- Missing Parent (Child without Parent)
SELECT 'CHILD_WITHOUT_PARENT' AS validation_type, * 
FROM missing_parent
ORDER BY EDL_RUN_ID;

-- Missing Child (Parent without Child)
SELECT 'PARENT_WITHOUT_CHILD' AS validation_type, *
FROM missing_child
ORDER BY EDL_RUN_ID;

-- Key Mismatches
SELECT 'KEY_MISMATCH' AS validation_type, *
FROM mismatch_keys
ORDER BY proc_run_id;

-- Consolidated Summary
SELECT 
     (SELECT COUNT(*) FROM parent_tbl) AS parent_count,
     (SELECT COUNT(*) FROM child_tbl) AS child_count,
     (SELECT COUNT(*) FROM missing_parent) AS child_without_parent,
     (SELECT COUNT(*) FROM missing_child) AS parent_without_child,
     (SELECT COUNT(*) FROM mismatch_keys) AS key_mismatches;