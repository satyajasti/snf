SELECT 
    r.trnsltn_ind,
    r.proc_cd,
    r.sys_nm,
    e.hlth_srvc_type_cd,
    e.icd_proc_cd
FROM S01_HOSCDA.HOSCDA.HLTH_OS_CDA_PROC_CD_RFRNC r
JOIN HOSCDA_EXTRACT.CLNCL_ENCNTR_PROC e
    ON r.proc_cd = e.icd_proc_cd
WHERE r.trnsltn_ind IN (0,1);



SELECT 
    r.trnsltn_ind,
    r.proc_cd,
    r.sys_nm
FROM S01_HOSCDA.HOSCDA.HLTH_OS_CDA_PROC_CD_RFRNC r
LEFT JOIN HOSCDA_EXTRACT.CLNCL_ENCNTR_PROC e
    ON r.proc_cd = e.icd_proc_cd
WHERE (r.trnsltn_ind NOT IN (0,1) OR r.trnsltn_ind IS NULL)
  AND e.icd_proc_cd IS NULL;



SELECT 
    CASE 
        WHEN trnsltn_ind IN (0,1) THEN 'SHOULD_NOT_LOAD'
        ELSE 'SHOULD_LOAD'
    END AS load_category,
    COUNT(*) AS ref_count
FROM S01_HOSCDA.HOSCDA.HLTH_OS_CDA_PROC_CD_RFRNC
GROUP BY 1;


SELECT COUNT(*) AS extract_count
FROM HOSCDA_EXTRACT.CLNCL_ENCNTR_PROC;



SELECT 
    CASE 
        WHEN err_cnt = 0 AND miss_cnt = 0 THEN 'PASS'
        ELSE 'FAIL'
    END AS validation_status,
    err_cnt AS invalid_loaded_records,
    miss_cnt AS missing_records
FROM (
    SELECT
        /* Records that should NOT load but did */
        COUNT(DISTINCT e.icd_proc_cd) AS err_cnt,
        
        /* Records that should load but did NOT */
        (
            SELECT COUNT(DISTINCT r.proc_cd)
            FROM S01_HOSCDA.HOSCDA.HLTH_OS_CDA_PROC_CD_RFRNC r
            LEFT JOIN HOSCDA_EXTRACT.CLNCL_ENCNTR_PROC e2
                ON r.proc_cd = e2.icd_proc_cd
            WHERE (r.trnsltn_ind NOT IN (0,1) OR r.trnsltn_ind IS NULL)
              AND e2.icd_proc_cd IS NULL
        ) AS miss_cnt
    FROM S01_HOSCDA.HOSCDA.HLTH_OS_CDA_PROC_CD_RFRNC r
    JOIN HOSCDA_EXTRACT.CLNCL_ENCNTR_PROC e
        ON r.proc_cd = e.icd_proc_cd
    WHERE r.trnsltn_ind IN (0,1)
);
