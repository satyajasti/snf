SELECT 
    r.trnsltn_ind,
    r.proc_cd,
    r.sys_nm,
    e.hlth_srvc_type_cd,
    e.icd_proc_cd
FROM S01_HOSCDA.HOSCDA.HLTH_OS_CDA_PROC_CD_RFRNC r
JOIN HOSCDA_EXTRACT.CLNCL_ENCNTR_PROC e
    ON r.proc_cd = e.icd_proc_cd
   AND r.edl_run_id = e.edl_run_id
WHERE r.edl_run_id = '<EDL_RUN_ID>'
  AND r.proc_cd = '80048'
  AND r.trnsltn_ind IN (0,1);


SELECT 
    r.trnsltn_ind,
    r.proc_cd,
    r.sys_nm
FROM S01_HOSCDA.HOSCDA.HLTH_OS_CDA_PROC_CD_RFRNC r
LEFT JOIN HOSCDA_EXTRACT.CLNCL_ENCNTR_PROC e
    ON r.proc_cd = e.icd_proc_cd
   AND r.edl_run_id = e.edl_run_id
WHERE r.edl_run_id = '<EDL_RUN_ID>'
  AND r.proc_cd = '80048'
  AND (r.trnsltn_ind NOT IN (0,1) OR r.trnsltn_ind IS NULL)
  AND e.icd_proc_cd IS NULL;

SELECT 
    CASE 
        WHEN trnsltn_ind IN (0,1) THEN 'SHOULD_NOT_LOAD'
        ELSE 'SHOULD_LOAD'
    END AS load_category,
    COUNT(*) AS ref_count
FROM S01_HOSCDA.HOSCDA.HLTH_OS_CDA_PROC_CD_RFRNC
WHERE edl_run_id = '<EDL_RUN_ID>'
  AND proc_cd = '80048'
GROUP BY 1;


