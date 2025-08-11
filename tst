/* A1. prvnance */
CREATE OR REPLACE TEMP TABLE dbg_p1_prvnance AS
SELECT DISTINCT
  src_nm, encntr_rfrnc_txt, PRVNANCE_RCRD_DTM, PRVNANCE_PRD_END_DTM, edl_run_id
FROM hoscda.hlth_os_cda_prvnance
WHERE PRVNANCE_RCRD_DTM > '20220531160322-0400'
-- AND edl_run_id = $test_run
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY edl_run_id
  ORDER BY PRVNANCE_PRD_END_DTM DESC
) = 1;

SELECT 'A1 prvnance' step, COUNT(*) cnt FROM dbg_p1_prvnance;

/* A2. encntr */
CREATE OR REPLACE TEMP TABLE dbg_p1_encntr AS
SELECT DISTINCT
  E.actl_prd_strt_dtm AS admt_dt,
  E.actl_prd_end_dtm  AS dschrg_dt,
  E.encntr_id, E.encntr_guid,
  SPLIT_PART(E.subj_rfrnc_txt,'/',2) AS pat_guid,
  COALESCE(NULLIF(TRIM(TYP.ENCNTR_TYPE_DSPLY_TXT),''), TYP.ORGNL_TXT) AS PAT_TYPE_NM,
  E.RCRD_EXCLSN_CD, E.src_nm, E.edl_run_id
FROM hoscda.hlth_os_cda_encntr E
LEFT JOIN HOSCDA.HLTH_OS_CDA_ENCNTR_TYPE TYP
  ON E.encntr_ID = TYP.RSRC_ID AND E.EDL_RUN_ID = TYP.EDL_RUN_ID
-- AND E.edl_run_id = $test_run
;
SELECT 'A2 encntr' step, COUNT(*) cnt FROM dbg_p1_encntr;

/* A3. prvnance × encntr (inner) */
CREATE OR REPLACE TEMP TABLE dbg_p1_s1 AS
SELECT p.*, e.*
FROM dbg_p1_prvnance p
JOIN dbg_p1_encntr  e
  ON p.src_nm = e.src_nm
 AND p.edl_run_id = e.edl_run_id;
SELECT 'A3 join prvnance×encntr' step, COUNT(*) cnt FROM dbg_p1_s1;

/* A4. PAI (inner on pat_guid as in your code) */
CREATE OR REPLACE TEMP TABLE dbg_p1_pai AS
SELECT DISTINCT COALESCE(mcid,0) AS mcid, pat_guid, edl_run_id, lob_cd,
       rptg_rcrd_authrty_nm, rcrd_authrty_nm
FROM hoscda.extra_table_name;  -- keep your same source
-- WHERE edl_run_id = $test_run
SELECT 'A4 PAI' step, COUNT(*) cnt FROM dbg_p1_pai;

CREATE OR REPLACE TEMP TABLE dbg_p1_s2 AS
SELECT s1.*, p.mcid, p.lob_cd, p.rptg_rcrd_authrty_nm, p.rcrd_authrty_nm
FROM dbg_p1_s1 s1
JOIN dbg_p1_pai p
  ON s1.pat_guid = p.pat_guid;
SELECT 'A4 join +PAI' step, COUNT(*) cnt FROM dbg_p1_s2;

/* A5. class (LEFT) */
CREATE OR REPLACE TEMP TABLE dbg_p1_cls AS
SELECT rsrc_id, edl_run_id, encntr_cls_cd AS pat_cls_nm,
       CASE
         WHEN (UPPER(encntr_cls_dsply_cd) IN ('I','INP','IMP','IP','INPATIENT','99221','99222')) THEN 'OUTPATIENT'
         WHEN (UPPER(encntr_cls_dsply_cd) LIKE '%ONC RECUR%') THEN 'OUTPATIENT'
         WHEN (UPPER(encntr_cls_dsply_cd) LIKE '%(OPHTHAMOLO) OPHTHAMOLOGY%' OR UPPER(encntr_cls_dsply_cd) LIKE '%335%') THEN 'OUTPATIENT'
         ELSE 'OTHER'
       END AS RPTG_PAT_CLS_NM
FROM hoscda.hlth_os_cda_encntr_cls
WHERE trnsln_ind = '0';
-- AND edl_run_id = $test_run

CREATE OR REPLACE TEMP TABLE dbg_p1_s3 AS
SELECT s2.*, c.pat_cls_nm, c.RPTG_PAT_CLS_NM
FROM dbg_p1_s2 s2
LEFT JOIN dbg_p1_cls c
  ON s2.encntr_guid = c.rsrc_id AND s2.edl_run_id = c.edl_run_id;
SELECT 'A5 +class' step, COUNT(*) cnt,
       COUNT_IF(c.pat_cls_nm IS NULL) cls_nulls
FROM dbg_p1_s3;

/* A6. encounter id ref (LEFT) */
CREATE OR REPLACE TEMP TABLE dbg_p1_idref AS
SELECT DISTINCT rsrc_id, edl_run_id, val_txt AS src_encntr_id
FROM hoscda.hlth_os_cda_encntr_id_rfrnc
WHERE encntr_type_nm = 'ENC';
-- AND edl_run_id = $test_run

CREATE OR REPLACE TEMP TABLE dbg_p1_s4 AS
SELECT s3.*, r.src_encntr_id
FROM dbg_p1_s3 s3
LEFT JOIN dbg_p1_idref r
  ON s3.encntr_guid = r.rsrc_id AND s3.edl_run_id = r.edl_run_id;
SELECT 'A6 +idref' step, COUNT(*) cnt,
       COUNT_IF(src_encntr_id IS NULL) idref_nulls
FROM dbg_p1_s4;

/* A7. participant (LEFT) */
CREATE OR REPLACE TEMP TABLE dbg_p1_prtcpnt AS
SELECT DISTINCT rsrc_id, edl_run_id,
       prtcpnt_type_cd AS rlnshp_type_cd,
       prtcpnt_type_dspl_txt AS rlnshp_type_nm,
       SPLIT_PART(actr_txt,'/',2) AS encntr_prtcpnt_id
FROM hoscda.hlth_os_cda_encntr_prtcpnt;
-- AND edl_run_id = $test_run

CREATE OR REPLACE TEMP TABLE dbg_p1_s5 AS
SELECT s4.*, ep.rlnshp_type_cd, ep.rlnshp_type_nm, ep.encntr_prtcpnt_id
FROM dbg_p1_s4 s4
LEFT JOIN dbg_p1_prtcpnt ep
  ON s4.encntr_guid = ep.rsrc_id AND s4.edl_run_id = ep.edl_run_id;
SELECT 'A7 +participant' step, COUNT(*) cnt,
       COUNT_IF(encntr_prtcpnt_id IS NULL) prtcpnt_nulls
FROM dbg_p1_s5;

/* A8. practitioner NPI (LEFT) */
CREATE OR REPLACE TEMP TABLE dbg_p1_pract_npi AS
SELECT DISTINCT rsrc_id, edl_run_id, val_txt AS CRGVR_NPI
FROM hoscda.hlth_os_cda_practnr_id_rfrnc
WHERE practnr_type_txt = 'NPI';
-- AND edl_run_id = $test_run

CREATE OR REPLACE TEMP TABLE dbg_p1_s6 AS
SELECT s5.*, n.CRGVR_NPI
FROM dbg_p1_s5 s5
LEFT JOIN dbg_p1_pract_npi n
  ON s5.encntr_prtcpnt_id = n.rsrc_id AND s5.edl_run_id = n.edl_run_id;
SELECT 'A8 +NPI' step, COUNT(*) cnt,
       COUNT_IF(CRGVR_NPI IS NULL) npi_nulls
FROM dbg_p1_s6;

/* A9. document reference (LEFT) */
CREATE OR REPLACE TEMP TABLE dbg_p1_rfrnc AS
SELECT DISTINCT src_nm, dcmnt_cntxt_txt, edl_run_id, dcmnt_rfrnc_id,
       load_dtm, dcmnt_type_cd AS rpt_type_cd, dspl_txt AS rpt_type_nm, DCMNT_STTS_CD
FROM hoscda.hlth_os_cda_dcmnt_rfrnc;
-- WHERE edl_run_id = $test_run

CREATE OR REPLACE TEMP TABLE dbg_p1_s7 AS
SELECT s6.*,
       r.dcmnt_rfrnc_id, r.rpt_type_cd, r.rpt_type_nm, r.DCMNT_STTS_CD, r.load_dtm
FROM dbg_p1_s6 s6
LEFT JOIN dbg_p1_rfrnc r
  ON s6.src_nm = r.src_nm
 AND s6.encntr_rfrnc_txt = r.dcmnt_cntxt_txt
 AND s6.edl_run_id = r.edl_run_id;
SELECT 'A9 +doc_ref' step, COUNT(*) cnt,
       COUNT_IF(dcmnt_rfrnc_id IS NULL) rfrnc_nulls
FROM dbg_p1_s7;

/* A10. content (LEFT), then filter */
CREATE OR REPLACE TEMP TABLE dbg_p1_cntnt AS
SELECT DISTINCT rsrc_id, edl_run_id,
       atchmnt_url_txt AS DCMNT_PATH_NM,
       atchmnt_cntnt_type_nm AS rpt_frmt_cd,
       ATCHMNT_TTL_NM
FROM hoscda.hlth_os_cda_dcmnt_rfrnc_cntnt;
-- WHERE edl_run_id = $test_run

CREATE OR REPLACE TEMP TABLE dbg_p1_s8 AS
SELECT s7.*, c.DCMNT_PATH_NM, c.rpt_frmt_cd, c.ATCHMNT_TTL_NM
FROM dbg_p1_s7 s7
LEFT JOIN dbg_p1_cntnt c
  ON s7.dcmnt_rfrnc_id = c.rsrc_id AND s7.edl_run_id = c.edl_run_id;
SELECT 'A10 +content' step, COUNT(*) cnt,
       COUNT_IF(DCMNT_PATH_NM IS NULL) path_nulls
FROM dbg_p1_s8;

/* A11. apply your WHERE filters (FINAL/F + CCDA + CONTAINS) */
CREATE OR REPLACE TEMP TABLE dbg_p1_filtered AS
SELECT *
FROM dbg_p1_s8
WHERE UPPER(DCMNT_STTS_CD) IN ('FINAL','F')
  AND UPPER(ATCHMNT_TTL_NM) = 'CCDA'
  AND CONTAINS(DCMNT_PATH_NM, SPLIT_PART(src_nm,'|',2));  -- same as your clause
SELECT 'A11 after filters' step, COUNT(*) cnt FROM dbg_p1_filtered;

/* A12. final QUALIFY (same as yours) */
CREATE OR REPLACE TEMP TABLE dbg_p1_final AS
SELECT *
FROM dbg_p1_filtered
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY edl_run_id, DCMNT_PATH_NM
  ORDER BY PRVNANCE_PRD_END_DTM DESC, dcmnt_rfrnc_id DESC, ATCHMNT_TTL_NM DESC
) = 1;

SELECT 'A12 final' step, COUNT(*) cnt FROM dbg_p1_final;



SELECT
  CASE
    WHEN dcmnt_rfrnc_id IS NULL THEN 'NO_DOC_REF'
    WHEN ATCHMNT_TTL_NM IS NULL THEN 'NO_ATTACHMENT'
    WHEN DCMNT_PATH_NM IS NULL THEN 'NO_PATH'
    WHEN NOT CONTAINS(DCMNT_PATH_NM, SPLIT_PART(src_nm,'|',2)) THEN 'CONTAINS_FAIL'
    ELSE 'OK'
  END AS reason,
  COUNT(*) rows
FROM dbg_p1_s8
GROUP BY 1 ORDER BY rows DESC;



/* B7. facility NPI (LEFT) */
CREATE OR REPLACE TEMP TABLE dbg_p2_org_npi AS
SELECT DISTINCT VAL_TXT AS CRGVR_NPI, rsrc_id, edl_run_id
FROM hoscda.hlth_os_cda_org_id_rfrnc
WHERE ORG_TYPE_TXT = 'NPI';

CREATE OR REPLACE TEMP TABLE dbg_p2_s5 AS
SELECT s4.*, org.CRGVR_NPI, 'SERVICING FACILITY' AS rlnshp_type_cd, 'SERVICING_FACILITY' AS rlnshp_type_nm
FROM dbg_p2_s4 s4
LEFT JOIN dbg_p2_org_npi org
  ON org.rsrc_id = SPLIT_PART(s4.SRVC_PROV_NM,'/',2)  -- or SRC_PROV_NM if that’s the correct column
 AND org.edl_run_id = s4.edl_run_id;


/* Cx. ATSTR (LEFT) */
CREATE OR REPLACE TEMP TABLE dbg_p3_atstr AS
SELECT DISTINCT edl_run_id,
       SPLIT_PART(RFRNC_TXT,'/',2) AS atstr_rfrnc_id,
       MDE_CD AS rlnshp_type_cd,
       DSPLY_MDE_TXT AS rlnshp_type_nm,
       RSRC_ID
FROM hoscda.hlth_os_cda_dcmnt_rfrnc_atstr;

CREATE OR REPLACE TEMP TABLE dbg_p3_s_atstr AS
SELECT s7.*, a.rlnshp_type_cd, a.rlnshp_type_nm, a.atstr_rfrnc_id
FROM dbg_p3_s7 s7
LEFT JOIN dbg_p3_atstr a
  ON a.RSRC_ID = s7.dcmnt_rfrnc_id AND a.edl_run_id = s7.edl_run_id;

/* then practitioner NPI off atstr_rfrnc_id */
CREATE OR REPLACE TEMP TABLE dbg_p3_s8 AS
SELECT s_at.*, n.CRGVR_NPI
FROM dbg_p3_s_atstr s_at
LEFT JOIN dbg_p1_pract_npi n
  ON s_at.atstr_rfrnc_id = n.rsrc_id AND s_at.edl_run_id = n.edl_run_id;



