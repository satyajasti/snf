WITH latest_run AS (
    SELECT MAX(edl_run_id) AS edl_run_id
    FROM HOSCDA.hlth_os_cda_load_log_key
    WHERE load_stts_cd = 'Completed'
)

SELECT COUNT(1) AS TARGET_CNT,
       table_name
FROM (
    SELECT DISTINCT
        EDL_RUN_ID,
        'HLTH_OS_CDA_ALRGY_INTLRNC_NOTE' AS table_name,
        'TARGET' AS LAYER
    FROM HOSCDA.HLTH_OS_CDA_ALRGY_INTLRNC_NOTE
    WHERE EDL_RUN_ID = (SELECT edl_run_id FROM latest_run)
)
GROUP BY ALL;