CREATE SCHEMA IF NOT EXISTS UTIL;

CREATE OR REPLACE PROCEDURE UTIL.SP_VALIDATE_TABLE_PAIR_TEMP(
    RUN_ID                STRING,
    JOB_NAME              STRING,
    DOMAIN_NM             STRING,
    SRC_TABLE_FQN         STRING,   -- e.g. 'HOSCDA.HLTH_OS_CDA_ENCNTR'
    TGT_TABLE_FQN         STRING,   -- e.g. 'HOSCDA_EXTRACT.HLTH_OS_CDA_ENCNTR'
    KEY_COLS_CSV          STRING,   -- e.g. 'ENCNTR_ID' or 'PAT_GUID'
    MANDATORY_COLS_CSV    STRING,   -- e.g. 'PAT_GUID' or 'MEMBER_ID'
    COMPARE_COLS_RULES    STRING,   -- e.g. 'PAT_GUID:EXACT,ENCOUNTER_DT:SAFE_TS,ENCNTR_TYPE_CD:TRIM_UPPER'
    RUNID_COLUMN          STRING,   -- default pass 'EDL_RUN_ID'
    TS_COLUMN             STRING,   -- default pass 'EDL_INCRMNTL_LOAD_DTM'
    WINDOW_TYPE           STRING,   -- 'LATEST_RUN' | 'ROLLING_MINUTES' | 'ROLLING_DAYS' | 'BETWEEN_DATES'
    P1                    STRING,   -- window param (e.g., minutes/days or start)
    P2                    STRING    -- window param (end for BETWEEN_DATES)
)
RETURNS STRING
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
function sql(s){ return snowflake.createStatement({sqlText:s}); }
function rows(stmt){ const rs=stmt.execute(); const out=[]; while(rs.next()){ const o={}; for(let i=1;i<=rs.getColumnCount();i++){ o[rs.getColumnName(i)] = rs.getColumnValue(i);} out.push(o);} return out; }
function qid(s){ return s.replace(/"/g,''); }

const runId        = RUN_ID;
const jobName      = JOB_NAME;
const domain       = DOMAIN_NM;
const srcFqn       = qid(SRC_TABLE_FQN);
const tgtFqn       = qid(TGT_TABLE_FQN);
const runCol       = RUNID_COLUMN || 'EDL_RUN_ID';
const tsCol        = TS_COLUMN    || 'EDL_INCRMNTL_LOAD_DTM';
const windowType   = (WINDOW_TYPE || 'LATEST_RUN').toUpperCase();

const keys = (KEY_COLS_CSV||'').split(',').map(s=>s.trim()).filter(Boolean);
const mandatory = (MANDATORY_COLS_CSV||'').split(',').map(s=>s.trim()).filter(Boolean);

// compare rules parsing: 'COL:RULE,COL2:TRIM_UPPER,COL3:SAFE_TS'
const rules = {};
(COMPARE_COLS_RULES||'').split(',').map(x=>x.trim()).filter(Boolean).forEach(pair=>{
  const i = pair.indexOf(':');
  if (i>0) { rules[pair.slice(0,i).trim()] = pair.slice(i+1).trim().toUpperCase(); }
});

// build window predicate
let wherePred = '1=1';
if (windowType === 'LATEST_RUN') {
  const latest = rows(sql(`SELECT MAX(${runCol}) AS L FROM ${tgtFqn}`))[0].L
             || rows(sql(`SELECT MAX(${runCol}) AS L FROM ${srcFqn}`))[0].L;
  if (latest !== null && latest !== undefined) {
    wherePred = `${runCol} = '${latest}'`;
  } else {
    wherePred = `${tsCol} >= DATEADD('day', -1, CURRENT_TIMESTAMP())`;
  }
} else if (windowType === 'ROLLING_MINUTES') {
  const mins = Number(P1||60);
  wherePred = `${tsCol} >= DATEADD('minute', -${mins}, CURRENT_TIMESTAMP())`;
} else if (windowType === 'ROLLING_DAYS') {
  const days = Number(P1||1);
  wherePred = `${tsCol} >= DATEADD('day', -${days}, CURRENT_TIMESTAMP())`;
} else if (windowType === 'BETWEEN_DATES') {
  const p1 = P1 || '1970-01-01';
  const p2 = P2 || '2999-12-31';
  wherePred = `${tsCol} BETWEEN TO_TIMESTAMP('${p1}') AND TO_TIMESTAMP('${p2}')`;
}

// helpers
if (keys.length === 0) {
  sql(`INSERT INTO TEMP_VALIDATION_RESULTS(run_id,batch_ts,job_name,domain_nm,table_src,table_tgt,validation_type,kpi_nm,status,severity,notes_short)
       SELECT '${runId}', CURRENT_TIMESTAMP(), '${jobName}', '${domain}', '${srcFqn}','${tgtFqn}','KEYS','STABILITY','ERROR','ERROR','No key columns provided'`).execute();
  return 'ERROR: No keys';
}
const keyCsv      = keys.map(k=>`"${k}"`).join(',');
const keyJoinPred = keys.map(k=>`s."${k}" = t."${k}"`).join(' AND ');
const sampleKeyObj = keys.map(k=>`'${k}', "${k}"`).join(',');

// ============ 2A: ROW_COVERAGE ============
sql(`
  INSERT INTO TEMP_VALIDATION_RESULTS
  (run_id,batch_ts,job_name,domain_nm,table_src,table_tgt,validation_type,kpi_nm,
   metric_value,metric_target,status,severity,src_count,tgt_count,owner_team,notes_short)
  WITH src AS (SELECT COUNT(*) c FROM ${srcFqn} WHERE ${wherePred}),
       tgt AS (SELECT COUNT(*) c FROM ${tgtFqn} WHERE ${wherePred})
  SELECT '${runId}', CURRENT_TIMESTAMP(), '${jobName}', '${domain}',
         '${srcFqn}','${tgtFqn}','ROW_COVERAGE','COMPLETENESS',
         IFF(s.c=0,NULL,t.c/NULLIF(s.c,0)) AS metric_value,
         0.99,
         CASE WHEN s.c=0 THEN 'FAIL'
              WHEN t.c/NULLIF(s.c,0) >= 0.99 THEN 'PASS'
              WHEN t.c/NULLIF(s.c,0) >= 0.98 THEN 'WARN'
              ELSE 'FAIL' END,
         CASE WHEN s.c=0 THEN 'ERROR'
              WHEN t.c/NULLIF(s.c,0) >= 0.99 THEN 'INFO'
              WHEN t.c/NULLIF(s.c,0) >= 0.98 THEN 'WARN'
              ELSE 'ERROR' END,
         s.c, t.c, 'Static','Row coverage (tgt/src)'
  FROM src s, tgt t
`).execute();

// ============ 2B: MINUS_SRC ============
sql(`
  INSERT INTO TEMP_VALIDATION_RESULTS
  (run_id,batch_ts,job_name,domain_nm,table_src,table_tgt,validation_type,kpi_nm,
   status,severity,only_in_src_cnt,owner_team,notes_short,sample_keys)
  WITH s AS (SELECT ${keyCsv} FROM ${srcFqn} WHERE ${wherePred}),
       t AS (SELECT ${keyCsv} FROM ${tgtFqn} WHERE ${wherePred}),
       only_src AS (
         SELECT s.* FROM s LEFT JOIN t USING (${keyCsv})
         WHERE ${keys.map(k=>`t."${k}" IS NULL`).join(' AND ')}
       ),
       samp AS (SELECT ARRAY_AGG(OBJECT_CONSTRUCT(${sampleKeyObj}))[:10] a FROM only_src)
  SELECT '${runId}', CURRENT_TIMESTAMP(), '${jobName}', '${domain}',
         '${srcFqn}','${tgtFqn}','MINUS_SRC','ACCURACY',
         IFF(COUNT(*)=0,'PASS','WARN'),
         IFF(COUNT(*)=0,'INFO','WARN'),
         COUNT(*),'Static','Rows only in SRC',
         (SELECT TO_JSON(a) FROM samp)
  FROM only_src
`).execute();

// ============ 2C: MINUS_TGT ============
sql(`
  INSERT INTO TEMP_VALIDATION_RESULTS
  (run_id,batch_ts,job_name,domain_nm,table_src,table_tgt,validation_type,kpi_nm,
   status,severity,only_in_tgt_cnt,owner_team,notes_short,sample_keys)
  WITH s AS (SELECT ${keyCsv} FROM ${srcFqn} WHERE ${wherePred}),
       t AS (SELECT ${keyCsv} FROM ${tgtFqn} WHERE ${wherePred}),
       only_tgt AS (
         SELECT t.* FROM t LEFT JOIN s USING (${keyCsv})
         WHERE ${keys.map(k=>`s."${k}" IS NULL`).join(' AND ')}
       ),
       samp AS (SELECT ARRAY_AGG(OBJECT_CONSTRUCT(${sampleKeyObj}))[:10] a FROM only_tgt)
  SELECT '${runId}', CURRENT_TIMESTAMP(), '${jobName}', '${domain}',
         '${srcFqn}','${tgtFqn}','MINUS_TGT','ACCURACY',
         IFF(COUNT(*)=0,'PASS','ERROR'),
         IFF(COUNT(*)=0,'INFO','ERROR'),
         COUNT(*),'Static','Rows only in TGT',
         (SELECT TO_JSON(a) FROM samp)
  FROM only_tgt
`).execute();

// helper to build normalized equality expression per rule
function normExpr(alias, col, rule) {
  if (!rule || rule === 'EXACT')  return `${alias}."${col}"`;
  if (rule === 'TRIM_UPPER')      return `UPPER(TRIM(${alias}."${col}"))`;
  if (rule === 'SAFE_TS')         return `TO_VARCHAR(DATE_TRUNC('SECOND', TRY_TO_TIMESTAMP_NTZ(${alias}."${col}")))`;
  return `${alias}."${col}"`; // default
}

// list compare columns = all rule keys EXCEPT key columns
const cmpCols = Object.keys(rules).filter(c => !keys.map(k=>k.toUpperCase()).includes(c.toUpperCase()));
const eqPreds = cmpCols.map(c => {
  const r = rules[c];
  const sExp = normExpr('s', c, r);
  const tExp = normExpr('t', c, r);
  return `((${sExp} IS NULL AND ${tExp} IS NULL) OR (${sExp} = ${tExp}))`;
});

// ============ 2D: VALUE_MISMATCH (rate) ============
if (eqPreds.length > 0) {
  sql(`
    INSERT INTO TEMP_VALIDATION_RESULTS
    (run_id,batch_ts,job_name,domain_nm,table_src,table_tgt,validation_type,kpi_nm,
     metric_value,metric_target,status,severity,mismatch_cnt,owner_team,notes_short)
    WITH joined AS (
      SELECT s.*, t.* FROM ${srcFqn} s JOIN ${tgtFqn} t ON ${keyJoinPred}
      WHERE s.${wherePred} AND t.${wherePred}
    ),
    total AS (SELECT COUNT(*) c FROM joined),
    bad AS (SELECT COUNT(*) c FROM joined WHERE NOT (${eqPreds.join(' AND ')}))
    SELECT '${runId}', CURRENT_TIMESTAMP(), '${jobName}', '${domain}',
           '${srcFqn}','${tgtFqn}','VALUE_MISMATCH','ACCURACY',
           IFF(total.c=0,0,bad.c/NULLIF(total.c,0)) AS mismatch_rate,
           0.005,
           CASE WHEN total.c=0 THEN 'PASS'
                WHEN bad.c/NULLIF(total.c,0) <= 0.005 THEN 'PASS'
                WHEN bad.c/NULLIF(total.c,0) <= 0.01  THEN 'WARN'
                ELSE 'FAIL' END,
           CASE WHEN total.c=0 THEN 'INFO'
                WHEN bad.c/NULLIF(total.c,0) <= 0.005 THEN 'INFO'
                WHEN bad.c/NULLIF(total.c,0) <= 0.01  THEN 'WARN'
                ELSE 'ERROR' END,
           bad.c, 'Static', 'Value parity using provided rules'
    FROM total, bad
  `).execute();
} else {
  // No compare columns supplied -> log INFO row
  sql(`
    INSERT INTO TEMP_VALIDATION_RESULTS
    (run_id,batch_ts,job_name,domain_nm,table_src,table_tgt,validation_type,kpi_nm,status,severity,notes_short)
    SELECT '${runId}', CURRENT_TIMESTAMP(), '${jobName}', '${domain}',
           '${srcFqn}','${tgtFqn}','VALUE_MISMATCH','ACCURACY','SKIPPED','INFO','No compare columns passed'
  `).execute();
}

// ============ 2E: NULL_STATS (avg NULL% on mandatory cols in TGT) ============
if (mandatory.length > 0) {
  const nullParts = mandatory.map(c => `AVG(IFF(TRIM(t."${c}") IS NULL OR TRIM(t."${c}")='',1,0))`);
  const avgExpr = nullParts.length === 1 ? nullParts[0] : `(${nullParts.join(' + ')})/${nullParts.length}`;
  sql(`
    INSERT INTO TEMP_VALIDATION_RESULTS
    (run_id,batch_ts,job_name,domain_nm,table_src,table_tgt,validation_type,kpi_nm,
     metric_value,metric_target,status,severity,null_pct_mandatory,owner_team,notes_short)
    WITH scoped AS (SELECT * FROM ${tgtFqn} WHERE ${wherePred}),
         m AS (SELECT ${avgExpr} AS null_pct FROM scoped t)
    SELECT '${runId}', CURRENT_TIMESTAMP(), '${jobName}', '${domain}',
           '${srcFqn}','${tgtFqn}','NULL_STATS','COMPLETENESS',
           m.null_pct, 0.01,
           CASE WHEN m.null_pct <= 0.01 THEN 'PASS'
                WHEN m.null_pct <= 0.03 THEN 'WARN'
                ELSE 'FAIL' END,
           CASE WHEN m.null_pct <= 0.01 THEN 'INFO'
                WHEN m.null_pct <= 0.03 THEN 'WARN'
                ELSE 'ERROR' END,
           m.null_pct, 'Static', 'Avg NULL% across mandatory columns'
    FROM m
  `).execute();
} else {
  sql(`
    INSERT INTO TEMP_VALIDATION_RESULTS
    (run_id,batch_ts,job_name,domain_nm,table_src,table_tgt,validation_type,kpi_nm,status,severity,notes_short)
    SELECT '${runId}', CURRENT_TIMESTAMP(), '${jobName}', '${domain}',
           '${srcFqn}','${tgtFqn}','NULL_STATS','COMPLETENESS','SKIPPED','INFO','No mandatory columns passed'
  `).execute();
}

// ============ FRESHNESS (minutes since last TGT load) ============
sql(`
  INSERT INTO TEMP_VALIDATION_RESULTS
  (run_id,batch_ts,job_name,domain_nm,table_src,table_tgt,validation_type,kpi_nm,
   metric_value,metric_target,status,severity,owner_team,notes_short)
  WITH last_dt AS (
    SELECT DATEDIFF('minute', MAX(${tsCol}), CURRENT_TIMESTAMP()) AS mins
    FROM ${tgtFqn}
  )
  SELECT '${runId}', CURRENT_TIMESTAMP(), '${jobName}', '${domain}',
         '${srcFqn}','${tgtFqn}','FRESHNESS','TIMELINESS',
         mins, 60,
         CASE WHEN mins <= 60 THEN 'PASS'
              WHEN mins <= 120 THEN 'WARN'
              ELSE 'FAIL' END,
         CASE WHEN mins <= 60 THEN 'INFO'
              WHEN mins <= 120 THEN 'WARN'
              ELSE 'ERROR' END,
         'Static','Minutes since last TGT load'
  FROM last_dt
`).execute();

return 'OK';
$$;
