-- 🔧 Set your inputs
SET A_DB='DB_A';      SET A_SCHEMA='SCH_A';      SET A_TABLE='TABLE_A';
SET B_DB='DB_B';      SET B_SCHEMA='SCH_B';      SET B_TABLE='TABLE_B';

WITH cols_a AS (
  SELECT
    UPPER(column_name) AS col,
    data_type,
    character_maximum_length AS char_len,
    numeric_precision       AS num_prec,
    numeric_scale           AS num_scale,
    is_nullable,
    column_default
  FROM IDENTIFIER($A_DB||'.'||$A_SCHEMA||'.INFORMATION_SCHEMA.COLUMNS')
  WHERE table_name = $A_TABLE
),
cols_b AS (
  SELECT
    UPPER(column_name) AS col,
    data_type,
    character_maximum_length AS char_len,
    numeric_precision       AS num_prec,
    numeric_scale           AS num_scale,
    is_nullable,
    column_default
  FROM IDENTIFIER($B_DB||'.'||$B_SCHEMA||'.INFORMATION_SCHEMA.COLUMNS')
  WHERE table_name = $B_TABLE
)
SELECT
  COALESCE(a.col, b.col)                        AS column_name,
  CASE
    WHEN a.col IS NOT NULL AND b.col IS NULL THEN 'ONLY_IN_A'
    WHEN a.col IS NULL AND b.col IS NOT NULL THEN 'ONLY_IN_B'
    WHEN a.data_type<>b.data_type
      OR NVL(a.char_len,-1)<>NVL(b.char_len,-1)
      OR NVL(a.num_prec,-1)<>NVL(b.num_prec,-1)
      OR NVL(a.num_scale,-1)<>NVL(b.num_scale,-1)
      OR a.is_nullable<>b.is_nullable
      OR NVL(a.column_default,'§')<>NVL(b.column_default,'§')
    THEN 'DIFFERENT_DEF'
    ELSE 'MATCH'
  END                                            AS diff_status,
  a.data_type      AS a_type,    b.data_type     AS b_type,
  a.char_len       AS a_len,     b.char_len      AS b_len,
  a.num_prec       AS a_prec,    b.num_prec      AS b_prec,
  a.num_scale      AS a_scale,   b.num_scale     AS b_scale,
  a.is_nullable    AS a_null,    b.is_nullable   AS b_null,
  a.column_default AS a_default, b.column_default AS b_default
FROM cols_a a
FULL OUTER JOIN cols_b b
  ON a.col=b.col
ORDER BY diff_status, column_name;




-- PK name + column list (comma-separated) for each table
WITH pk_a AS (
  SELECT tc.constraint_name,
         LISTAGG(kcu.column_name, ',') WITHIN GROUP (ORDER BY kcu.ordinal_position) AS pk_cols
  FROM IDENTIFIER($A_DB||'.'||$A_SCHEMA||'.INFORMATION_SCHEMA.TABLE_CONSTRAINTS') tc
  JOIN IDENTIFIER($A_DB||'.'||$A_SCHEMA||'.INFORMATION_SCHEMA.KEY_COLUMN_USAGE') kcu
    ON tc.constraint_name = kcu.constraint_name
   AND tc.table_name      = kcu.table_name
  WHERE tc.table_name = $A_TABLE AND tc.constraint_type='PRIMARY KEY'
  GROUP BY 1
),
pk_b AS (
  SELECT tc.constraint_name,
         LISTAGG(kcu.column_name, ',') WITHIN GROUP (ORDER BY kcu.ordinal_position) AS pk_cols
  FROM IDENTIFIER($B_DB||'.'||$B_SCHEMA||'.INFORMATION_SCHEMA.TABLE_CONSTRAINTS') tc
  JOIN IDENTIFIER($B_DB||'.'||$B_SCHEMA||'.INFORMATION_SCHEMA.KEY_COLUMN_USAGE') kcu
    ON tc.constraint_name = kcu.constraint_name
   AND tc.table_name      = kcu.table_name
  WHERE tc.table_name = $B_TABLE AND tc.constraint_type='PRIMARY KEY'
  GROUP BY 1
)
SELECT
  NVL(a.constraint_name,'(none)') AS a_pk_name,
  a.pk_cols                        AS a_pk_cols,
  NVL(b.constraint_name,'(none)')  AS b_pk_name,
  b.pk_cols                        AS b_pk_cols,
  CASE WHEN NVL(a.pk_cols,'')=NVL(b.pk_cols,'') THEN 'MATCH' ELSE 'DIFFERENT' END AS pk_match
FROM pk_a a
FULL OUTER JOIN pk_b b ON 1=1;



-- 🔧 Inputs
SET A_DB='DB_A'; SET A_SCHEMA='SCH_A'; SET A_TABLE='TABLE_A';
SET B_DB='DB_B'; SET B_SCHEMA='SCH_B'; SET B_TABLE='TABLE_B';

-- Key and data columns (edit these!)
SET KEY_COLS = 'id';                                    -- e.g., 'id' or 'id,acct_id'
SET DATA_COLS = 'col1,col2,col3';                       -- only columns you want to compare

-- Rows in A not in B (by key)
WITH a AS (SELECT ${KEY_COLS}, ${DATA_COLS} FROM IDENTIFIER($A_DB||'.'||$A_SCHEMA||'.'||$A_TABLE)),
     b AS (SELECT ${KEY_COLS}, ${DATA_COLS} FROM IDENTIFIER($B_DB||'.'||$B_SCHEMA||'.'||$B_TABLE))
SELECT a.*
FROM a LEFT JOIN b USING (${KEY_COLS})
WHERE b.${SPLIT_PART(:KEY_COLS,',',1)} IS NULL;

-- Rows in B not in A (by key)
WITH a AS (SELECT ${KEY_COLS}, ${DATA_COLS} FROM IDENTIFIER($A_DB||'.'||$A_SCHEMA||'.'||$A_TABLE)),
     b AS (SELECT ${KEY_COLS}, ${DATA_COLS} FROM IDENTIFIER($B_DB||'.'||$B_SCHEMA||'.'||$B_TABLE))
SELECT b.*
FROM b LEFT JOIN a USING (${KEY_COLS})
WHERE a.${SPLIT_PART(:KEY_COLS,',',1)} IS NULL;

-- Same keys but any data column differs
WITH a AS (SELECT ${KEY_COLS}, ${DATA_COLS} FROM IDENTIFIER($A_DB||'.'||$A_SCHEMA||'.'||$A_TABLE)),
     b AS (SELECT ${KEY_COLS}, ${DATA_COLS} FROM IDENTIFIER($B_DB||'.'||$B_SCHEMA||'.'||$B_TABLE))
SELECT
  a.${KEY_COLS},
  -- show per-column values side by side
  ${LISTAGG('a.'||c||' AS a_'||c||', b.'||c||' AS b_'||c, ', ') OVER ()}
FROM a JOIN b USING (${KEY_COLS})
WHERE
  ${LISTAGG('(a.'||c||' IS DISTINCT FROM b.'||c||')', ' OR ') OVER ()};




WITH a AS (SELECT ${KEY_COLS}, ${DATA_COLS} FROM IDENTIFIER($A_DB||'.'||$A_SCHEMA||'.'||$A_TABLE)),
     b AS (SELECT ${KEY_COLS}, ${DATA_COLS} FROM IDENTIFIER($B_DB||'.'||$B_SCHEMA||'.'||$B_TABLE)),
     j AS (SELECT * FROM a JOIN b USING (${KEY_COLS}))
SELECT
  ${KEY_COLS},
  col_name,
  a_val,
  b_val
FROM j
UNPIVOT (a_val FOR col_name IN (${DATA_COLS})) AS ua
JOIN (
  SELECT ${KEY_COLS}, ${LISTAGG('b.'||c||' AS '||c, ', ') OVER ()}
  FROM j
) USING (${KEY_COLS})
UNPIVOT (b_val FOR col_name IN (${DATA_COLS})) AS ub
WHERE ua.col_name = ub.col_name
  AND (a_val IS DISTINCT FROM b_val)
ORDER BY ${SPLIT_PART(:KEY_COLS,',',1)}, col_name;





-- 🔧 Set list of columns to compare in order
SET COLS = 'col1,col2,col3';

WITH a AS (
  SELECT MD5(TO_VARCHAR(ARRAY_CONSTRUCT(${COLS}))) AS row_hash, ${COLS}
  FROM IDENTIFIER($A_DB||'.'||$A_SCHEMA||'.'||$A_TABLE)
),
b AS (
  SELECT MD5(TO_VARCHAR(ARRAY_CONSTRUCT(${COLS}))) AS row_hash, ${COLS}
  FROM IDENTIFIER($B_DB||'.'||$B_SCHEMA||'.'||$B_TABLE)
)
-- A minus B
SELECT a.* FROM a
LEFT JOIN b USING (row_hash)
WHERE b.row_hash IS NULL;

-- B minus A
SELECT b.* FROM b
LEFT JOIN a USING (row_hash)
WHERE a.row_hash IS NULL;
