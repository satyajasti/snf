-- 1) Where to search + what to look for
SET target_db      = '';     -- or ''
SET schema_like    = '%';          -- narrow like '%' if you want
SET search_values  = ARRAY_CONSTRUCT('');

-- 2) Results table
CREATE OR REPLACE TEMP TABLE search_hits (
  database_name STRING,
  schema_name   STRING,
  table_name    STRING,
  column_name   STRING,
  matched_value STRING,
  hit_count     NUMBER
);

-- 3) Scan every VARCHAR column and insert matches
DECLARE v_db STRING DEFAULT $target_db;
DECLARE v_schema_like STRING DEFAULT $schema_like;
DECLARE vals ARRAY DEFAULT $search_values;

FOR col IN (
  SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME
  FROM IDENTIFIER(v_db || '.INFORMATION_SCHEMA.COLUMNS')
  WHERE TABLE_SCHEMA ILIKE v_schema_like
    AND DATA_TYPE ILIKE 'VARCHAR%'          -- adjust if you also want VARIANT/ARRAY
) DO
  FOR i IN 0..ARRAY_SIZE(vals)-1 DO
    LET val STRING := vals[i]::STRING;

    LET q STRING :=
      'INSERT INTO search_hits
       SELECT ' ||
         QUOTE_LITERAL(v_db) || ',' ||
         QUOTE_LITERAL(col.TABLE_SCHEMA) || ',' ||
         QUOTE_LITERAL(col.TABLE_NAME) || ',' ||
         QUOTE_LITERAL(col.COLUMN_NAME) || ',' ||
         QUOTE_LITERAL(val) || ',
         COUNT(*)
       FROM ' || v_db || '.' || col.TABLE_SCHEMA || '.' || col.TABLE_NAME || '
       WHERE ' || col.COLUMN_NAME || ' ILIKE ''%' || val || '%''';

    EXECUTE IMMEDIATE :q;
  END FOR;
END FOR;

-- 4) See where we found them (only positives)
SELECT *
FROM search_hits
WHERE hit_count > 0
ORDER BY hit_count DESC, schema_name, table_name, column_name, matched_value;
