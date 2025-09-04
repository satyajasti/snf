-- 1. Choose the database you want to scan
SET target_db = '';   -- or ''

-- 2. Create a results table
CREATE OR REPLACE TEMP TABLE search_hits (
  database_name STRING,
  schema_name   STRING,
  table_name    STRING,
  column_name   STRING,
  matched_value STRING,
  hit_count     NUMBER
);

-- 3. Generate dynamic SQL for each column
SELECT
  'INSERT INTO search_hits
   SELECT ' || QUOTE_LITERAL($target_db) || ',
          ' || QUOTE_LITERAL(c.TABLE_SCHEMA) || ',
          ' || QUOTE_LITERAL(c.TABLE_NAME) || ',
          ' || QUOTE_LITERAL(c.COLUMN_NAME) || ',
          ''HL7'', COUNT(*)
   FROM ' || $target_db || '.' || c.TABLE_SCHEMA || '.' || c.TABLE_NAME || '
   WHERE ' || c.COLUMN_NAME || ' ILIKE ''%%''
   UNION ALL
   SELECT ' || QUOTE_LITERAL($target_db) || ',
          ' || QUOTE_LITERAL(c.TABLE_SCHEMA) || ',
          ' || QUOTE_LITERAL(c.TABLE_NAME) || ',
          ' || QUOTE_LITERAL(c.COLUMN_NAME) || ',
          ''Direct'', COUNT(*)
   FROM ' || $target_db || '.' || c.TABLE_SCHEMA || '.' || c.TABLE_NAME || '
   WHERE ' || c.COLUMN_NAME || ' ILIKE ''%%''
   UNION ALL
   SELECT ' || QUOTE_LITERAL($target_db) || ',
          ' || QUOTE_LITERAL(c.TABLE_SCHEMA) || ',
          ' || QUOTE_LITERAL(c.TABLE_NAME) || ',
          ' || QUOTE_LITERAL(c.COLUMN_NAME) || ',
          ''IMM'', COUNT(*)
   FROM ' || $target_db || '.' || c.TABLE_SCHEMA || '.' || c.TABLE_NAME || '
   WHERE ' || c.COLUMN_NAME || ' ILIKE ''%%'';'
   AS sql_stmt
FROM IDENTIFIER($target_db || '.INFORMATION_SCHEMA.COLUMNS') c
WHERE DATA_TYPE ILIKE 'VARCHAR%';
