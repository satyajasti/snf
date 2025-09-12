WITH fk AS (
  SELECT tc.constraint_name,
         tc.table_schema   AS child_schema,
         tc.table_name     AS child_table
  FROM <DB>.<SCHEMA>.INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
  WHERE tc.constraint_type = 'FOREIGN KEY'
),
fk_cols AS (
  SELECT kcu.constraint_name,
         kcu.table_schema   AS child_schema,
         kcu.table_name     AS child_table,
         kcu.column_name    AS child_column,
         kcu.ordinal_position,
         kcu.position_in_unique_constraint  -- aligns FK col to parent col
  FROM <DB>.<SCHEMA>.INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
),
rc AS (
  SELECT rc.constraint_name,
         rc.unique_constraint_schema AS parent_schema,
         rc.unique_constraint_name   AS parent_constraint
  FROM <DB>.<SCHEMA>.INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS rc
),
parent_tbl AS (
  SELECT tc.constraint_name,
         tc.table_schema   AS parent_schema,
         tc.table_name     AS parent_table
  FROM <DB>.<SCHEMA>.INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
  WHERE tc.constraint_type IN ('PRIMARY KEY','UNIQUE')
)
SELECT
  f.child_schema,
  f.child_table,
  fc.child_column,
  pt.parent_schema,
  pt.parent_table,
  pc.column_name      AS parent_column,
  f.constraint_name   AS fk_name
FROM fk f
JOIN fk_cols fc
  ON f.constraint_name = fc.constraint_name
JOIN rc
  ON f.constraint_name = rc.constraint_name
JOIN parent_tbl pt
  ON rc.parent_constraint = pt.constraint_name
 AND rc.parent_schema     = pt.parent_schema
-- map FK column to the corresponding parent column by position
JOIN <DB>.<SCHEMA>.INFORMATION_SCHEMA.KEY_COLUMN_USAGE pc
  ON pc.constraint_name   = rc.parent_constraint
 AND pc.table_schema      = pt.parent_schema
 AND pc.table_name        = pt.parent_table
 AND pc.ordinal_position  = fc.position_in_unique_constraint
ORDER BY child_schema, child_table, fk_name, fc.ordinal_position;




WITH fk AS (
  SELECT constraint_name, table_schema AS child_schema, table_name AS child_table,
         referenced_constraint_name    AS parent_constraint,
         referenced_table_schema       AS parent_schema,
         referenced_table_name         AS parent_table,
         is_enforced
  FROM SNOWFLAKE.ACCOUNT_USAGE.TABLE_CONSTRAINTS
  WHERE constraint_type = 'FOREIGN KEY'
),
fk_cols AS (
  SELECT constraint_name, column_name, ordinal_position, position_in_unique_constraint
  FROM SNOWFLAKE.ACCOUNT_USAGE.KEY_COLUMN_USAGE
),
parent_cols AS (
  SELECT constraint_name, table_schema, table_name, column_name, ordinal_position
  FROM SNOWFLAKE.ACCOUNT_USAGE.KEY_COLUMN_USAGE
)
SELECT
  f.child_schema, f.child_table, fc.column_name AS child_column,
  f.parent_schema, f.parent_table, pc.column_name AS parent_column,
  f.constraint_name AS fk_name,
  f.is_enforced
FROM fk f
JOIN fk_cols fc
  ON f.constraint_name = fc.constraint_name
JOIN parent_cols pc
  ON pc.constraint_name  = f.parent_constraint
 AND pc.table_schema     = f.parent_schema
 AND pc.table_name       = f.parent_table
 AND pc.ordinal_position = fc.position_in_unique_constraint
ORDER BY f.child_schema, f.child_table, fk_name, fc.ordinal_position;

