=======================================================================
AFTER MIGRATION VALIDATION TEST CASES
Table: datapostingtasks  (NEW active)
Backup: datapostingtasks_bkp_121225 (OLD original)
=======================================================================


=======================================================================
TC-01: TABLE EXISTENCE VALIDATION
=======================================================================
SQL:
SELECT 
    OBJECT_ID('dbo.datapostingtasks') AS ActiveTableExists,
    OBJECT_ID('dbo.datapostingtasks_bkp_121225') AS BackupTableExists;

Expected:
Both must NOT be NULL.



=======================================================================
TC-02: ROW COUNT MATCH (DATA PARITY CHECK)
=======================================================================
SQL:
SELECT 
    (SELECT COUNT(*) FROM dbo.datapostingtasks) AS ActiveCount,
    (SELECT COUNT(*) FROM dbo.datapostingtasks_bkp_121225) AS BackupCount;

Expected:
ActiveCount = BackupCount.



=======================================================================
TC-03: MIN/MAX DATE VALIDATION (inserteddate)
=======================================================================
SQL:
SELECT MIN(inserteddate), MAX(inserteddate)
FROM dbo.datapostingtasks;

SELECT MIN(inserteddate), MAX(inserteddate)
FROM dbo.datapostingtasks_bkp_121225;

Expected:
Min & Max values should match.



=======================================================================
TC-04: MIN/MAX DATE VALIDATION (ALL DATE COLUMNS)
=======================================================================
SQL:
-- modifieddate
SELECT MIN(modifieddate), MAX(modifieddate) FROM dbo.datapostingtasks;
SELECT MIN(modifieddate), MAX(modifieddate) FROM dbo.datapostingtasks_bkp_121225;

-- processbegindate
SELECT MIN(processbegindate), MAX(processbegindate) FROM dbo.datapostingtasks;
SELECT MIN(processbegindate), MAX(processbegindate) FROM dbo.datapostingtasks_bkp_121225;

-- processenddate
SELECT MIN(processenddate), MAX(processenddate) FROM dbo.datapostingtasks;
SELECT MIN(processenddate), MAX(processenddate) FROM dbo.datapostingtasks_bkp_121225;

Expected:
All values must match.



=======================================================================
TC-05: COLUMN NAME + DATATYPE VALIDATION
=======================================================================
SQL:
SELECT name, system_type_id, max_length, is_nullable
FROM sys.columns
WHERE object_id = OBJECT_ID('dbo.datapostingtasks')
ORDER BY column_id;

SELECT name, system_type_id, max_length, is_nullable
FROM sys.columns
WHERE object_id = OBJECT_ID('dbo.datapostingtasks_bkp_121225')
ORDER BY column_id;

Expected:
Column definitions match.



=======================================================================
TC-06: PRIMARY KEY VALIDATION
=======================================================================
SQL:
EXEC sp_helpindex 'dbo.datapostingtasks';
EXEC sp_helpindex 'dbo.datapostingtasks_bkp_121225';

Expected:
PK exists in both tables on column [id].



=======================================================================
TC-07: FOREIGN KEY INTEGRITY CHECK
=======================================================================
SQL:
SELECT parenttype_id
FROM dbo.datapostingtasks
WHERE parenttype_id IS NOT NULL
  AND parenttype_id NOT IN (SELECT id FROM taskparenttypes);

Expected:
0 rows.



=======================================================================
TC-08: CHECKSUM VALIDATION (ROW-BY-ROW DATA MATCH)
=======================================================================
SQL:
SELECT CHECKSUM_AGG(BINARY_CHECKSUM(*)) AS NewChecksum
FROM dbo.datapostingtasks;

SELECT CHECKSUM_AGG(BINARY_CHECKSUM(*)) AS OldChecksum
FROM dbo.datapostingtasks_bkp_121225;

Expected:
NewChecksum = OldChecksum.



=======================================================================
TC-09: SAMPLE RECORD VALIDATION
=======================================================================
SQL:
SELECT * FROM dbo.datapostingtasks WHERE id IN (1,100,5000);
SELECT * FROM dbo.datapostingtasks_bkp_121225 WHERE id IN (1,100,5000);

Expected:
All rows match.



=======================================================================
TC-10: NULL PERCENTAGE VALIDATION (ALL COLUMNS)
=======================================================================
-- Auto-generate SQL for null check across all columns:
SELECT 
    'SELECT ''' + name + ''' AS ColumnName, ' +
    'SUM(CASE WHEN ' + name + ' IS NULL THEN 1 ELSE 0 END) AS NullRows, ' +
    'COUNT(*) AS TotalRows FROM dbo.datapostingtasks'
FROM sys.columns
WHERE object_id = OBJECT_ID('dbo.datapostingtasks');

Run same for backup table.

Expected:
Null distribution should match.



=======================================================================
TC-11: PARENT-CHILD DISTRIBUTION VALIDATION
=======================================================================
SQL:
SELECT parent_id, COUNT(*)
FROM dbo.datapostingtasks
GROUP BY parent_id;

SELECT parent_id, COUNT(*)
FROM dbo.datapostingtasks_bkp_121225
GROUP BY parent_id;

Expected:
Distribution matches.



=======================================================================
TC-12: MAX BINARY SIZE VALIDATION (varbinary(max))
=======================================================================
SQL:
SELECT MAX(DATALENGTH(data)) FROM dbo.datapostingtasks;
SELECT MAX(DATALENGTH(data)) FROM dbo.datapostingtasks_bkp_121225;

Expected:
Sizes match (no truncation).



=======================================================================
TC-13: LOGMESSAGE SIZE VALIDATION
=======================================================================
SQL:
SELECT MAX(LEN(logmessage)) FROM dbo.datapostingtasks;
SELECT MAX(LEN(logmessage)) FROM dbo.datapostingtasks_bkp_121225;

Expected:
Values match.



=======================================================================
TC-14: DUPLICATE PK VALIDATION
=======================================================================
SQL:
SELECT id, COUNT(*) 
FROM dbo.datapostingtasks
GROUP BY id
HAVING COUNT(*) > 1;

Expected:
0 rows.



=======================================================================
TC-15: IDENTITY PROPERTY VALIDATION
=======================================================================
SQL:
SELECT name, is_identity, seed_value, increment_value
FROM sys.identity_columns
WHERE object_id = OBJECT_ID('dbo.datapostingtasks');

Expected:
Identity settings correct.



=======================================================================
TC-16: PERFORMANCE VALIDATION
=======================================================================
SQL:
SELECT TOP 1000 * 
FROM dbo.datapostingtasks
ORDER BY inserteddate DESC;

Expected:
Query performance acceptable (no degradation).



=======================================================================
TC-17: INDEX VALIDATION
=======================================================================
SQL:
SELECT * 
FROM sys.indexes 
WHERE object_id = OBJECT_ID('dbo.datapostingtasks');

Expected:
Indexes match expectations.



=======================================================================
TC-18: TRIGGER VALIDATION (IF ANY)
=======================================================================
SQL:
SELECT * 
FROM sys.triggers
WHERE parent_id = OBJECT_ID('dbo.datapostingtasks');

Expected:
Any required triggers exist.



=======================================================================
TC-19: PRIORITY VALUE DISTRIBUTION MATCH
=======================================================================
SQL:
SELECT priority, COUNT(*) 
FROM dbo.datapostingtasks
GROUP BY priority;

SELECT priority, COUNT(*) 
FROM dbo.datapostingtasks_bkp_121225
GROUP BY priority;

Expected:
Distribution identical.



=======================================================================
TC-20: MIN/MAX ID VALIDATION
=======================================================================
SQL:
SELECT MIN(id), MAX(id) FROM dbo.datapostingtasks;
SELECT MIN(id), MAX(id) FROM dbo.datapostingtasks_bkp_121225;

Expected:
Values match.



=======================================================================
END OF AFTER-MIGRATION TEST CASE SUITE
=======================================================================