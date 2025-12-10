/* =============================
     ROLLBACK SCRIPT
   ============================= */

USE P360Prod_careeevolutiontasks;
GO

/* Step 1: Rename current datapostingtasks (new table) */
IF OBJECT_ID('dbo.datapostingtasks', 'U') IS NOT NULL
BEGIN
    EXEC sp_rename 'dbo.datapostingtasks', 'datapostingtasks_new_rollback';
END
GO

/* Step 2: Restore original table */
IF OBJECT_ID('dbo.datapostingtasks_bkp_121225', 'U') IS NOT NULL
BEGIN
    EXEC sp_rename 'dbo.datapostingtasks_bkp_121225', 'datapostingtasks';
END
GO

/* Step 3: Restore original constraint names */
EXEC sp_rename 'dbo.datapostingtasks_bkp121225', 'PK_datapostingtasks';
EXEC sp_rename 'dbo.DF_datapostingtasks_taskstatus_bkp121225', 'DF_datapostingtasks_taskstatus';
EXEC sp_rename 'dbo.DF_datapostingtasks_priority_bkp121225', 'DF_datapostingtasks_priority';
EXEC sp_rename 'dbo.DF_datapostingtasks_retries_bkp121225', 'DF_datapostingtasks_retries';
EXEC sp_rename 'dbo.FK_datapostingtasks_taskparenttypes_parenttype_id_bkp121225', 
               'FK_datapostingtasks_taskparenttypes_parenttype_id';
GO

/* Step 4: Cleanup temporary rollback table */
IF OBJECT_ID('dbo.datapostingtasks_new_rollback', 'U') IS NOT NULL
BEGIN
    DROP TABLE dbo.datapostingtasks_new_rollback;
END
GO

/* =============================
     ROLLBACK COMPLETED
   ============================= */



-- Check if backup table DOES NOT already exist
SELECT OBJECT_ID('dbo.datapostingtasks_bkp_121225') AS backup_exists;

-- Check if new table DOES NOT already exist
SELECT OBJECT_ID('dbo.datapostingtasks_new') AS newtable_exists;

-- Check row count of existing table
SELECT COUNT(*) AS ExistingRowCount 
FROM dbo.datapostingtasks;

-- Check for FK dependencies
EXEC sp_help 'dbo.datapostingtasks';



vadlaition

SELECT 
  (SELECT COUNT(*) FROM dbo.datapostingtasks_bkp_121225) AS OldTableCount,
  (SELECT COUNT(*) FROM dbo.datapostingtasks) AS NewTableCount;

EXEC sp_help 'dbo.datapostingtasks';
EXEC sp_help 'dbo.datapostingtasks_bkp_121225';


SELECT CHECKSUM_AGG(BINARY_CHECKSUM(*)) AS NewChecksum
FROM dbo.datapostingtasks;

SELECT CHECKSUM_AGG(BINARY_CHECKSUM(*)) AS OldChecksum
FROM dbo.datapostingtasks_bkp_121225;

Test Case ID
Test Objective
SQL to Execute
Expected Result
FM-01
Validate source table exists
SELECT * FROM datapostingtasks
Table exists
FM-02
Validate backup not already created
Check OBJECT_ID
No backup exists
FM-03
Constraint rename succeeds
Run Step 1
No failure
FM-04
New table created
SELECT * FROM datapostingtasks_new
Table exists
FM-05
Data inserted correctly
Row count match
Same counts
FM-06
Data integrity preserved
Checksum match
Same checksum
FM-07
Swap old → new executed
sp_rename
Name updated
FM-08
Application smoke test
API calls / jobs
No failures



Test Case ID
Step
Expected Result
RB-01
Rename current → rollback
Table renamed
RB-02
Restore backup
Backup becomes main table
RB-03
Constraints restored
Names restored
RB-04
Schema validated
Schema identical to original
RB-05
Data validated
Row counts match
RB-06
Cleanup
rollback temp table dropped
RB-07
Application test
System works without errors



S – Schema
✔ Columns present
✔ Constraints correct
✔ Indexes valid

C – Counts
✔ Row counts match

O – Operations
✔ Insert/update/delete works

U – Upstream
✔ Jobs reading from table still run

T – Triggers/Tasks
✔ Any SQL Agent job not failing

S – Stability
✔ No deadlocks
✔ No blocking
✔ No performance regression




