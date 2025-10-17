Snowflake Data Validation & Automation Framework
1. Overview

The Snowflake Automation Testing Framework is a reusable, Python-based solution designed to automate data validation and schema verification for Snowflake databases. 
It eliminates manual effort by automating end-to-end checks — from reading configurations and running SQL validations to producing Excel-based reports with results.

2. Primary Objective

To ensure data integrity, consistency, and schema alignment between Snowflake environments (e.g., between staging and target tables) through automation — minimizing human error and maximizing repeatability.

3. Key Use Case
Area	Problem	Framework Solution
Schema validation	Manual comparison of columns, datatypes, and keys	Automated DDL (schema) comparison between Snowflake tables
Data duplication	Hard to identify duplicate rows across large data sets	Automated duplicate record validation
Null value checks	Tedious to verify null completeness column-by-column	Null validation automation per column
Data patterns	Regex validations needed for fields like email, date, or IDs	Regex-driven pattern validation (defined in Excel)
Auditability	No single location for all validation results	Centralized Excel-based output reporting
Time efficiency	Manual SQL runs for each validation	Automated SQL execution via Python + SQLAlchemy

4. Why This Framework is Needed

The framework provides a structured, automated approach to data validation by eliminating manual SQL efforts, reducing human errors, and improving the overall consistency of results. 
Its modular design allows easy maintenance, scalability, and integration into CI/CD workflows.


| **Challenge**               | **Impact**                          | **Framework Advantage**                  |
| --------------------------- | ----------------------------------- | ---------------------------------------- |
| Manual validation process   | Time-consuming and inconsistent     | Automated & repeatable execution         |
| Schema drift undetected     | Potential data loss or mismatch     | Automated schema DDL comparison          |
| Lack of audit trail         | No evidence of what was tested      | Excel-based summary & result tracking    |
| Repeated effort per project | Code duplication & human dependency | Config-driven & reusable across projects |
| Expensive external tools    | High license & maintenance costs    | 100% open-source and customizable        |


5. Framework Architecture

The framework follows a modular design with clear separation between configuration, connection, validation logic, and reporting. 
All validations are defined in Excel inputs, executed via BDD tests, and written back to Excel outputs.

project_root/
│
├── common/
│   ├── config_loader.py         → Reads Snowflake config (JSON)
│   ├── snowflake_connection.py  → Establishes Snowflake connection
│   ├── table_loader.py          → Reads Excel input
│   ├── excel_writer.py          → Writes results to Excel
│   └── validation_tracker.py    → Tracks run summaries
│
├── validators/
│   ├── ddl_lib.py               → Schema-level (DDL) comparison
│   ├── dup_val_exc_audit.py     → Duplicate record validation
│   ├── pattern_validation.py    → Regex pattern checks
│   └── null_validation.py       → Null/Not Null checks
│
├── features/
│   ├── ddl_compare.feature      → BDD feature file
│   └── steps/
│       └── ddl_steps.py         → Behave step definitions
│
├── config/
│   └── config.json              → Snowflake credentials & default parameters
│
└── output/
    └── *.xlsx                   → Generated validation reports


6. End-to-End Workflow

1. Enter table details in Excel (input file)
2. Run BDD test case (Behave command)
3. Framework reads Snowflake connection and executes validations
4. Output is generated as Excel reports in the 'output' folder

| **Step** | **Action**                                                   | **Component**                            | **Outcome**                                                        |
| -------- | ------------------------------------------------------------ | ---------------------------------------- | ------------------------------------------------------------------ |
| 1        | Enter the list of tables to validate in the Excel input file | `input_tables.xlsx`                      | Each row defines a database, schema, and table name                |
| 2        | Run the BDD test (e.g., duplicate validation)                | `behave -k features/ddl_compare.feature` | Executes Behave BDD steps using Python and SQLAlchemy              |
| 3        | Framework reads Snowflake connection details                 | `config/config.json`                     | Establishes connection via secure authenticator                    |
| 4        | Validation scripts execute automatically                     | `validators/*.py`                        | SQL queries run directly inside Snowflake (no data download)       |
| 5        | Results are written into Excel                               | `output/{table_name}.xlsx`               | Separate sheet per validation type (Duplicate, Null, Schema, etc.) |
| 6        | Final summary report generated                               | `output/summary.xlsx`                    | Contains validation statuses for all tables                        |


7. Example: Duplicate Validation Workflow

Step 1: Enter the table name(s) into the Excel file (e.g., .PUBLIC.MY_TABLE)
Step 2: Run the command 'behave -k features/ddl_compare.feature'
Step 3: Framework connects to Snowflake and performs duplicate record checks
Step 4: Excel output (output/MY_TABLE.xlsx) includes duplicate counts and query details
Step 5: A summary Excel (output/summary.xlsx) consolidates all validation results


| **Step** | **Description**                                                                                              |
| -------- | ------------------------------------------------------------------------------------------------------------ |
| 1️⃣      | **Input Preparation:** Tester enters the table name(s) into the Excel file (e.g., `HOSCDA.PUBLIC.MY_TABLE`). |
| 2️⃣      | **Run Test:** Execute command:                                                                               |

behave -k features/ddl_compare.feature
``` |
| 3️⃣ | **Execution:** Framework connects to Snowflake, reads the table schema, and executes a “duplicate record” SQL check. |
| 4️⃣ | **Output:** An Excel file (e.g., `output/MY_TABLE.xlsx`) is created with a sheet named `duplicate_check` showing results:  
   - Total duplicate count  
   - Columns used for grouping  
   - Query used  
   - Error details (if any) |
| 5️⃣ | **Summary Report:** A separate summary sheet (`output/summary.xlsx`) consolidates validation results for all tables. |

---

## **8. Excel Configuration**

### **Input Excel (Example: `input_tables.xlsx`)**

| **Database** | **Schema** | **Table** | **Validation Type** |
|---------------|------------|------------|----------------------|
| HOSCDA | PUBLIC | ABC | DUPLICATE |
| CDA | PUBLIC | XYZ | DDL_COMPARE |
| DG_DX | RAW | MEMBER | NULL_CHECK |

---

### **Configuration JSON (Example: `config.json`)**

```json
{
  "snowflake": {
    "user": "",
    "account": "",
    "authenticator": "externalbrowser",
    "warehouse": "",
    "database": "",
    "role": "",
    "schema": "",
    "schema1": "DG_DX"
  },
  "table_name": "",
  "table_name1": "",
  "primary_keys": ["S"],
  "data_pattern_checks": {
    "FRST_NM": "\\d"
  }
}

| **Column Name** | **Validation Type** | **Result**                          | **Comments**    |
| --------------- | ------------------- | ----------------------------------- | --------------- |
| Table           | Duplicate Check     | 0 duplicates found                  | ✅ Passed        |
| Table           | Schema Compare      | Column mismatch: ID length 10 vs 12 | ⚠️ Needs Review |
| Table           | Null Check          | 3 nulls in column FRST_NM           | ❌ Failed        |



| **Technology**      | **Purpose**                       |
| ------------------- | --------------------------------- |
| Python              | Core automation language          |
| Snowflake Connector | Database connectivity             |
| SQLAlchemy          | Engine for SQL execution          |
| Behave (BDD)        | Test scenario execution framework |
| Pandas              | Data handling & Excel I/O         |
| OpenPyXL            | Writing reports to Excel          |


8. Benefits
• Automation of repetitive SQL validation tasks
• Excel-driven configuration for business-friendly usage
• Centralized and auditable reporting
• Reusable, scalable, and secure design
• Reduces validation effort by up to 90%
| **Alternative**                                 | **Limitation**                                   |
| ----------------------------------------------- | ------------------------------------------------ |
| Manual SQL queries                              | Time-consuming, prone to human error             |
| Commercial tools (IcedQ, QuerySurge, Tricentis) | Costly and less flexible                         |
| Custom one-off scripts                          | Lack standardization and reporting               |
| Our Framework                                   | Open-source, consistent, scalable, and auditable |


| **Planned Feature**   | **Description**                                                           |
| --------------------- | ------------------------------------------------------------------------- |
| CI/CD Integration     | Run tests automatically during deployment (e.g., Jenkins, GitHub Actions) |
| Email Notifications   | Email test summary with Excel attachments                                 |
| Dashboard Integration | Real-time web-based visualization of validation metrics                   |
| Multi-Cloud Support   | Extend framework to AWS Athena, BigQuery, and Redshift                    |
| Auto Healing          | Automatically fix known schema mismatches (optional feature)              |



9. Summary
This framework automates and standardizes Snowflake data validations — transforming manual, repetitive checks into a reliable, Excel-driven, and auditable testing process. 
It empowers data teams to validate large data sets quickly, accurately, and consistently.

