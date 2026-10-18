# Data Extraction Documentation

## 1. Data Source and Extraction Specification
### 1. Source System

- **System Name:** 
EduLITE: Learning Performance Monitoring System With Assessment Analytics For Navotas Elementary School

- **Purpose:** 
The EduLITE system is designed to manage student academic records, assessment results, and learning performance data to help teachers monitor student progress and identify areas that may require academic support.

### 2. Source Database

- **Database Management System:** MongoDB
- **Source:** CSV, JSON

### 3. Extraction Method

**Extraction Process:**
The required data will be extracted from the EduLite MongoDB Atlas database. The extraction will retrieve relevant records from the Students, Sections, Subjects, Assessments, and Assessment Scores collections.

**Extraction Tool/Technology:**
Python will be used as the extraction script, with PyMongo to connect to MongoDB Atlas and retrieve the required records. Prefect will manage the execution of the extraction workflow.

**Extraction Type:**
The pipeline will use **full extraction**, meaning all available relevant records from the selected collections will be retrieved during each extraction run.

**Incremental Extraction Field and Logic:**
Not applicable because the pipeline uses full extraction. No timestamp or extraction window will be used to determine which records are extracted.

**Extraction Schedule:**
The extraction will run **daily at 12:00 AM**. It may also be manually triggered when updated EduLite data needs to be processed.


### 4. Extraction Scope
**The extraction process will retrieve all relevant records from the following EduLITE MongoDB collections:**
**Students** – student information such as student ID, name, grade level, and section.
**Sections** – section information such as section ID, section name, and grade level.
**Subjects** – subject information such as subject ID, subject name, and grade level.
**Assessments** – assessment information such as assessment ID, subject, assessment type, and assessment date.
**Assessment Scores** – student assessment results such as student ID, assessment ID, score, and total score.

### 5. Source Limitations and Assumptions
**Schema Changes:**
Changes to the MongoDB collection structure or field names may affect the extraction process and require updates to the extraction script.

**Database Access:**
The extraction depends on access to the EduLITE MongoDB Atlas database and valid authentication credentials.

**Historical Records:**
The extraction assumes that the required student, subject, section, assessment, and assessment score records are available in the source database.

**Data Availability:**
The extraction assumes that the required collections and fields exist and contain the necessary records for the pipeline.

**Source Connectivity:**
Temporary network or MongoDB Atlas connection problems may interrupt the extraction process.

## 2. Source Tables and Column Specifications


| **Name** | **Purpose** | **Important Columns and Data Types** | **Primary Key / Unique Identifier** | **Relevant Relationship** | **Reason for Inclusion** |
|---|---|---|---|---|---|
| Students | Stores student information needed for academic performance records. | `_id` (ObjectId); `name` (String); `grade` (Integer); `sectionId` (ObjectId); `subjectIds` (Array) | `_id` | `sectionId` → `Sections`; `studentId` → `Assessment Scores`; `subjectIds` → `Subjects` | Needed to identify the student, grade level, section, and subjects associated with the student. |
| Sections | Stores the sections assigned to students. | `_id` (ObjectId); `section_name` (String) | `_id` | `_id` → `Students.sectionId` | Needed to identify the student's section. | | Subjects | Stores the subjects associated with student assessments. | `_id` (ObjectId); `subject_name` (String) | `_id` | `_id` → `Assessments.subjectId` | Needed to identify the subject associated with assessment results. |
| Assessments | Stores information about assessments used to evaluate students. | `_id` (ObjectId); `subjectId` (ObjectId); `totalItems` (Integer) | `_id` | `subjectId` → `Subjects`; `_id` → `Assessment Scores.assessmentId` | Needed to connect assessment scores to subjects and determine the total number of items for grade calculations. |
| Assessment Score | Stores the scores obtained by students in assessments. | `_id` (ObjectId); `studentId` (ObjectId); `assessmentId` (ObjectId); `score` (Integer) | `_id` | `studentId` → `Students`; `assessmentId` → `Assessments` | Provides the actual assessment scores used to calculate student performance and average grades. | 


## Descriptive Analytics

### Table Name: Class Average Scores

| ID | Grade | Section | Student Name | Student Grade | Class Subject | Class Average Scores |

### Table Name: Class Performance Summaries

| ID | Grade | Section | Student Name | Student Grade | Class Subject | Class Performance Average |

## Predictive Analytics

### Table Name: Student At Risk of Low Performance

| ID | Student Name | Class Subject | Section | Grade Level | Student Average Score |

### Table Name: Students Likely To Achieve High Performance

| ID | Student Name | Class Subject | Section | Grade Level | Student Average Score |

## 3. Extraction Validation and Data Quality Checks

The extraction validation process ensures that the required data is successfully retrieved from the EduLite MongoDB Atlas database. The checks verify database connectivity, collection availability, required fields, identifiers, record counts, and extraction errors before the extracted data is handed over to the Transformation Stage.

| **Check Name** | **Target** | **Purpose** | **Validation Criteria** |
|---|---|---|---|
| **Source Connection** | MongoDB Atlas connection | To ensure that the source database is accessible before extraction begins. | **PASS** if the connection is successful and the database can be accessed. **FAIL** if the connection cannot be established. |
| **Required Collections** | Students, Sections, Subjects, Assessments, and Assessment Scores | To confirm that all required source collections are available. | **PASS** if all five collections exist. **FAIL** if any required collection is missing. |
| **Required Columns** | Fields required from each source collection | To ensure that the fields needed for the pipeline are available. | **PASS** if all required fields exist in their respective collections. **FAIL** if any required field is missing. |
| **Required Identifiers** | `_id`, `studentId`, `assessmentId`, `sectionId`, and `subjectId`, where applicable | To ensure that records contain the identifiers needed to identify records and connect related data. | **PASS** if required identifiers are present and non-null in applicable records. **FAIL** if required identifiers are missing or null. |
| **Primary Key Uniqueness** | `_id` in each source collection | To ensure that each source record has a unique identifier. | **PASS** if no duplicate `_id` values are found within a collection. **FAIL** if duplicate identifiers are detected. |
| **Record Count** | Records read and extracted from each collection | To monitor how many records are retrieved during extraction. | **PASS** if the number of extracted records matches the number of records read by the extraction process. **FAIL** if a discrepancy is detected. |
| **Extraction Completeness** | Extracted records from all five collections | To verify that the full extraction retrieves all available records within the selected collections. | **PASS** if the extraction finishes and all records returned by the source queries are accounted for in the extracted output. **FAIL** if records are missing from the output or extraction ends prematurely. |
| **Extraction Errors and Interruptions** | Extraction script and Prefect workflow execution | To identify failed queries, connection interruptions, or incomplete extraction runs. | **PASS** if all required extraction tasks finish without unhandled errors. **FAIL** if a required task fails or extraction is interrupted before completion. |

### Validation Outcome

The extracted data will be approved for handover to the Transformation Stage when all critical validation checks pass. If a connection problem or extraction error occurs, the pipeline will record the error and stop the handover until the extraction can be completed successfully. Any record-count discrepancy or missing required field will be reported for investigation.

These checks validate the extraction process only. Data cleaning, value standardization, grade calculations, and other business transformations will be handled in the next stage.

## 4. Extraction Metadata and Log Specification

The EduLITE Data Pipeline records extraction metadata and logs to monitor, validate, and trace each extraction run. These records contain information about execution status, extracted collections, record counts, validation results, and errors.

### Extraction Metadata Fields

| *Field Name* | *Purpose* | *Example Value or Format* |
|---|---|---|
| pipeline_run_id | Uniquely identifies each pipeline execution and links extraction logs belonging to the same run. | b2e9fab9-742a-48d6-a9f8-4d9974e10b4d (UUID) |
| table_or_file_name | Identifies the source collection being extracted or the pipeline summary entry. | students, sections, subjects, assessments, assessmentScores, PIPELINE_SUMMARY |
| output_file | Records the destination file where extracted data is stored. | pipeline/data/raw/students.json (example relative path; logs store the full path) |
| extraction_start_timestamp | Records the date and time when the extraction started. | 2026-10-15T09:15:30.650768+00:00 (ISO 8601) |
| extraction_end_timestamp | Records the date and time when the extraction finished. | 2026-10-15T09:15:30.709084+00:00 (ISO 8601) |
| extraction_status | Indicates whether the extraction completed successfully or encountered an error. | SUCCESS (String) |
| records_read | Records the total number of records read from the source collection. | 3 (Integer) |
| records_extracted | Records the number of records successfully extracted into the output file. | 3 (Integer) |
| records_rejected | Records the number of records rejected during extraction. | 0 (Integer) |
| extraction_window | Identifies whether the extraction processes all records or a selected subset. | FULL (String) |
| validation_results | Stores the results of validation checks performed during extraction. | JSON Object containing Boolean and Integer values |
| error_message | Records error details when an extraction encounters a problem. | null (No error) |
| extraction_duration_seconds | Measures the time required to complete the extraction process. | 0.058 (Float, seconds) |

### Extraction Validation Log Fields

The validation_results object contains the following fields, depending on the extraction run and log entry.

| *Field Name* | *Purpose* | *Example Value or Format* |
|---|---|---|
| source_read | Confirms that the source collection was successfully read. | true (Boolean) |
| required_fields_present | Verifies that extracted records contain the required fields. | true (Boolean) |
| required_identifiers_present | Confirms that required identifiers exist in extracted records. | true (Boolean) |
| primary_key_unique | Verifies that primary key values are unique within the extracted collection. | true (Boolean) |
| missing_field_count | Counts records or field occurrences with missing required fields, as defined by the validator. | 0 (Integer) |
| missing_identifier_count | Records the number of missing required identifiers. | 0 (Integer) |
| duplicate_id_count | Records the number of duplicate identifiers detected. | 0 (Integer) |
| invalid_record_count | Records the number of records that failed validation checks. | 0 (Integer) |
| record_counts_match | Confirms that the number of records read matches the number of successfully extracted records. | true (Boolean) |
| output_file_exists | Confirms that the extracted JSON output file exists. | true (Boolean) |
| source_connection | Confirms successful connection to the source database. | true (Boolean) |
| required_collections | Confirms that the required source collections are available. | true (Boolean) |
| all_collections_completed | Confirms that all selected collections completed extraction. | true (Boolean) |
| all_collection_checks_passed | Confirms that all collection-level validation checks passed. | true (Boolean) |

### Use of Metadata and Logs

*Monitoring:* Extraction logs are used to monitor pipeline execution through timestamps, extraction statuses, record counts, and execution durations. These fields help determine whether each extraction completed successfully and whether the expected records were extracted.

*Troubleshooting:* Logs help identify extraction problems by recording errors, failed validation checks, missing fields, duplicate identifiers, and record-count mismatches. This information allows developers to locate the affected collection and investigate the cause of the issue.

*Auditing:* Each extraction is assigned a unique pipeline_run_id, allowing records to be traced to a specific execution. Timestamps, collection names, extracted record counts, and validation results provide evidence of when and how extraction operations were performed.

*Recovery:* When an extraction fails, the logs can identify incomplete or unsuccessful collections that need to be processed again. Since the current pipeline uses full extraction, affected collections can be re-extracted and validated to confirm that the output files contain the expected records. Existing files should be backed up when necessary before rerunning an extraction.
