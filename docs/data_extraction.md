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
