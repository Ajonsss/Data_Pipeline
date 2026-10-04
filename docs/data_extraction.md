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