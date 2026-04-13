# AI ETL Pipeline Project - Client Demo Guide

## Project Overview

This is an **Intelligent ETL (Extract, Transform, Load) Pipeline** powered by **Apache Airflow** and **LLM (Large Language Model)**. It automatically processes data files, identifies quality issues, and suggests code-based transformations.

---

## What This System Does

### 1. **Read Data** (Task 1)
- ✓ Reads Excel (.xlsx, .xls) and JSON files from the `data_folder`
- ✓ Automatically detects file types
- ✓ Loads all records into memory for processing

**Input:** Data files in `airflow/data_folder/`
**Output:** Structured data ready for validation

---

### 2. **Validate Data** (Task 2)
- ✓ Validates data against business rules:
  - Required fields (name, email)
  - Email format validation
  - Date format validation (YYYY-MM-DD or DD/MM/YYYY)
  - Numeric field validation (amounts must be positive)
- ✓ Tracks ALL issues found (critical, high, medium severity)
- ✓ Generates validation score (0-100)

**Input:** Raw data from Task 1
**Output:** Validation report with detailed issues

**Example Issues Detected:**
- Missing names (critical)
- Invalid email formats (high)
- Invalid date formats (high)
- Negative amounts (medium)

---

### 3. **Analyze Issues with LLM** (Task 3)
- ✓ AI analyzes actual data problems
- ✓ Suggests specific code transformations
- ✓ Generates 2+ complete Python code blocks ready to use
- ✓ Provides step-by-step transformation guidance

**Input:** Validation issues and transformation rules
**Output:** 
- Problem analysis
- Suggested transformations (top 2)
- Complete Python code blocks

**Example Output:**
```
TRANSFORMATION 1: MISSING_NAME
Problem: first_name and last_name are both null/empty
Suggested Code: [Python function to handle name composition]

TRANSFORMATION 2: INVALID_DATE_FORMAT  
Problem: Date values like 'invalid-date' cannot be parsed
Suggested Code: [Python function to convert dates]
```

---

### 4. **Transform Data** (Task 4)
- ✓ Applies transformations:
  - Combines first_name + last_name → full_name
  - Converts dates from yyyy/mm/dd → dd/mm/yyyy
  - Skips invalid/null records
- ✓ **Saves cleaned data as JSON** to `airflow/data_folder/output/`
- ✓ Generates summary metrics

**Input:** Raw data + transformation rules
**Output:** Cleaned data files (JSON format)

**Example Output Files:**
- `sample_data_cleaned.json` 
- `sample_data_with_issues_cleaned.json`

---

### 5. **Final Report** (Task 5)
- ✓ Displays complete pipeline summary:
  - Data quality metrics
  - Transformation results
  - LLM analysis summary
  - Output file locations
  - Next steps for implementation

---

## Key Features

### ✨ Intelligent Features
1. **LLM-Powered Analysis** - AI analyzes actual data problems
2. **Code Generation** - Provides ready-to-use Python code
3. **Data Quality Scoring** - Automatic quality metrics
4. **Transformation Rules** - Business logic driven
5. **Error Handling** - Comprehensive issue tracking

### 🔒 Data Safety
- ✓ NO file modifications to source files
- ✓ All output saved to separate folder
- ✓ Original data untouched
- ✓ Audit trail of all operations

### 📊 Output Formats
- ✓ JSON files (human-readable)
- ✓ Structured validation reports
- ✓ LLM analysis with code suggestions
- ✓ Detailed metrics and summaries

---

## Project Structure

```
ai-etl-pipeline/
├── docker-compose.yml          # Docker configuration
├── dockerfile                   # Docker image definition
├── requirements.txt             # Python dependencies
├── airflow/
│   ├── airflow.cfg             # Airflow configuration
│   ├── dags/
│   │   ├── local_etl_dag.py    # Main ETL pipeline (ACTIVE)
│   │   ├── llm_service.py       # LLM integration service
│   │   └── tasks/
│   │       ├── task_read_data.py        # Read Excel/JSON files
│   │       ├── task_validate_data.py    # Validate data quality
│   │       └── task_transform_data.py   # Transform & save data
│   └── data_folder/
│       ├── sample_data.json             # Example clean data
│       └── output/                       # Cleaned data output ⬅ HERE
│
└── PROJECT_OVERVIEW.md         # This file
```

---

## How to Use

### 1. **Add Input Data**
Place your Excel or JSON files in:
```
airflow/data_folder/
```

### 2. **Run the Pipeline**
```bash
# In Docker
docker-compose up

# Navigate to http://localhost:8080
# Trigger the "local_etl_pipeline" DAG
```

### 3. **View Results**
- **Logs:** See real-time output in Airflow UI
- **Cleaned Data:** Check `airflow/data_folder/output/` for cleaned JSON files
- **Code Suggestions:** Review the LLM-generated Python code in logs

### 4. **Implement Fixes**
Copy the suggested Python code from logs and integrate into your ETL pipelines

---

## Data Flow Diagram

```
INPUT DATA
   ↓
[Task 1] READ DATA → Loads Excel/JSON files
   ↓
[Task 2] VALIDATE → Check quality, find issues
   ↓
[Task 3] ANALYZE WITH LLM → AI suggests code fixes
   ↓
[Task 4] TRANSFORM → Apply transformations, save as JSON
   ↓
[Task 5] REPORT → Display summary & next steps
   ↓
OUTPUT: Cleaned JSON + Code Suggestions
```

---

## Example Transformation Results

### Before (Invalid Data)
```json
[
  {"name": "", "first_name": "", "last_name": "", "dob": "invalid-date"},
  {"name": "John Doe", "first_name": "John", "last_name": "Doe", "dob": "2001/01/29"},
  {"name": "", "first_name": "Jane", "last_name": "", "dob": "1995/05/15"}
]
```

### After (Cleaned Data)
```json
[
  {"name": "John Doe", "first_name": "John", "last_name": "Doe", "dob": "29/01/2001", "full_name": "John Doe"},
  {"name": "Jane", "first_name": "Jane", "last_name": "", "dob": "15/05/1995", "full_name": "Jane"}
]
```

**Note:** First record skipped (missing both first_name and last_name)

---

## LLM Suggestions Example

```
TRANSFORMATION 1: MISSING_NAME
Problem: first_name and last_name are both null/empty
How It Fixes: Creates full_name only when both fields present

TRANSFORMATION 2: INVALID_DATE_FORMAT
Problem: Date values like 'invalid-date' cannot be parsed
How It Fixes: Parses valid date formats and converts to dd/mm/yyyy
```

---

## Technology Stack

| Component | Purpose |
|-----------|---------|
| **Apache Airflow** | Workflow orchestration & scheduling |
| **Python** | Data processing & transformation |
| **Pandas** | Data manipulation & validation |
| **LLM (OpenRouter)** | AI analysis & code suggestions |
| **Docker** | Containerization & deployment |
| **JSON** | Data format (human-readable) |

---

## Business Benefits

✅ **Automated Quality Checks** - No manual validation needed
✅ **AI-Powered Insights** - LLM identifies patterns and issues
✅ **Code Generation** - Ready-to-use transformation code
✅ **Scalability** - Process 1 to 1M records
✅ **Audit Trail** - Complete logging of all operations
✅ **Data Safety** - Source data never modified

---

## Next Steps for Client Implementation

1. **Data Source Integration**
   - Connect to your data source (database, API, files)
   - Add custom input adapters as needed

2. **Custom Rules**
   - Define your specific validation rules
   - Add industry-specific transformations

3. **Output Integration**
   - Store cleaned data in your data warehouse
   - Integrate with downstream systems

4. **Scheduling**
   - Set up daily/weekly/monthly runs
   - Configure alerts & notifications

---

## Support & Customization

The pipeline is fully customizable:
- Add new validation rules in `task_validate_data.py`
- Extend transformations in `task_transform_data.py`
- Modify LLM prompts in `llm_service.py`

---

**Version:** 1.0  
**Last Updated:** April 13, 2026  
**Status:** Production Ready ✅
