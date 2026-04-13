"""
LLM Service for ETL Data Processing
Provides intelligent analysis and issue summarization (NO file editing)
"""

from openai import OpenAI
import os
import json
from typing import Dict, List


class EnhancedLLMService:
    """LLM service - analysis only, does NOT modify files"""
    
    def __init__(self):
        """Initialize LLM client"""
        self.api_key = os.getenv("OPENAI_API_KEY")
        self.client = OpenAI(
            api_key=self.api_key or "mock_key",
            base_url="https://openrouter.ai/api/v1"
        ) if self.api_key else None
        
        self.model = "nvidia/nemotron-3-super-120b-a12b:free"
    
    # =========================================================================
    # DATA ISSUES ANALYSIS (Analysis only - NO file editing)
    # =========================================================================
    
    def analyze_data_issues(self, issue_context: dict) -> dict:
        """
        Analyzes data validation issues and provides summary with suggestions.
        ONLY provides analysis and manual fix suggestions - does NOT modify any files.
        
        Args:
            issue_context: Dictionary with issues found during validation
            {
                "total_issues": int,
                "files_affected": int,
                "issues": [list of issues],
                "affected_files": [list of files]
            }
            
        Returns:
            Dictionary with LLM analysis and fix suggestions
        """
        if not issue_context.get("issues"):
            return {
                "summary": "No issues found",
                "records_to_fix": 0,
                "suggested_fixes": [],
                "analysis": "Data quality is excellent"
            }
        
        if not self.client:
            return self._mock_issue_analysis(issue_context)
        
        context_json = json.dumps(issue_context, indent=2)
        
        prompt = f"""You are a data quality expert analyzing validation issues.
        
DATA VALIDATION ISSUES:
{context_json}

Analyze these issues and provide:
1. Overall assessment of data quality 
2. Root causes of the issues
3. 3-5 SPECIFIC MANUAL FIXES the user should apply (do NOT write code)
4. How many records need manual correction  
5. Priority level (High/Medium/Low)

IMPORTANT: You are ONLY providing analysis and suggestions.
You are NOT modifying any files. The user will apply fixes manually.

Return JSON:
{{
  "summary": "brief summary of all issues",
  "assessment": "detailed assessment of data quality",
  "root_causes": ["cause 1", "cause 2"],
  "records_to_fix": (number),
  "priority": "High|Medium|Low",
  "suggested_fixes": [
    "specific manual fix 1",
    "specific manual fix 2",
    "specific manual fix 3"
  ],
  "analysis_note": "Use these suggestions to manually fix your data files."
}}"""

        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[{"role": "user", "content": prompt}],
                temperature=0.3,
                max_tokens=1000
            )
            
            response_text = response.choices[0].message.content
            return json.loads(response_text)
        except Exception as e:
            print(f"Issue analysis failed: {str(e)}")
            return self._mock_issue_analysis(issue_context)
    
    # =========================================================================
    # ETL ISSUES ANALYSIS (PySpark ETL Pipeline)
    # =========================================================================
    
    def analyze_etl_issues(self, etl_context: dict) -> dict:
        """
        Analyzes issues detected during PySpark ETL pipeline execution.
        Provides root cause analysis, priority assessment, and fix recommendations.
        
        Args:
            etl_context: Dictionary with ETL execution details
            {
                "pipeline_summary": {...},
                "detailed_issues": [...],
                "aggregation_stats": {...},
                "error_frequencies": {...}
            }
            
        Returns:
            Dictionary with LLM analysis, root causes, and recommendations
        """
        
        if not etl_context.get("detailed_issues"):
            return {
                "summary": "ETL pipeline executed successfully with no data quality issues",
                "assessment": "All data passed validation and transformation stages",
                "total_issues": 0,
                "priority": "NONE",
                "recommended_action": "Proceed to warehouse ingestion or downstream processing",
                "root_cause_analysis": [],
                "recommended_fixes": []
            }
        
        if not self.client:
            return self._mock_etl_analysis(etl_context)
        
        context_json = json.dumps(etl_context, indent=2)
        
        prompt = f"""You are a data engineering expert analyzing a PySpark ETL pipeline execution.

ETL EXECUTION DETAILS:
{context_json}

Analyze this ETL execution and provide:
1. Summary of what went wrong
2. Root cause analysis (technical reasons)
3. Which data quality issues are most critical
4. 3-5 specific recommendations to fix the issues (manual steps or data cleaning)
5. Impact assessment (how many records affected)
6. Next steps for the data pipeline

IMPORTANT: You are ONLY providing analysis and recommendations.
You are NOT modifying any files or data. The user will implement fixes.

Return JSON:
{{
  "summary": "brief summary of all issues found",
  "assessment": "detailed assessment of ETL execution and data quality impact",
  "total_issues": (number),
  "priority": "CRITICAL|HIGH|MEDIUM|LOW",
  "root_cause_analysis": [
    "root cause 1",
    "root cause 2",
    "root cause 3"
  ],
  "priority_issues": [
    "most critical issue 1",
    "critical issue 2",
    "critical issue 3"
  ],
  "data_quality_score": (0-100),
  "records_affected": (estimated number),
  "recommended_fixes": [
    "specific manual fix or action 1",
    "specific manual fix or action 2",
    "specific manual fix or action 3",
    "specific manual fix or action 4"
  ],
  "recommended_action": "Next step for the pipeline (load with caution/quarantine data/retry/etc)"
}}"""

        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[{"role": "user", "content": prompt}],
                temperature=0.3,
                max_tokens=1500
            )
            
            response_text = response.choices[0].message.content
            return json.loads(response_text)
        except Exception as e:
            print(f"ETL analysis failed: {str(e)}")
            return self._mock_etl_analysis(etl_context)
    
    # =========================================================================
    # ETL CODE SUGGESTION (Provide specific code changes for ETL fixes)
    # =========================================================================
    
    def suggest_etl_code_changes(self, validation_failures: dict) -> dict:
        """
        Suggests specific ETL code changes to fix data validation issues.
        Provides code snippets and explanations for ETL pipeline improvements.
        
        Args:
            validation_failures: Dictionary with validation failure details
            {
                "failed_fields": [...],
                "failure_patterns": [...],
                "sample_bad_records": [...],
                "current_validation_rules": [...]
            }
            
        Returns:
            Dictionary with suggested code changes for ETL pipeline
        """
        
        if not validation_failures.get("failed_fields"):
            return {
                "status": "success",
                "no_changes_needed": True,
                "message": "No validation failures detected. Current ETL code is sufficient."
            }
        
        if not self.client:
            return self._mock_etl_code_suggestions(validation_failures)
        
        context_json = json.dumps(validation_failures, indent=2)
        
        prompt = f"""You are an ETL and Python expert. You need to suggest specific code improvements for a data pipeline.

VALIDATION FAILURES DETECTED:
{context_json}

Provide specific code changes to:
1. Add validation for required fields (especially 'name' field)
2. Better handle missing or null values
3. Improve date format conversion and validation
4. Add better email validation
5. Handle edge cases in data transformation

For EACH issue:
- Explain what's wrong with current approach
- Provide specific Python code snippet to fix it
- Explain why this fix is important
- Give example of how it handles bad data

IMPORTANT:
- Provide ACTUAL working Python code
- Use pandas/pyspark where appropriate
- Make code production-ready
- Include error handling

Return JSON with exact structure:
{{
  "status": "success",
  "summary": "brief summary of suggested changes",
  "total_changes_suggested": (number),
  "code_changes": [
    {{
      "field_or_issue": "name",
      "problem": "Field is marked as required but no validation exists",
      "current_approach": "No validation, code silently allows NULL",
      "suggested_fix": "PYTHON CODE SNIPPET HERE",
      "explanation": "Why this fix solves the problem",
      "example": "Example of how it handles bad data"
    }},
    {{
      "field_or_issue": "date",
      "problem": "Date format varies (YYYY-MM-DD, DD/MM/YYYY, DD/01/2022 with word)",
      "current_approach": "Simple pandas to_datetime without format specification",
      "suggested_fix": "PYTHON CODE SNIPPET HERE",
      "explanation": "Handles multiple date formats and text in dates",
      "example": "Converts '01/01/2026', '2026-01-01', 'twothousand twosix /january/five' appropriately"
    }}
  ],
  "critical_improvements": [
    "Improvement 1",
    "Improvement 2"
  ],
  "next_steps": [
    "step 1",
    "step 2"
  ]
}}"""

        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[{"role": "user", "content": prompt}],
                temperature=0.3,
                max_tokens=2000
            )
            
            response_text = response.choices[0].message.content
            return json.loads(response_text)
        except Exception as e:
            print(f"Code suggestion failed: {str(e)}")
            return self._mock_etl_code_suggestions(validation_failures)
    
    def analyze_runtime_data_issues(self, runtime_issues: dict, transformation_rules: dict) -> dict:
        """
        Analyzes RUNTIME data issues (real data problems from validation).
        Given issues from actual data and transformation rules, suggests code fixes.
        
        Args:
            runtime_issues: Dictionary with actual data issues found
            {
                "total_issues": int,
                "sample_issues": [
                    {
                        "record_id": int,
                        "field": str,
                        "value": str,
                        "problem": str,
                        "severity": str
                    }
                ],
                "issue_patterns": {} or counts of each issue type
            }
            
            transformation_rules: Rules for how data should be transformed
            {
                "name_composition": "first_name + last_name → full_name",
                "date_conversion": "yyyy/mm/dd → dd/mm/yyyy",
                "null_handling": "skip null values",
                "field_mappings": {...}
            }
            
        Returns:
            Dictionary with suggested code transformations to fix issues
        """
        
        if not runtime_issues.get("sample_issues"):
            return {
                "status": "success",
                "message": "No runtime issues detected. Data is clean.",
                "suggested_transformations": [],
                "code_suggestions": []
            }
        
        if not self.client:
            return self._mock_runtime_analysis(runtime_issues, transformation_rules)
        
        context_json = json.dumps({
            "issues": runtime_issues,
            "rules": transformation_rules
        }, indent=2)
        
        prompt = f"""You are an ETL data transformation expert.
You are given RUNTIME DATA ISSUES (real data problems) and TRANSFORMATION RULES.
You must suggest PYTHON CODE TRANSFORMATIONS to fix these data issues.

RUNTIME DATA ISSUES (from actual data):
{context_json}

Your task:
1. Identify patterns in the data issues
2. Suggest specific Python transformation code
3. Apply the transformation rules provided
4. For each issue type, provide working Python code
5. Explain how the code fixes the issue

IMPORTANT:
- You are NOT modifying the ETL pipeline code structure
- You are suggesting DATA TRANSFORMATION logic
- Code should be Pandas-friendly
- Include error handling
- Focus on fixing actual data problems

Return JSON:
{{
  "status": "success",
  "issues_summary": "Brief summary of data issues",
  "suggested_transformations": [
    {{
      "issue_type": "null_first_name",
      "problem": "first_name is null or empty, cannot create full_name",
      "transformation_rule_applied": "name_composition: first_name + last_name",
      "suggested_code": "Python code snippet",
      "how_it_fixes": "Explanation of how this fixes the issue",
      "example": "Example of input and output"
    }}
  ],
  "code_suggestions": [
    "Complete code block 1 for fixing issue type 1",
    "Complete code block 2 for fixing issue type 2"
  ],
  "next_steps": [
    "Step 1 to implement",
    "Step 2 to implement"
  ]
}}"""

        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[{"role": "user", "content": prompt}],
                temperature=0.3,
                max_tokens=2000
            )
            
            response_text = response.choices[0].message.content
            return json.loads(response_text)
        except Exception as e:
            print(f"Runtime issue analysis failed: {str(e)}")
            return self._mock_runtime_analysis(runtime_issues, transformation_rules)
    
    # =========================================================================
    # MOCK METHODS (for testing without LLM API)
    # =========================================================================
    
    def _mock_runtime_analysis(self, runtime_issues: dict, transformation_rules: dict) -> dict:
        """Mock runtime issue analysis - suggests transformations"""
        
        sample_issues = runtime_issues.get('sample_issues', [])
        
        return {
            "status": "success",
            "issues_summary": f"Found {runtime_issues.get('total_issues', 0)} data quality issues. Suggesting transformations to fix.",
            "suggested_transformations": [
                {
                    "issue_type": "null_first_name",
                    "problem": "first_name is NULL or empty - cannot create full_name by combining first_name + last_name",
                    "transformation_rule_applied": "name_composition: first_name + last_name → full_name",
                    "suggested_code": """
# Handle null/empty first_name for name composition
def create_full_name(row):
    first = str(row.get('first_name', '')).strip() if pd.notna(row.get('first_name')) else ''
    last = str(row.get('last_name', '')).strip() if pd.notna(row.get('last_name')) else ''
    
    if first and last:
        return f"{first} {last}"
    elif first or last:
        return first or last
    else:
        return None  # Skip if both are null

df['full_name'] = df.apply(create_full_name, axis=1)
df = df[df['full_name'].notna()]  # Skip records with null full_name
                    """,
                    "how_it_fixes": "Handles null/empty first_name by checking both fields and creating full_name only when possible. Skips records where both are null.",
                    "example": "Input: {first_name: NULL, last_name: 'Doe'} → Output: {full_name: 'Doe'}; Input: {first_name: 'John', last_name: NULL} → Output: {full_name: 'John'}"
                },
                {
                    "issue_type": "date_format_conversion",
                    "problem": f"DOB format is yyyy/mm/dd but needs to be converted to dd/mm/yyyy",
                    "transformation_rule_applied": "date_conversion: yyyy/mm/dd → dd/mm/yyyy",
                    "suggested_code": """
from datetime import datetime

# Convert DOB from yyyy/mm/dd to dd/mm/yyyy
def convert_dob_format(dob_value):
    if pd.isna(dob_value) or dob_value == '':
        return None  # Skip null values
    
    try:
        # Try parsing as yyyy/mm/dd
        parsed_date = datetime.strptime(str(dob_value).strip(), '%Y/%m/%d')
        # Return as dd/mm/yyyy
        return parsed_date.strftime('%d/%m/%Y')
    except ValueError:
        return None  # Skip if format doesn't match

df['dob'] = df['dob'].apply(convert_dob_format)
df = df[df['dob'].notna()]  # Skip records with invalid DOB
                    """,
                    "how_it_fixes": "Parses dates in yyyy/mm/dd format and converts to dd/mm/yyyy. Skips records with invalid or null dates.",
                    "example": "Input: '2001/01/29' → Output: '29/01/2001'; Input: NULL → Skipped"
                },
                {
                    "issue_type": "null_column_skip",
                    "problem": "Null/empty values in critical columns should be skipped during transformation",
                    "transformation_rule_applied": "null_handling: skip null values",
                    "suggested_code": """
# Skip records where critical fields are null
critical_fields = ['full_name', 'dob', 'email']

for field in critical_fields:
    df = df[df[field].notna()]  # Keep only non-null rows
    df = df[df[field] != '']    # Keep only non-empty rows

# Also remove completely duplicate rows
df = df.drop_duplicates()

print(f"Rows after null/empty removal: {len(df)}")
                    """,
                    "how_it_fixes": "Removes records with null or empty values in critical fields. Prevents invalid data from being stored.",
                    "example": "Input: 10 records with 3 having null email → Output: 7 valid records"
                }
            ],
            "code_suggestions": [
                """
# COMPLETE TRANSFORMATION BLOCK for handling data issues

import pandas as pd
from datetime import datetime

def transform_json_data(input_data):
    '''Transform JSON data: combine names, convert dates, handle nulls'''
    
    df = pd.DataFrame(input_data) if isinstance(input_data, list) else pd.DataFrame([input_data])
    
    # STEP 1: Create full_name from first_name + last_name
    def create_full_name(row):
        first = str(row.get('first_name', '')).strip() if pd.notna(row.get('first_name')) else ''
        last = str(row.get('last_name', '')).strip() if pd.notna(row.get('last_name')) else ''
        
        if first and last:
            return f"{first} {last}"
        elif first or last:
            return first or last
        else:
            return None
    
    df['full_name'] = df.apply(create_full_name, axis=1)
    
    # STEP 2: Convert DOB from yyyy/mm/dd to dd/mm/yyyy
    def convert_dob_format(dob_value):
        if pd.isna(dob_value) or dob_value == '':
            return None
        
        try:
            parsed_date = datetime.strptime(str(dob_value).strip(), '%Y/%m/%d')
            return parsed_date.strftime('%d/%m/%Y')
        except ValueError:
            return None
    
    df['dob'] = df['dob'].apply(convert_dob_format)
    
    # STEP 3: Skip records with null critical fields
    critical_fields = ['full_name', 'dob']
    for field in critical_fields:
        df = df[df[field].notna()]
    
    # STEP 4: Remove duplicates
    df = df.drop_duplicates()
    
    # STEP 5: Convert back to JSON/dict format
    result_json = df.to_dict(orient='records')
    
    return result_json

# Usage in Airflow task:
# transformed_data = transform_json_data(input_json_data)
# Store in JSON file (not Parquet)
# with open('output.json', 'w') as f:
#     json.dump(transformed_data, f, indent=2)
                """
            ],
            "next_steps": [
                "1. Add the transformation functions to your ETL pipeline (task_transform_data.py)",
                "2. Call transform_json_data() on the input JSON data",
                "3. Store the transformed data in JSON format (not Parquet initially)",
                "4. Log how many records were skipped due to null values",
                "5. Return the transformed JSON for downstream processing"
            ]
        }
    
    def _mock_issue_analysis(self, issue_context: dict) -> dict:
        """Mock data issue analysis - provides suggestions only"""
        total_issues = issue_context.get('total_issues', 0)
        
        return {
            "summary": f"Found {total_issues} data quality issues across {issue_context.get('files_affected', 0)} files",
            "assessment": "Data contains quality issues that require manual correction",
            "root_causes": [
                "Missing or empty cells in required fields",
                "Invalid data type conversions",
                "Inconsistent data formatting",
                "Referential integrity violations"
            ],
            "records_to_fix": total_issues,
            "priority": "High" if total_issues > 5 else "Medium",
            "suggested_fixes": [
                "Review and fill in all empty/missing values in critical columns",
                "Verify numeric columns contain only valid positive numbers",
                "Standardize date format to YYYY-MM-DD across all records",
                "Validate email addresses contain @ and domain",
                "Check for and remove duplicate records"
            ],
            "analysis_note": "Manually apply these fixes to your Excel files and re-upload for validation."
        }
    
    def _mock_etl_analysis(self, etl_context: dict) -> dict:
        """Mock ETL issue analysis - provides intelligent recommendations"""
        pipeline_summary = etl_context.get('pipeline_summary', {})
        total_issues = pipeline_summary.get('total_issues', 0)
        
        # Determine priority based on issue count and error records
        error_records = pipeline_summary.get('error_records', 0)
        if total_issues > 10 or error_records > 100:
            priority = "CRITICAL"
        elif total_issues > 5 or error_records > 50:
            priority = "HIGH"
        else:
            priority = "MEDIUM"
        
        # Get error frequency for root cause analysis
        error_freqs = etl_context.get('error_frequencies', {})
        top_errors = sorted(error_freqs.items(), key=lambda x: x[1], reverse=True)[:3]
        
        root_causes = [
            f"Primary issue: {top_errors[0][0] if top_errors else 'Data quality issues'} ({top_errors[0][1] if top_errors else 0} occurrences)",
            "Possible data source inconsistency or validation logic mismatch",
            "Missing or malformed data in source files"
        ] if total_issues > 0 else []
        
        return {
            "summary": f"Found {total_issues} data quality issues during ETL execution",
            "assessment": f"{error_records} records failed validation out of total input records. Data requires cleaning before warehouse loading.",
            "total_issues": total_issues,
            "priority": priority,
            "root_cause_analysis": root_causes,
            "priority_issues": [
                f"{error_type} - {count} records affected" 
                for error_type, count in top_errors
            ] if top_errors else [],
            "data_quality_score": 100 - min(total_issues * 5, 50),
            "records_affected": error_records,
            "recommended_fixes": [
                "Review and validate source JSON files for missing or null values",
                "Implement stricter input validation in source systems",
                "Create data cleaning procedures for invalid amounts and dates",
                "Add data quality checks at source before ETL ingestion"
            ],
            "recommended_action": "Quarantine failed records and apply recommended fixes before loading to warehouse"
        }
    
    def _mock_etl_code_suggestions(self, validation_failures: dict) -> dict:
        """Mock ETL code suggestions - provides Python code improvements"""
        
        failed_fields = validation_failures.get('failed_fields', [])
        
        return {
            "status": "success",
            "summary": f"Suggested code improvements to handle {len(failed_fields)} validation failures",
            "total_changes_suggested": 5,
            "code_changes": [
                {
                    "field_or_issue": "name (required field)",
                    "problem": "Field 'name' is required but validation doesn't check explicitly",
                    "current_approach": "No explicit validation - silently skips invalid records",
                    "suggested_fix": "def validate_required_fields(record, required=['name', 'email']):\n    for field in required:\n        value = record.get(field, '')\n        if pd.isna(value) or str(value).strip() == '':\n            return False, f\"Required field '{field}' is missing or empty\"\n    return True, None",
                    "explanation": "Explicitly validates required fields and provides clear error messages",
                    "example": "If 'name' is null/empty, returns error 'Required field name is missing or empty'"
                },
                {
                    "field_or_issue": "date formatting (YYYY-MM-DD to DD/MM/YYYY)",
                    "problem": "Need to convert dates to DD/MM/YYYY format for Parquet storage",
                    "current_approach": "Hardcoded format or simple to_datetime",
                    "suggested_fix": "def convert_date_format(date_value, target_format='%d/%m/%Y'):\n    if pd.isna(date_value) or date_value == '':\n        return None\n    date_str = str(date_value).strip()\n    input_formats = ['%Y-%m-%d', '%d/%m/%Y', '%Y/%m/%d', '%d-%m-%Y']\n    for fmt in input_formats:\n        try:\n            parsed = datetime.strptime(date_str, fmt)\n            return parsed.strftime(target_format)\n        except ValueError:\n            continue\n    return None",
                    "explanation": "Handles multiple input formats and converts to consistent DD/MM/YYYY for Parquet",
                    "example": "Input '2001-01-29' converts to '29/01/2001'; '2026/01/29' converts to '29/01/2026'"
                },
                {
                    "field_or_issue": "email validation",
                    "problem": "Basic email check misses invalid formats",
                    "current_approach": "Just checks for '@' symbol",
                    "suggested_fix": "import re\ndef is_valid_email(email):\n    if pd.isna(email) or email == '':\n        return False\n    pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$'\n    return re.match(pattern, str(email).strip()) is not None",
                    "explanation": "Uses regex pattern to validate proper email format with domain",
                    "example": "Accepts 'user@example.com', rejects 'user@.com'"
                },
                {
                    "field_or_issue": "join first_name and last_name",
                    "problem": "If 'name' field missing, should create from first_name + last_name",
                    "current_approach": "No handling for this scenario",
                    "suggested_fix": "def join_names(record):\n    if 'name' in record and pd.notna(record.get('name')):\n        return str(record.get('name')).strip()\n    first = str(record.get('first_name', '')).strip()\n    last = str(record.get('last_name', '')).strip()\n    if first and last:\n        return f\"{first} {last}\"\n    elif first or last:\n        return first or last\n    return None",
                    "explanation": "Intelligently combines first and last names when 'name' field is missing",
                    "example": "first_name='John', last_name='Doe' creates name='John Doe'"
                },
                {
                    "field_or_issue": "error handling and logging",
                    "problem": "Pipeline failures are not clearly documented",
                    "current_approach": "No detailed error tracking",
                    "suggested_fix": "def log_validation_failure(record_id, field, value, error, severity='WARNING'):\n    timestamp = datetime.now().isoformat()\n    print(f\"[{severity}] {timestamp} - Record {record_id}, Field '{field}': {error}\")\n    return {'timestamp': timestamp, 'record_id': record_id, 'field': field, 'error': error}",
                    "explanation": "Provides clear, timestamped error messages for debugging",
                    "example": "Output: '[HIGH] 2026-04-13T14:30:40 - Record 5, Field name: Required field missing'"
                }
            ],
            "critical_improvements": [
                "Implement explicit validation for required fields ('name', 'email')",
                "Add multi-format date conversion with DD/MM/YYYY output",
                "Create regex-based email validation",
                "Handle name composition from first_name + last_name",
                "Add comprehensive error logging"
            ],
            "next_steps": [
                "Update task_validate_data.py with required field checks",
                "Update task_transform_data.py with date conversion and name joining",
                "Add enhanced logging throughout pipeline",
                "Re-run validation on source data",
                "Test with problematic records (missing names, various date formats)"
            ]
        }


# Backward compatibility functions
def analyze_data_issues(issue_context: dict) -> dict:
    """Legacy function - analyzes issues"""
    service = EnhancedLLMService()
    return service.analyze_data_issues(issue_context)


def analyze_etl_issues(etl_context: dict) -> dict:
    """Legacy function - analyzes ETL issues"""
    service = EnhancedLLMService()
    return service.analyze_etl_issues(etl_context)


def suggest_etl_code_changes(validation_failures: dict) -> dict:
    """Legacy function - suggests code changes"""
    service = EnhancedLLMService()
    return service.suggest_etl_code_changes(validation_failures)
