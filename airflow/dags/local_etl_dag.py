"""
Local ETL Pipeline DAG - Read Excel, Validate, and Load

Simple batch processing:
  READ → VALIDATE → ANALYZE ISSUES WITH LLM → SUMMARY
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.task.trigger_rule import TriggerRule
import json
import logging

# Import task functions
import sys
from pathlib import Path

# Add paths for imports
dags_dir = Path(__file__).parent
tasks_dir = dags_dir / "tasks"
airflow_dir = dags_dir.parent  # /opt/airflow
sys.path.insert(0, str(tasks_dir))
sys.path.insert(0, str(airflow_dir))

from task_read_data import task_read_data
from task_validate_data import task_validate_data
from task_transform_data import task_transform_data

# Setup logging
logger = logging.getLogger(__name__)

# Default arguments for the DAG
default_args = {
    'owner': 'data-team',
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
    'start_date': datetime(2026, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'execution_timeout': timedelta(minutes=30),
}


# Define the DAG
dag = DAG(
    'local_etl_pipeline',
    default_args=default_args,
    description='Local ETL pipeline - Read Excel, validate, and load data',
    schedule=None,
    catchup=False,
    tags=['etl', 'local', 'data-processing'],
    max_active_runs=1,
)


# =============================================================================
# TASK 1: Read Excel and JSON Files
# =============================================================================
read_task = PythonOperator(
    task_id='read_excel_files',
    python_callable=task_read_data,
    dag=dag,
    doc="Reads all Excel and JSON files from data folder and loads into memory"
)


# =============================================================================
# TASK 2: Validate Data Quality
# =============================================================================
validate_task = PythonOperator(
    task_id='validate_data_quality',
    python_callable=task_validate_data,
    dag=dag,
    doc="Validates data against schema and business rules"
)


# =============================================================================
# TASK 3: Analyze Issues (LLM) - Analyze runtime issues and suggest transformations
# =============================================================================
def analyze_issues_with_llm(**context):
    """
    Analyzes RUNTIME DATA ISSUES using LLM.
    Based on actual data problems and transformation rules, LLM suggests code fixes.
    """
    from llm_service import EnhancedLLMService
    
    task_instance = context['task_instance']
    
    # Get validation results
    validation_result = task_instance.xcom_pull(
        task_ids='validate_data_quality',
        key='validation_result'
    )
    
    if not validation_result:
        print("No validation data found")
        return {"status": "success", "issues": []}
    
    # Get RUNTIME ISSUES (actual data problems) from validation
    runtime_issues = validation_result.get('runtime_issues', {})
    
    if not runtime_issues.get('sample_issues'):
        print("\n✓ No data quality issues found - data is clean")
        return {
            "status": "success",
            "message": "All data passed validation",
            "issues_found": 0,
            "llm_analysis": "Data quality is excellent. No issues detected."
        }
    
    # Define TRANSFORMATION RULES for the LLM
    transformation_rules = {
        "name_composition": {
            "rule": "first_name + last_name → full_name",
            "description": "Combine first_name and last_name into a single full_name field",
            "skip_if": "Both first_name and last_name are null/empty"
        },
        "date_conversion": {
            "rule": "yyyy/mm/dd → dd/mm/yyyy",
            "description": "Convert date of birth from yyyy/mm/dd format to dd/mm/yyyy format",
            "skip_if": "DOB is null or invalid format"
        },
        "null_handling": {
            "rule": "Skip null/empty columns",
            "description": "Skip records where critical fields are null or empty",
            "skip_if": "Not applicable - always skip nulls"
        },
        "email_validation": {
            "rule": "Validate email format",
            "description": "Email must be in valid format (user@domain.com)",
            "skip_if": "Email is null - skip that record"
        }
    }
    
    # Use LLM to analyze runtime issues and suggest transformations
    llm_service = EnhancedLLMService()
    
    # LLM analyzes actual data issues and transformation rules
    llm_analysis = llm_service.analyze_runtime_data_issues(
        runtime_issues=runtime_issues,
        transformation_rules=transformation_rules
    )
    
    # Push results
    task_instance.xcom_push(key='llm_analysis', value=llm_analysis)
    
    print("\n" + "="*80)
    print("RUNTIME DATA ISSUES & LLM TRANSFORMATION SUGGESTIONS")
    print("="*80)
    print(f"\nTotal Issues Found: {runtime_issues.get('total_issues', 0)}")
    print(f"Issue Patterns: {runtime_issues.get('issue_patterns', {})}")
    print(f"\nLLM Analysis Summary:\n{llm_analysis.get('issues_summary', 'N/A')}")
    
    if llm_analysis.get('suggested_transformations'):
        print(f"\n\n{'='*80}")
        print("SUGGESTED TRANSFORMATIONS")
        print("="*80)
        num_transforms = min(2, len(llm_analysis['suggested_transformations']))
        for i, transform in enumerate(llm_analysis['suggested_transformations'][:num_transforms], 1):
            print(f"\n[TRANSFORMATION {i}] {transform.get('issue_type', 'Unknown').upper()}")
            print(f"{'─'*80}")
            print(f"Problem: {transform.get('problem', 'N/A')}")
            print(f"\nTransformation Rule Applied: {transform.get('transformation_rule_applied', 'N/A')}")
            print(f"\nHow It Fixes:\n{transform.get('how_it_fixes', 'N/A')}")
            print(f"\nExample:\n{transform.get('example', 'N/A')}")
            print(f"\nSuggested Code:\n{transform.get('suggested_code', 'N/A')}")
            print(f"{'─'*80}")
    
    if llm_analysis.get('code_suggestions'):
        print(f"\n\n{'='*80}")
        print("READY-TO-USE CODE BLOCKS (COMPLETE IMPLEMENTATIONS)")
        print("="*80)
        print(f"\nTotal code blocks: {len(llm_analysis['code_suggestions'])}")
        print(f"✓ Full transformation code is provided and ready to implement\n")
        
        for i, code_block in enumerate(llm_analysis['code_suggestions'], 1):
            print(f"\n{'#'*80}")
            print(f"# CODE BLOCK {i} - COMPLETE IMPLEMENTATION")
            print(f"{'#'*80}\n")
            print(code_block)
            print(f"\n{'#'*80}\n")
    
    print("="*80 + "\n")
    
    return llm_analysis



analyze_task = PythonOperator(
    task_id='analyze_issues',
    python_callable=analyze_issues_with_llm,
    dag=dag,
    trigger_rule=TriggerRule.ALL_DONE,
    doc="LLM analyzes validation issues and suggests code changes (NO FILE EDITING)"
)


# =============================================================================
# TASK 4: Transform Data (Join names, convert dates)
# =============================================================================
transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=task_transform_data,
    dag=dag,
    doc="Transform data: join names, convert dates to DD/MM/YYYY format"
)


# =============================================================================
# TASK 5: Load Valid Data or Report Results
# =============================================================================
def load_or_report_data(**context):
    """
    Load valid data or report on issues and suggested fixes
    """
    task_instance = context['task_instance']
    
    validation_result = task_instance.xcom_pull(
        task_ids='validate_data_quality',
        key='validation_result'
    )
    
    transform_result = task_instance.xcom_pull(
        task_ids='transform_data',
        key='transformed_data'
    )
    
    llm_analysis = task_instance.xcom_pull(
        task_ids='analyze_issues',
        key='llm_analysis'
    )
    
    print("\n" + "="*80)
    print("ETL PIPELINE FINAL REPORT")
    print("="*80)
    
    if validation_result:
        total_records = validation_result.get('total_records', 0)
        valid_records = validation_result.get('valid_records', 0)
        invalid_records = validation_result.get('invalid_records', 0)
        
        print(f"\nData Summary:")
        print(f"  Total Records: {total_records}")
        print(f"  Valid: {valid_records}")
        print(f"  Invalid: {invalid_records}")
        print(f"  Quality Score: {validation_result.get('validation_score', 0)}/100")
        
        if valid_records > 0:
            print(f"\n✓ {valid_records} records are ready to load")
    
    if transform_result and transform_result.get('files_transformed', 0) > 0:
        print(f"\nData Transformation Summary:")
        print(f"  Files Transformed: {transform_result.get('files_transformed', 0)}")
        print(f"  Records Transformed: {transform_result.get('records_transformed', 0)}")
        print(f"  - Names joined from first_name/last_name")
        print(f"  - Dates converted to DD/MM/YYYY format")
        print(f"  - Email addresses validated and cleaned")
    
    # Print LLM analysis summary
    if llm_analysis:
        print(f"\n" + "="*80)
        print("LLM ANALYSIS SUMMARY & TRANSFORMATION CODE")
        print("="*80)
        
        if llm_analysis.get('issues_summary'):
            print(f"\nIssues Found:\n{llm_analysis['issues_summary']}")
        
        # Print top 2 suggested transformations
        if llm_analysis.get('suggested_transformations'):
            print(f"\n" + "─"*80)
            print("TOP 2 SUGGESTED TRANSFORMATIONS:")
            print("─"*80)
            for i, transform in enumerate(llm_analysis['suggested_transformations'][:2], 1):
                print(f"\n[{i}] {transform.get('issue_type', 'Unknown').upper()}")
                print(f"Problem: {transform.get('problem', 'N/A')}")
                print(f"Solution: {transform.get('how_it_fixes', 'N/A')}")
        
        # Print all code suggestion blocks
        if llm_analysis.get('code_suggestions'):
            print(f"\n" + "="*80)
            print(f"TRANSFORMATION CODE ({len(llm_analysis['code_suggestions'])} blocks)")
            print("="*80)
            
            for i, code_block in enumerate(llm_analysis['code_suggestions'], 1):
                print(f"\n{'#'*80}")
                print(f"# CODE BLOCK {i}")
                print(f"{'#'*80}\n")
                print(code_block)
                print(f"\n{'#'*80}\n")
        
        # Print next steps
        if llm_analysis.get('next_steps'):
            print(f"\n" + "─"*80)
            print("NEXT STEPS:")
            print("─"*80)
            for i, step in enumerate(llm_analysis['next_steps'], 1):
                print(f"{i}. {step}")
    
    print("="*80 + "\n")


load_task = PythonOperator(
    task_id='load_data_or_report',
    python_callable=load_or_report_data,
    dag=dag,
    trigger_rule=TriggerRule.ALL_DONE,
    doc="Load valid data or display issue report with LLM suggestions and code changes"
)


# =============================================================================
# TASK DEPENDENCIES
# =============================================================================
read_task >> validate_task >> analyze_task >> transform_task >> load_task
