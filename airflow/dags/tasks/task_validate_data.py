"""
Task 2: Validate Excel and JSON Data
Enhanced validation with required fields, email format, and date format checking
"""
import pandas as pd
from datetime import datetime
import json
import re


# Define required fields by data type
REQUIRED_FIELDS = {
    'name': 'Required - Cannot be null or empty',
    'first_name': 'Optional but if present, cannot be empty',
    'last_name': 'Optional but if present, cannot be empty',
    'email': 'Required - Must be valid email format',
    'date': 'Required - Must be in YYYY-MM-DD or DD/MM/YYYY format',
    'amount': 'Optional but if present, must be numeric',
    'dob': 'Optional but if present, must be in YYYY-MM-DD format'
}


def get_scalar_value(val):
    """
    Extract scalar value from pandas Series or other array-like objects.
    Handles cases where values from records might be pandas Series instead of scalars.
    """
    if val is None:
        return None
    if hasattr(val, 'iloc'):  # pandas Series
        return val.iloc[0] if len(val) > 0 else None
    if hasattr(val, '__iter__') and isinstance(val, (list, tuple)):
        return val[0] if len(val) > 0 else None
    return val


def is_valid_email(email):
    """Check if email is in valid format"""
    # Handle pandas Series or array values
    email = get_scalar_value(email)
    
    if pd.isna(email) or email is None or email == "":
        return False
    email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    return re.match(email_pattern, str(email)) is not None


def is_valid_date_format(date_str):
    """Check if date string is in acceptable format"""
    # Handle pandas Series or array values
    date_str = get_scalar_value(date_str)
    
    if pd.isna(date_str) or date_str is None or date_str == "":
        return False, None
    
    date_str = str(date_str).strip()
    
    # Try common date formats
    formats = [
        '%Y-%m-%d',      # 2026-01-29
        '%d/%m/%Y',      # 29/01/2026
        '%m/%d/%Y',      # 01/29/2026
        '%Y/%m/%d',      # 2026/01/29
        '%d-%m-%Y',      # 29-01-2026
    ]
    
    for fmt in formats:
        try:
            parsed_date = datetime.strptime(date_str, fmt)
            return True, parsed_date
        except ValueError:
            continue
    
    return False, None


def validate_excel_data(data_files: list) -> dict:
    """
    Validate Excel and JSON data files with enhanced rules
    
    Args:
        data_files: List of file data dictionaries
        
    Returns:
        Validation report with detailed issues found
    """
    result = {
        "status": "success",
        "timestamp": datetime.now().isoformat(),
        "total_records": 0,
        "valid_records": 0,
        "invalid_records": 0,
        "files_with_errors": {},
        "issues": [],
        "validation_score": 100,
        "detailed_issues": [],
        "runtime_issues": {  # NEW: For LLM analysis
            "total_issues": 0,
            "sample_issues": [],
            "issue_patterns": {},
            "issue_counts": {}
        }
    }
    
    if not data_files:
        result["status"] = "warning"
        result["issues"].append("No files to validate")
        return result
    
    # Track issue patterns
    issue_patterns = {}
    
    # Validate each file
    for file_data in data_files:
        filename = file_data.get("filename", "unknown")
        rows = file_data.get("rows", 0)
        data_records = file_data.get("data", [])
        
        result["total_records"] += rows
        file_issues = []
        
        # Check each record
        for idx, record in enumerate(data_records):
            record_errors = []
            record_id = get_scalar_value(record.get('id', idx + 1))
            
            # Check for required fields (name)
            name_val = get_scalar_value(record.get('name'))
            if 'name' not in record or name_val is None or pd.isna(name_val) or str(name_val).strip() == "":
                # Ensure scalar checks for first_name and last_name
                first_name_val = get_scalar_value(record.get('first_name'))
                last_name_val = get_scalar_value(record.get('last_name'))
                
                first_name = str(first_name_val).strip() if first_name_val and pd.notna(first_name_val) else ''
                last_name = str(last_name_val).strip() if last_name_val and pd.notna(last_name_val) else ''

                if not first_name and not last_name:
                    # TRUE missing - cannot create name
                    record_errors.append({
                        'field': 'name',
                        'severity': 'CRITICAL',
                        'message': "Required field 'name' is missing and cannot be created from first_name/last_name"
                    })
                    # Track for LLM
                    issue_key = 'missing_name'
                    issue_patterns[issue_key] = issue_patterns.get(issue_key, 0) + 1
                    if len(result["runtime_issues"]["sample_issues"]) < 3:  # Keep first 3 samples
                        result["runtime_issues"]["sample_issues"].append({
                            "record_id": record_id,
                            "field": "name",
                            "value": str(name_val) if name_val is not None else 'NULL',
                            "problem": "name is missing/empty and first_name/last_name are also empty - cannot create full name",
                            "severity": "CRITICAL"
                        })
            
            # Validate each field
            for col_name, value in record.items():
                # Extract scalar value to avoid ambiguous truth value errors
                value = get_scalar_value(value)
                
                # Skip ID field and null values (unless required)
                if col_name.lower() in ['id']:
                    continue
                    
                if value is None or pd.isna(value):
                    continue
                
                value_str = str(value).strip()
                if value_str == "":
                    continue
                
                # Email validation
                if col_name.lower() in ['email', 'email_address']:
                    if not is_valid_email(value):
                        record_errors.append({
                            'field': col_name,
                            'severity': 'HIGH',
                            'message': f"Invalid email format: {value}"
                        })
                        issue_key = 'invalid_email'
                        issue_patterns[issue_key] = issue_patterns.get(issue_key, 0) + 1
                        if len(result["runtime_issues"]["sample_issues"]) < 3:
                            result["runtime_issues"]["sample_issues"].append({
                                "record_id": record_id,
                                "field": col_name,
                                "value": str(value),
                                "problem": f"email format invalid - should be user@domain.com",
                                "severity": "HIGH"
                            })
                
                # Date validation (date, dob, created_date, etc.)
                if col_name.lower() in ['date', 'dob', 'created_date', 'updated_date', 'birth_date']:
                    is_valid, parsed_date = is_valid_date_format(value)
                    if not is_valid:
                        record_errors.append({
                            'field': col_name,
                            'severity': 'HIGH',
                            'message': f"Invalid date format: {value}. Expected YYYY-MM-DD or DD/MM/YYYY"
                        })
                        issue_key = 'invalid_date_format'
                        issue_patterns[issue_key] = issue_patterns.get(issue_key, 0) + 1
                        if len(result["runtime_issues"]["sample_issues"]) < 3:
                            result["runtime_issues"]["sample_issues"].append({
                                "record_id": record_id,
                                "field": col_name,
                                "value": str(value),
                                "problem": f"date format not recognized - should be yyyy/mm/dd or dd/mm/yyyy",
                                "severity": "HIGH"
                            })
                
                # Numeric validation
                if col_name.lower() in ['amount', 'quantity', 'price', 'cost']:
                    try:
                        num_val = float(value)
                        if num_val < 0:
                            record_errors.append({
                                'field': col_name,
                                'severity': 'MEDIUM',
                                'message': f"Negative value not allowed: {value}"
                            })
                    except (ValueError, TypeError):
                        record_errors.append({
                            'field': col_name,
                            'severity': 'HIGH',
                            'message': f"Must be numeric, got: {value}"
                        })
            
            # Track issues
            if record_errors:
                file_issues.append({
                    "row": idx + 2,
                    "record_id": record_id,
                    "errors": record_errors
                })
                result["detailed_issues"].append({
                    "file": filename,
                    "row": idx + 2,
                    "record_id": record_id,
                    "errors": record_errors
                })
            else:
                result["valid_records"] += 1
        
        result["invalid_records"] = result["total_records"] - result["valid_records"]
        
        if file_issues:
            result["files_with_errors"][filename] = file_issues
            # Track first few issues
            for issue in file_issues[:3]:
                for error in issue["errors"]:
                    result["issues"].append(
                        f"{filename} (Row {issue['row']}, ID {issue['record_id']}): "
                        f"[{error['severity']}] {error['field']} - {error['message']}"
                    )
            
            # Reduce score based on error count and severity
            error_count = sum(len(issue['errors']) for issue in file_issues)
            result["validation_score"] -= min(50, error_count * 2)
    
    result["validation_score"] = max(0, result["validation_score"])
    
    # Populate runtime issues for LLM
    result["runtime_issues"]["total_issues"] = len(result["detailed_issues"])
    result["runtime_issues"]["issue_patterns"] = issue_patterns
    result["runtime_issues"]["issue_counts"] = {
        "total_invalid_records": result["invalid_records"],
        "total_valid_records": result["valid_records"]
    }
    
    if result["invalid_records"] > 0:
        result["status"] = "validation_issues"
    
    return result


def task_validate_data(**context):
    """
    Airflow task wrapper for validation
    """
    print("=" * 60)
    print("TASK 2: Validating Excel Data")
    print("=" * 60)
    
    # Pull data from previous task
    task_instance = context['task_instance']
    read_data_result = task_instance.xcom_pull(
        task_ids='read_excel_files',
        key='read_data_result'
    )
    
    if not read_data_result or not read_data_result['files']:
        print("No data to validate!")
        result = {
            "status": "error",
            "total_records": 0,
            "valid_records": 0,
            "invalid_records": 0,
            "issues": ["No files found to validate"],
            "files_with_errors": {},
            "validation_score": 0
        }
        task_instance.xcom_push(key='validation_result', value=result)
        return result
    
    # Validate the data
    validation_result = validate_excel_data(read_data_result['files'])
    
    print(f"\nValidation Summary:")
    print(f"  Status: {validation_result['status']}")
    print(f"  Total Records: {validation_result['total_records']}")
    print(f"  Valid: {validation_result['valid_records']}")
    print(f"  Invalid: {validation_result['invalid_records']}")
    print(f"  Score: {validation_result['validation_score']}/100")
    
    if validation_result['issues']:
        print(f"\nIssues Found ({len(validation_result['issues'])} total):")
        for issue in validation_result['issues'][:10]:
            print(f"  ⚠ {issue}")
        
        if len(validation_result['issues']) > 10:
            print(f"  ... and {len(validation_result['issues']) - 10} more issues")
    
    if not validation_result['issues']:
        print("\n✓ All data passed validation!")
    
    # Push to XCom
    task_instance.xcom_push(key='validation_result', value=validation_result)
    
    return validation_result

