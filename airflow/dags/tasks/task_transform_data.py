"""
Task 3: Transform Data
Handles data transformation according to rules:
- Combines first_name and last_name into full_name
- Converts dob from yyyy/mm/dd to dd/mm/yyyy
- Skips null/empty values
- Outputs to JSON files in output folder
"""
import pandas as pd
from datetime import datetime
import json
from typing import Dict, List, Any
import os
from pathlib import Path


def parse_date_to_ddmmyyyy(date_value: Any) -> str:
    """
    Convert DOB from yyyy/mm/dd to dd/mm/yyyy format
    
    Args:
        date_value: Date in yyyy/mm/dd format
        
    Returns:
        Date string in dd/mm/yyyy format or None if invalid
    """
    if pd.isna(date_value) or date_value is None or date_value == "":
        return None
    
    date_str = str(date_value).strip()
    
    # Try to parse as yyyy/mm/dd
    try:
        parsed_date = datetime.strptime(date_str, '%Y/%m/%d')
        return parsed_date.strftime('%d/%m/%Y')
    except ValueError:
        # Try other common formats
        formats = [
            '%Y-%m-%d',      # 2001-01-29
            '%d/%m/%Y',      # 29/01/2001
            '%m/%d/%Y',      # 01/29/2001
            '%Y/%m/%d',      # 2001/01/29
            '%d-%m-%Y',      # 29-01-2001
        ]
        
        for fmt in formats:
            try:
                parsed_date = datetime.strptime(date_str, fmt)
                return parsed_date.strftime('%d/%m/%Y')
            except ValueError:
                continue
    
    return None


def create_full_name(record: dict) -> str:
    """
    Create full_name by combining first_name and last_name
    Returns None if both are null/empty (will be skipped)
    
    Args:
        record: Single data record
        
    Returns:
        Full name string or None
    """
    first = str(record.get('first_name', '')).strip() if record.get('first_name') and pd.notna(record.get('first_name')) else ''
    last = str(record.get('last_name', '')).strip() if record.get('last_name') and pd.notna(record.get('last_name')) else ''
    
    # Prefer existing 'name' field if present
    if 'name' in record and pd.notna(record.get('name')):
        existing_name = str(record.get('name')).strip()
        if existing_name:
            return existing_name
    
    # Create from first_name + last_name
    if first and last:
        return f"{first} {last}"
    elif first:
        return first
    elif last:
        return last
    else:
        return None  # Both are null/empty - will skip this record


def transform_record(record: dict) -> tuple:
    """
    Transform a single record:
    - Create full_name from first_name + last_name
    - Convert dob to dd/mm/yyyy
    - Skip null/empty critical fields
    
    Args:
        record: Single data record
        
    Returns:
        Tuple of (transformed_record, should_keep)
    """
    transformed = record.copy()
    
    # TRANSFORMATION 1: Create full_name
    full_name = create_full_name(record)
    transformed['full_name'] = full_name
    
    # TRANSFORMATION 2: Convert DOB format
    if 'dob' in transformed and pd.notna(transformed['dob']):
        formatted_dob = parse_date_to_ddmmyyyy(transformed['dob'])
        transformed['dob'] = formatted_dob
    
    # TRANSFORMATION 3: Check if record should be kept (has full_name and valid dob)
    should_keep = True
    
    # Skip if full_name is null (cannot create valid name)
    if not full_name:
        should_keep = False
    
    # Skip if dob is null (cannot convert date)
    if should_keep and 'dob' in transformed and pd.isna(transformed.get('dob')):
        should_keep = False
    
    return transformed, should_keep


def transform_data(data_files: list, output_folder: str = "/opt/airflow/data_folder/output") -> dict:
    """
    Transform all data files according to rules and save to JSON files
    
    Args:
        data_files: List of data file dictionaries
        output_folder: Path to folder where transformed JSON files will be saved
        
    Returns:
        Dictionary with transformed data (JSON format) and output file paths
    """
    
    # Create output folder if it doesn't exist
    os.makedirs(output_folder, exist_ok=True)
    
    result = {
        "status": "success",
        "timestamp": datetime.now().isoformat(),
        "files_processed": 0,
        "total_records_input": 0,
        "total_records_output": 0,
        "records_skipped": 0,
        "files": [],
        "output_files": [],
        "transformation_summary": {},
        "output_folder": output_folder
    }
    
    if not data_files:
        result["status"] = "warning"
        return result
    
    for file_data in data_files:
        filename = file_data.get("filename", "unknown")
        data_records = file_data.get("data", [])
        
        result["total_records_input"] += len(data_records)
        
        transformed_records = []
        skipped_count = 0
        
        # Transform each record
        for record in data_records:
            transformed_record, should_keep = transform_record(record)
            
            if should_keep:
                transformed_records.append(transformed_record)
                result["total_records_output"] += 1
            else:
                skipped_count += 1
                result["records_skipped"] += 1
        
        # Generate output filename (remove extension, add _cleaned.json)
        base_name = Path(filename).stem
        output_filename = f"{base_name}_cleaned.json"
        output_filepath = os.path.join(output_folder, output_filename)
        
        # Save transformed data to JSON file
        try:
            with open(output_filepath, 'w', encoding='utf-8') as f:
                json.dump(transformed_records, f, indent=2, ensure_ascii=False)
            
            file_status = "success"
            file_message = f"Saved {len(transformed_records)} records"
        except Exception as e:
            file_status = "error"
            file_message = f"Failed to save JSON: {str(e)}"
        
        # Store file info
        file_info = {
            "filename": filename,
            "path": file_data.get("path", ""),
            "rows_input": len(data_records),
            "rows_output": len(transformed_records),
            "rows_skipped": skipped_count,
            "data": transformed_records,  # Transformed data in memory
            "output_file": output_filepath,
            "output_filename": output_filename,
            "output_status": file_status,
            "output_message": file_message
        }
        
        result["files"].append(file_info)
        result["output_files"].append({
            "input_file": filename,
            "output_file": output_filepath,
            "output_filename": output_filename,
            "records": len(transformed_records),
            "status": file_status
        })
        result["files_processed"] += 1
        
        # Summary by file
        result["transformation_summary"][filename] = {
            "input_records": len(data_records),
            "output_records": len(transformed_records),
            "skipped_records": skipped_count,
            "output_file": output_filepath,
            "transformations_applied": [
                "first_name + last_name → full_name",
                "dob: yyyy/mm/dd → dd/mm/yyyy",
                "skip records with null full_name or invalid dob"
            ]
        }
    
    return result


def task_transform_data(**context):
    """
    Airflow task: Transform data according to rules and save as JSON files
    """
    print("=" * 60)
    print("TASK 4: Transforming Data (JSON processing)")
    print("=" * 60)
    
    # Pull validated data
    task_instance = context['task_instance']
    read_data_result = task_instance.xcom_pull(
        task_ids='read_excel_files',
        key='read_data_result'
    )
    
    if not read_data_result or not read_data_result.get('files'):
        print("No data to transform!")
        result = {
            "status": "error",
            "files_processed": 0,
            "total_records_input": 0,
            "total_records_output": 0,
            "files": [],
            "output_files": []
        }
        task_instance.xcom_push(key='transformed_data', value=result)
        return result
    
    # Transform the data with output to folder
    output_folder = "/opt/airflow/data_folder/output"
    transform_result = transform_data(read_data_result['files'], output_folder)
    
    print(f"\nTransformation Summary:")
    print(f"  Total Input Records: {transform_result['total_records_input']}")
    print(f"  Total Output Records: {transform_result['total_records_output']}")
    print(f"  Records Skipped: {transform_result['records_skipped']}")
    print(f"  Files Processed: {transform_result['files_processed']}")
    
    print(f"\nTransformations Applied:")
    print(f"  ✓ first_name + last_name → full_name")
    print(f"  ✓ dob: yyyy/mm/dd → dd/mm/yyyy")
    print(f"  ✓ Skip null/invalid records")
    
    print(f"\n" + "="*60)
    print("OUTPUT FILES SAVED")
    print("="*60)
    print(f"\nOutput Folder: {output_folder}\n")
    
    for output_file_info in transform_result['output_files']:
        status_icon = "✓" if output_file_info['status'] == "success" else "✗"
        print(f"{status_icon} {output_file_info['output_filename']}")
        print(f"   Input: {output_file_info['input_file']}")
        print(f"   Records: {output_file_info['records']}")
        print(f"   Path: {output_file_info['output_file']}\n")
    
    print(f"Output Format: JSON (human-readable)")
    print(f"Status: {transform_result['status']}")
    print("="*60 + "\n")
    
    # Push transformed data to XCom
    task_instance.xcom_push(key='transformed_data', value=transform_result)
    
    return transform_result
