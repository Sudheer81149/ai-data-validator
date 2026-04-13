"""
Task 1: Read Excel and JSON Files from data folder
Enhanced to support both Excel and JSON data sources
"""
import pandas as pd
import os
import json
from pathlib import Path
from datetime import datetime


def read_json_data(json_file: Path) -> dict:
    """
    Read JSON data file
    
    Args:
        json_file: Path to JSON file
        
    Returns:
        Dictionary with file info and data
    """
    with open(json_file, 'r') as f:
        data = json.load(f)
    
    # Handle both array and object formats
    if isinstance(data, dict):
        records = [data]
    elif isinstance(data, list):
        records = data
    else:
        raise ValueError(f"JSON must be array or object, got {type(data)}")
    
    # Infer columns from records
    columns = set()
    for record in records:
        if isinstance(record, dict):
            columns.update(record.keys())
    
    return {
        "filename": json_file.name,
        "path": str(json_file),
        "rows": len(records),
        "columns": list(columns),
        "data": records,
        "size_bytes": os.path.getsize(json_file),
        "file_type": "json"
    }


def read_excel_data(data_folder: str = "/opt/airflow/data_folder") -> dict:
    """
    Read all Excel and JSON files from the specified folder
    
    Args:
        data_folder: Path to folder containing Excel/JSON files
        
    Returns:
        Dictionary containing all parsed data with metadata
    """
    result = {
        "status": "success",
        "timestamp": datetime.now().isoformat(),
        "files_processed": 0,
        "total_records": 0,
        "files": [],
        "errors": []
    }
    
    # Create folder if it doesn't exist
    if not os.path.exists(data_folder):
        os.makedirs(data_folder, exist_ok=True)
        result["errors"].append(f"Folder created: {data_folder}. Please add Excel or JSON files.")
        result["status"] = "warning"
        return result
    
    # Find all Excel and JSON files
    excel_files = list(Path(data_folder).glob("*.xlsx")) + list(Path(data_folder).glob("*.xls"))
    json_files = list(Path(data_folder).glob("*.json"))
    
    all_files = excel_files + json_files
    
    if not all_files:
        result["errors"].append(f"No Excel (.xlsx, .xls) or JSON files found in {data_folder}")
        result["status"] = "warning"
        return result
    
    # Read each Excel file
    for excel_file in excel_files:
        try:
            # Try to read first sheet
            df = pd.read_excel(excel_file)
            
            file_info = {
                "filename": excel_file.name,
                "path": str(excel_file),
                "rows": len(df),
                "columns": list(df.columns),
                "data": df.to_dict('records'),
                "size_bytes": os.path.getsize(excel_file),
                "file_type": "excel"
            }
            
            result["files"].append(file_info)
            result["files_processed"] += 1
            result["total_records"] += len(df)
            
        except Exception as e:
            result["errors"].append(f"Error reading {excel_file.name}: {str(e)}")
            result["status"] = "partial_success"
    
    # Read each JSON file
    for json_file in json_files:
        try:
            file_info = read_json_data(json_file)
            result["files"].append(file_info)
            result["files_processed"] += 1
            result["total_records"] += file_info["rows"]
            
        except Exception as e:
            result["errors"].append(f"Error reading {json_file.name}: {str(e)}")
            result["status"] = "partial_success"
    
    return result


def task_read_data(**context):
    """
    Airflow task wrapper for reading Excel and JSON data
    """
    print("=" * 60)
    print("TASK 1: Reading Excel and JSON Files")
    print("=" * 60)
    
    data = read_excel_data()
    
    print(f"\nStatus: {data['status']}")
    print(f"Files processed: {data['files_processed']}")
    print(f"Total records: {data['total_records']}")
    
    if data["files"]:
        print(f"\nFiles loaded:")
        for file_info in data["files"]:
            file_type = file_info.get('file_type', 'unknown').upper()
            print(f"  - {file_info['filename']} ({file_type}): {file_info['rows']} rows, {len(file_info['columns'])} columns")
            print(f"    Columns: {', '.join(file_info['columns'][:5])}")
    
    if data["errors"]:
        print(f"\nWarnings/Errors:")
        for error in data["errors"]:
            print(f"  ⚠ {error}")
    
    # Push to XCom for next task
    task_instance = context['task_instance']
    task_instance.xcom_push(key='read_data_result', value=data)
    
    return data
