import json
from validator import basic_validation
from llm_service import analyze_json

def run_validation(json_data):

    print("🔍 Running Rule-Based Validation...")
    rule_errors = basic_validation(json_data)

    print("🤖 Running LLM Validation...")
    llm_output = analyze_json(json_data)

    return {
        "rule_errors": rule_errors,
        "llm_output": llm_output
    }


if __name__ == "__main__":

    with open("test_data.json") as f:
        data = json.load(f)

    result = run_validation(data)

    print("\n✅ FINAL RESULT:\n")
    print(result)