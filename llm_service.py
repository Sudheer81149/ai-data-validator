from openai import OpenAI
import os
import json

client = OpenAI(
    api_key=os.getenv("OPENAI_API_KEY"),
    base_url="https://openrouter.ai/api/v1"   # important for your model
)

def analyze_json(json_data):

    prompt = f"""
Return output strictly in JSON format:

{{
  "errors": [],
  "suggestions": [],
  "corrected_json": {{}},
  "status": "valid/invalid"
}}

Analyze this JSON:
{json.dumps(json_data)}
"""

    response = client.chat.completions.create(
        model="nvidia/nemotron-3-super-120b-a12b:free",
        messages=[{"role": "user", "content": prompt}],
        extra_body={"reasoning": {"enabled": True}}
    )

    return response.choices[0].message.content