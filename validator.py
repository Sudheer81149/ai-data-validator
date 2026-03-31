from datetime import datetime

def basic_validation(data):
    errors = []

    # Date validation
    try:
        datetime.strptime(data.get("claim_date", ""), "%Y-%m-%d")
    except:
        errors.append("Invalid date format (expected YYYY-MM-DD)")

    # Amount validation
    if data.get("claim_amount", 0) < 0:
        errors.append("Claim amount cannot be negative")

    # Email validation
    if "@" not in data.get("email", ""):
        errors.append("Invalid email format")

    return errors