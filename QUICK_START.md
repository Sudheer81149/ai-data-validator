# ⚡ QUICK START - PySpark ETL + LLM Demo

## 🎯 In 3 Steps:

### Step 1: Rebuild & Start (30 seconds)
```bash
cd c:\Users\sudhe\Desktop\AI_Project_demo
docker-compose down
docker-compose up -d --build
```
Wait ~40 seconds for Airflow to be ready.

### Step 2: Trigger DAG (1 second)
1. Open: http://localhost:8080
2. Login: airflow / airflow
3. Search: `pyspark_etl_with_llm_analysis`
4. Click Play (Trigger DAG)
5. Confirm

### Step 3: View Results (30-60 seconds)
1. Watch 3 tasks execute
2. Click `generate_final_report` task
3. Scroll logs to see:
   - ETL statistics
   - LLM intelligence analysis
   - Data quality score
   - Recommended fixes

**Total time: ~2 minutes** ✓

---

## 📊 What You'll See

### ETL Processing
```
✓ Read 10 records from JSON
✓ Validated each field
✓ Found 6 quality issues
✓ Stored 7 valid records as Parquet
```

### LLM Analysis
```
Summary: 6 data quality issues detected
Priority: HIGH (70% quality score)

Root Causes:
- NULL values in required fields (3 instances)
- Invalid date formats (1 instance)
- Negative amounts (1 instance)

Recommended Fixes:
1. Add email validation at source
2. Validate name field not null
3. Implement date format check
4. Add business rule for amounts
```

### Final Report
```
ETL Execution: SUCCESS
Input: 10 records
Output: 7 valid records → Parquet
Issues: 6 found → LLM analyzed
Quality Score: 70%
Next Step: Apply recommended fixes
```

---

## 📚 Reference Files

| File | Purpose |
|------|---------|
| `pyspark_etl_dag.py` | Main demo DAG |
| `task_pyspark_etl.py` | PySpark ETL engine |
| `llm_service.py` | LLM analysis service |
| `sample_data.json` | Test data (with issues) |
| `DEMO_GUIDE.md` | Full demo guide |
| `SETUP_SUMMARY.md` | Complete setup details |

---

## 🎨 Key Points

✅ **PySpark-based** - Industry-standard ETL  
✅ **4-Stage Pipeline** - Read → Validate → Filter → Store  
✅ **Smart Detection** - Finds nulls, invalid dates, anomalies  
✅ **LLM Analysis** - Root causes + recommendations  
✅ **Production-Ready** - Parquet output format  
✅ **Demo-Perfect** - 30-60 second execution  

---

## ❓ Troubleshooting

**Docker won't start?**
```bash
docker system prune -f
docker-compose up -d --build
```

**Containers fail to build?**
```bash
docker-compose logs
# Check for PySpark installation errors
```

**DAG not showing?**
- Wait 30 seconds after containers start
- Refresh Airflow UI (F5)
- Check Airflow logs: `docker-compose logs airflow-webserver`

**Tasks failing?**
- Click task → View logs
- Check sample_data.json exists in data_folder/
- Verify PySpark installed: Docker rebuild required

---

## 💬 Demo Script

**Opening:**
> "Today I'm showing how LLM helps detect and analyze ETL issues. Instead of reviewing thousands of error messages, AI understands your data patterns and gives actionable insights."

**During Demo:**
> *Trigger DAG*
> "Watch as the ETL runs in realtime - reading JSON, validating each record, detecting problems, and storing valid data as Parquet."

**Analysis Phase:**
> "Now LLM analyzes all the issues found - identifies root causes, prioritizes them, and suggests specific fixes."

**Results:**
> "In 45 seconds, we got what would take a data engineer 2 hours. That's the power of AI-assisted data processing."

---

## 📋 Checklist for Demo Day

- [ ] Docker containers running
- [ ] Airflow UI accessible (http://localhost:8080)
- [ ] sample_data.json in data_folder/
- [ ] PySpark installed (v3.3.0+)
- [ ] DAG visible: `pyspark_etl_with_llm_analysis`
- [ ] LLM API key configured (or mock mode ready)
- [ ] Presentation materials ready
- [ ] Client environment/laptop connected

---

**Ready? Trigger the DAG and amaze your client! 🚀**
