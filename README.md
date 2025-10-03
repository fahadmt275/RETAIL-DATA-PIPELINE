# Cloud Composer CI/CD with Cloud Build & GitHub

This repository has a CI/CD workflow for GCP using Cloud Build and GitHub.

---

##  Project Structure
```
.
├── dags/                     # Airflow DAGs
│   ├── bq_dag.py             # BigQuery workflows
│   ├── pyspark_dag.py        # PySpark workflows
│   ├── main_dag.py        # parent workflow
│
├── data/                     # SQL scripts & ingestion
│   ├── BQ/
│   │   ├── raw.sql
│   │   ├── intermediate.sql
│   │   ├── main.sql
│   ├── DBs/
│   │   ├── retailerdb.sql
│   ├── INGESTION/
│       ├── retailerMysqlToLanding.py
│
├── utils/                    # Deployment helpers
│   ├── add_dags_to_composer.py
│   ├── requirements.txt
│
├── cloudbuild.yaml           # Cloud Build pipeline config
└── README.md                 # Documentation
```

---

## CI/CD Workflow

1. **Develop & Push**  
   - Create/modify DAGs or ingestion scripts  
   - Push changes to a branch  

2. **Pull Request**  
   - Open PR → main branch  

3. **Validation (Cloud Build)**  
   - DAG syntax & integrity checks run automatically  

4. **Approval & Merge**  
   - Reviewed, approved, and merged into `main`  

5. **Deploy to Composer**  
   - Cloud Build syncs DAGs + dependencies with Cloud Composer  

6. **Verify Execution**  
   - Confirm DAGs run as expected in Composer  

---

 This setup enables automated testing, deployment, and synchronization of DAGs and dependencies, ensuring workflow management in Cloud Composer.
