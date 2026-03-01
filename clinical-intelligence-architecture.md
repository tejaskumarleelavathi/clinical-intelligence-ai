# Clinical Intelligence Platform — Complete Architecture Guide v3.0
### Synthetic Patient Data · Data Engineering · GenAI · Full-Stack · Cloud

> **How to use this doc:** Read the entire thing once before writing a single line of code. Understand the big picture first. Then go phase by phase. Every section explains *what* a technology is, *why* it exists in this project specifically, and *what your concrete goal is* before you build it.

---

## What Are You Building?

A **two-layer platform** combining a full data engineering pipeline with a multi-agent AI system, built on a synthetic patient dataset with 12 naturally related tables.

**Layer 1 — Data Engineering Pipeline:**
12 SQLite tables are exported to CSV, uploaded to Azure Data Lake Storage, processed through a PySpark ETL pipeline on Databricks using the Medallion Architecture (Bronze → Silver → Gold), and loaded into a unified Snowflake data warehouse.

**Layer 2 — Clinical GenAI Application:**
A React chat interface where a clinician or analyst asks a natural language question. Four AI agents coordinate automatically: one queries Snowflake with generated SQL, one searches the web for real-time medical context, one synthesizes both into a clinical insight, and one scores the quality. All powered by Azure OpenAI.

### The Interview Story You Tell

> *"I built an end-to-end clinical intelligence platform. On the data engineering side, I exported 12 related healthcare tables, built a PySpark ETL pipeline on Databricks using Medallion Architecture, and loaded a unified patient analytics schema into Snowflake. On the AI side, I built a four-agent system where analysts ask natural language questions and get grounded insights — with a live Web Search Agent that fetches real-time medical context, such as what a medication is used for, its side effects, and clinical guidelines."*

That story covers Data Engineering, Big Data, GenAI, Agentic AI, Cloud, DevOps, and Full-Stack — all in one project. And nobody else in your interview pool has it.

---

## Capability Overview

| Capability | Technology | What It Proves |
|---|---|---|
| Data Engineering / ETL | PySpark + Databricks + ADLS | Pipeline design at scale |
| Data Warehousing | Snowflake | Cloud DW + SQL analytics |
| Agentic AI | LangGraph + Azure OpenAI | Multi-agent orchestration |
| Live Web Context | Web Search Agent | Real-time data augmentation |
| Full Stack | FastAPI + React + TypeScript | End-to-end engineering |
| Cloud Infrastructure | Azure + Terraform + AKS | Production deployment |
| CI/CD | GitHub Actions | Automated test/build/deploy |

---

## The Dataset — Synthetic Patient Data

Your SQLite database contains 12 tables with real relational structure connected via `patient_id` foreign keys. This is a proper star schema — no artificial joins needed.

### The 12 Tables

| Table | Key Columns | Role in Platform |
|---|---|---|
| `patients` | patient_id, patient_name, age, gender, dob | Core dimension — center of the star schema |
| `medications` | patient_id, medication_name, dosage, frequency | What drugs each patient is on — key for drug interaction analysis |
| `lab_results` | patient_id, test_name, result_value, normal_range, test_date | Quantitative health metrics — abnormal results trigger insights |
| `allergies` | patient_id, allergen, severity, reaction_type | Safety-critical — checked against medications for cross-reactions |
| `immunizations` | patient_id, vaccine_name, date_administered | Preventive care history |
| `care_providers` | patient_id, provider_name, specialty, contact | Which doctor manages each patient |
| `progress_notes` | patient_id, note_date, note_text, author | Unstructured clinical text |
| `recent_visits` | patient_id, visit_date, visit_reason, diagnosis | Visit history and diagnoses |
| `medical_history` | patient_id, medical_history_type_id, onset_date | Chronic conditions and past diagnoses |
| `medical_history_type` | medical_history_type_id, condition_name | Lookup table for condition names |
| `diagnostic_imaging` | patient_id, imaging_type, result, image_date | X-rays, MRIs, CT scan results |
| `sqlite_sequence` | name, seq | SQLite internal — skip in ETL |

### How the Tables Relate

```
patients (patient_id PK)
    ├── medications          (patient_id FK)
    ├── lab_results          (patient_id FK)
    ├── allergies            (patient_id FK)
    ├── care_providers       (patient_id FK)
    ├── progress_notes       (patient_id FK)
    ├── recent_visits        (patient_id FK)
    ├── immunizations        (patient_id FK)
    ├── diagnostic_imaging   (patient_id FK)
    └── medical_history      (patient_id FK)
              └── medical_history_type (medical_history_type_id FK)
```

Every clinical table connects back to `patients` via `patient_id`. These are real foreign key relationships — no artificial mapping needed. This is what makes the data engineering genuinely impressive.

---

## Full System Architecture

```
════════════════════════════════════════════════════════════════════
                   LAYER 1: DATA ENGINEERING
════════════════════════════════════════════════════════════════════

  SQLite Database (12 tables)
  patients, medications, lab_results, allergies,
  immunizations, care_providers, progress_notes,
  recent_visits, medical_history, diagnostic_imaging
           │
           │  00_export_sqlite.py (Python script)
           ▼
  ┌──────────────────────────────────────────────────┐
  │        AZURE DATA LAKE STORAGE Gen2              │
  │        BRONZE LAYER (/raw/)                      │
  │                                                  │
  │  /raw/patients/patients.csv                      │
  │  /raw/medications/medications.csv                │
  │  /raw/lab_results/lab_results.csv                │
  │  /raw/allergies/allergies.csv                    │
  │  /raw/immunizations/immunizations.csv            │
  │  /raw/care_providers/care_providers.csv          │
  │  /raw/progress_notes/progress_notes.csv          │
  │  /raw/recent_visits/recent_visits.csv            │
  │  /raw/medical_history/medical_history.csv        │
  │  /raw/medical_history_type/...csv                │
  │  /raw/diagnostic_imaging/...csv                  │
  └──────────────────────┬───────────────────────────┘
                         │
                         ▼
  ┌──────────────────────────────────────────────────┐
  │         DATABRICKS (Apache Spark)                │
  │                                                  │
  │  Notebook 1: 01_ingest.py                        │
  │    → Read all 11 CSVs from ADLS                  │
  │    → Validate schema (check expected columns)    │
  │    → Drop duplicate rows                         │
  │    → Cast types (dates, integers, floats)        │
  │    → Log null counts per column                  │
  │    → Write to /silver/ as Parquet                │
  │                                                  │
  │  Notebook 2: 02_transform.py                     │
  │    → Standardize all column names to snake_case  │
  │    → Add derived columns (age_group, visit_year) │
  │    → Normalize medication names to uppercase     │
  │    → Handle NULLs in result_value columns        │
  │    → Write clean tables to /gold/ as Parquet     │
  │                                                  │
  │  Notebook 3: 03_load.py                          │
  │    → Read gold Parquet for all 11 tables         │
  │    → Connect to Snowflake via Spark connector    │
  │    → Run MERGE (upsert) into each Snowflake table│
  │    → Log row counts loaded per table             │
  │                                                  │
  │  Databricks Workflow: 01 → 02 → 03 in sequence  │
  └──────────────────────┬───────────────────────────┘
                         │
                         ▼
  ┌──────────────────────────────────────────────────┐
  │              SNOWFLAKE                           │
  │         DATABASE: CLINICAL_DB                    │
  │         SCHEMA: PUBLIC                           │
  │                                                  │
  │  patients           medications                  │
  │  lab_results        allergies                    │
  │  immunizations      care_providers               │
  │  progress_notes     recent_visits                │
  │  medical_history    medical_history_type         │
  │  diagnostic_imaging                              │
  │                                                  │
  │  All joined on: patient_id                       │
  └──────────────────────┬───────────────────────────┘
                         │
════════════════════════════════════════════════════════════════════
                   LAYER 2: GENAI APPLICATION
════════════════════════════════════════════════════════════════════
                         │
                         ▼
  ┌──────────────────────────────────────────────────┐
  │         FASTAPI BACKEND (Python)                 │
  │                                                  │
  │  routes.py ──▶ orchestrator.py                   │
  │                     │                            │
  │         ┌───────────┴───────────┐                │
  │         ▼                       ▼                │
  │    SQL Agent            Web Search Agent         │
  │  (queries Snowflake)   (searches web for         │
  │                         medical context)         │
  │         │                       │                │
  │         └───────────┬───────────┘                │
  │                     ▼                            │
  │              Insight Agent                       │
  │         (synthesizes SQL data +                  │
  │          web context into clinical               │
  │          recommendation)                         │
  │                     │                            │
  │                     ▼                            │
  │              Judge Agent                         │
  │         (scores quality 1-10,                    │
  │          flags safety issues)                    │
  │                     │                            │
  │                     ▼                            │
  │           JSON Response                          │
  └──────────┬──────────────────────────────────────┘
             │
             ▼
  ┌─────────────────────────────────────────────────┐
  │         React + TypeScript Frontend             │
  │              (Chat UI)                          │
  │                                                 │
  │  ⚠️ FOR DEMONSTRATION ONLY — NOT FOR CLINICAL USE│
  │                                                 │
  │  [Ask a question about patients...]    [Ask]    │
  │                                                 │
  │  ┌─────────────────────────────────────────┐   │
  │  │ AI Insight:                             │   │
  │  │ "Patient 47 (68yo) shows glucose of     │   │
  │  │  187 mg/dL — above normal range..."     │   │
  │  └─────────────────────────────────────────┘   │
  │                                                 │
  │  Agents: [SQL] [WebSearch] [Insight] [Judge]    │
  │  Score: 9/10  Safety: ✅ Clear                  │
  │  SQL: [view query ▼]                            │
  │  Web Sources: [PubMed] [Mayo Clinic]            │
  └─────────────────────────────────────────────────┘

════════════════════════════════════════════════════════════════════
                   DEPLOYMENT LAYER
════════════════════════════════════════════════════════════════════

  Docker images → Azure Container Registry → AKS (Kubernetes)
  Terraform provisions: ADLS, ACR, AKS, Resource Group
  GitHub Actions CI/CD: test → build → push → deploy on every push
```

---

## Complete Folder Structure

```
clinical-intelligence-ai/
│
├── data_engineering/                    ← EVERYTHING for the ETL pipeline
│   ├── notebooks/
│   │   ├── 00_export_sqlite.py          ← Export SQLite tables to CSVs
│   │   ├── 01_ingest.py                 ← Read CSVs, validate, write silver Parquet
│   │   ├── 02_transform.py              ← Clean, standardize, write gold Parquet
│   │   └── 03_load.py                   ← Load gold Parquet into Snowflake
│   ├── schemas/                         ← Expected schema JSON per table (11 files)
│   │   ├── patients_schema.json
│   │   ├── medications_schema.json
│   │   ├── lab_results_schema.json
│   │   └── ... (one per table)
│   ├── tests/
│   │   ├── test_ingest.py
│   │   ├── test_transform.py
│   │   └── test_load.py
│   └── requirements_spark.txt
│
├── backend/                             ← FastAPI + AI Agents
│   ├── app/
│   │   ├── main.py                      ← FastAPI entry point, CORS setup
│   │   ├── api/
│   │   │   └── routes.py                ← POST /api/query, GET /api/health
│   │   ├── agents/
│   │   │   ├── sql_agent.py             ← NL → SQL → Snowflake → structured data
│   │   │   ├── web_search_agent.py      ← Question → web search → medical context
│   │   │   ├── insight_agent.py         ← SQL data + web context → GPT-4o → insight
│   │   │   └── judge_agent.py           ← Answer → GPT-4o evaluation → scores + safety flag
│   │   ├── services/
│   │   │   ├── data_service.py          ← All Snowflake connection and query logic
│   │   │   └── orchestrator.py          ← Coordinates the 4-agent pipeline
│   │   └── models/
│   │       └── schemas.py               ← Pydantic request/response models
│   ├── tests/
│   │   ├── test_data_service.py
│   │   ├── test_sql_agent.py
│   │   ├── test_web_search_agent.py
│   │   └── test_orchestrator.py
│   ├── requirements.txt
│   ├── .env                             ← NEVER COMMIT — all secrets live here
│   └── Dockerfile
│
├── frontend/                            ← React TypeScript chat UI
│   ├── src/
│   │   ├── App.tsx                      ← Root component with disclaimer banner
│   │   ├── components/
│   │   │   ├── QuestionInput.tsx        ← Text input + submit button
│   │   │   ├── AnswerCard.tsx           ← Displays the clinical insight
│   │   │   ├── AgentBadges.tsx          ← Shows which agents fired
│   │   │   ├── ScoreDisplay.tsx         ← Judge scores with color coding
│   │   │   ├── SqlPreview.tsx           ← Collapsible SQL query panel
│   │   │   └── WebSources.tsx           ← Lists web sources the agent used
│   │   └── api/
│   │       └── client.ts                ← Axios calls to FastAPI
│   ├── package.json
│   └── Dockerfile
│
├── infra/                               ← Terraform Infrastructure as Code
│   ├── main.tf                          ← Resource Group, ADLS, ACR, AKS
│   ├── variables.tf
│   └── outputs.tf
│
├── k8s/                                 ← Kubernetes deployment manifests
│   ├── backend-deployment.yaml          ← FastAPI: 2 replicas
│   ├── frontend-deployment.yaml         ← React/Nginx: 2 replicas
│   ├── service.yaml
│   └── ingress.yaml
│
├── docs/
│   ├── architecture.md                  ← This document
│   ├── data_dictionary.md               ← Every table and column defined
│   └── adr/
│       ├── 001-why-snowflake.md
│       ├── 002-why-web-search-not-rag.md
│       └── 003-why-databricks.md
│
├── .github/
│   └── workflows/
│       └── ci-cd.yml                    ← GitHub Actions CI/CD pipeline
├── docker-compose.yml
└── README.md
```

---

## Technology Glossary

### Apache Spark (via Databricks)
Spark is a distributed computing engine that processes large datasets in parallel. You write Python (PySpark) that looks like Pandas but runs at scale. In this project, Spark handles all three ETL stages: reading CSVs from ADLS, transforming and cleaning 11 tables, and loading to Snowflake.

**Key concepts you will use:**
- `DataFrame` — a distributed table, like Pandas but runs across a cluster
- `SparkSession` — always called `spark`, the entry point to all Spark operations
- `.read.csv()` — reads a CSV from ADLS into a Spark DataFrame
- `.withColumn()` — adds or transforms a column
- `.join()` — joins two DataFrames on shared keys
- `.write.parquet()` — writes to Parquet format in ADLS
- **Lazy evaluation** — transformations don't execute until you call `.write()` or `.show()`

---

### Medallion Architecture (Bronze / Silver / Gold)
The industry-standard pattern for organizing a data lake. You never modify raw data.

| Layer | Folder | Contains | What Happens |
|---|---|---|---|
| Bronze (Raw) | `/raw/` | Original CSV files, unchanged | Upload once — never touched again |
| Silver (Cleaned) | `/silver/` | Parquet after validation | Nulls handled, duplicates dropped, types cast |
| Gold (Unified) | `/gold/` | Parquet ready for Snowflake | Standardized names, derived columns added |

Name-dropping "Medallion Architecture" in an interview immediately signals you understand production data engineering.

---

### Snowflake
A cloud-hosted SQL data warehouse. Unlike a transactional database (built for INSERT/UPDATE speed), Snowflake is optimized for analytical queries — GROUP BY, aggregations, multi-table JOINs. Your Spark job loads data here once. Your SQL Agent queries it live for every user question.

---

### Azure Data Lake Storage Gen2 (ADLS)
Cloud file storage optimized for big data workloads. Think of it as a scalable folder structure in Azure. Your raw CSVs land here first, and your Spark notebooks read from and write back to it at each medallion layer.

---

### FastAPI
A modern Python web framework. It auto-generates Swagger documentation at `/docs`, validates all requests and responses using Pydantic, and supports async Python natively. Your four agents are exposed through FastAPI HTTP endpoints.

---

### LangGraph
A Python library for building multi-agent AI systems. You define each agent as a node in a directed graph. LangGraph manages data flow between nodes, handles parallel execution (SQL Agent and Web Search Agent run at the same time), and supports retries.

---

### Azure OpenAI
Microsoft's hosted version of OpenAI models. You use `gpt-4.1-mini` (which you already deployed) for all text generation across the SQL, Insight, Judge, and Web Search agents.

---

### Web Search Agent (Why Not RAG)
RAG requires maintaining PDF documents that go out of date. Medical information changes constantly — drug guidelines update, new interactions are discovered, dosing recommendations change. The Web Search Agent solves this by fetching live medical context at the moment the question is asked. This is more impressive technically and always current.

---

### Pydantic
Python data validation library. You define request and response shapes as Python classes and Pydantic enforces them automatically. A wrong request shape returns a 422 error with a clear message. FastAPI uses Pydantic for all validation.

---

### Docker and Kubernetes (AKS)
Docker packages your app and all dependencies into a portable container. Kubernetes (AKS — Azure Kubernetes Service) manages those containers at scale: starts them, restarts on crash, scales under load, and routes traffic. Terraform provisions the AKS cluster.

---

### GitHub Actions CI/CD
Defined in `.github/workflows/ci-cd.yml`. Every push to `main` triggers three stages automatically: run Pytest tests → build Docker images and push to Azure Container Registry → deploy updated images to AKS.

---

## The Four AI Agents — Detailed

### Agent 1: SQL Agent
**Input:** User's natural language question
**What it does:**
1. Sends the question + your complete Snowflake schema (all 11 tables, all column names) to GPT-4o
2. GPT-4o returns a valid Snowflake SQL query
3. Runs the SQL against Snowflake via the DataService class
4. Returns result rows as a Python list of dicts

**Example:**
```
Question: "Patient 47 is on Metformin — what are their recent lab results?"

Generated SQL:
SELECT p.patient_name, p.age, m.medication_name, m.dosage,
       lr.test_name, lr.result_value, lr.normal_range, lr.test_date
FROM patients p
JOIN medications m ON p.patient_id = m.patient_id
JOIN lab_results lr ON p.patient_id = lr.patient_id
WHERE p.patient_id = 47
ORDER BY lr.test_date DESC

Returns: [{"patient_name": "John D.", "age": 68, "medication_name": "Metformin",
           "dosage": "500mg", "test_name": "Glucose", "result_value": 187,
           "normal_range": "70-100 mg/dL", "test_date": "2024-01-15"}, ...]
```

---

### Agent 2: Web Search Agent
**Input:** User's question (especially when it involves medications, conditions, or lab values)
**What it does:**
1. Identifies medical entities in the question (drug names, condition names, lab test names)
2. Formulates specific medical search queries
3. Calls the Azure OpenAI web search tool
4. Extracts and summarizes the most relevant clinical information
5. Returns structured medical context to the Orchestrator

**Example searches it runs:**
- `"Metformin 500mg standard dosage Type 2 Diabetes clinical guidelines"`
- `"glucose level 187 mg/dL clinical significance diabetes management"`
- `"penicillin allergy cross-reactivity cephalosporins risk"`
- `"Warfarin pre-surgical protocol blood thinner management"`

---

### Agent 3: Insight Agent
**Input:** Original question + SQL data results + web search context
**What it does:** Combines all three inputs into a structured prompt and sends to GPT-4o asking for a direct answer, key insight, and specific actionable recommendation.

**Example output:**
> *"Patient 47 (68yo) shows glucose of 187 mg/dL — significantly above the normal range of 70-100 mg/dL. They are currently on Metformin 500mg for Type 2 Diabetes. Current medical guidelines indicate this glucose level suggests suboptimal glycemic control. Metformin at 500mg is typically a starting dose, with standard titration up to 1000-2000mg based on tolerance. Given the patient's age and that their last visit was 3 weeks ago, this warrants prompt follow-up. Recommendation: Schedule follow-up within 2 weeks for medication review and HbA1c testing."*

---

### Agent 4: Judge Agent
**Input:** Original question + generated insight
**What it does:** Asks GPT-4o to evaluate the answer on four dimensions, returning structured JSON:

```json
{
  "overall_score": 9,
  "relevance": 9,
  "data_grounding": 10,
  "actionability": 8,
  "safety_flag": false,
  "feedback": "Well-grounded in patient data with current clinical context and specific recommendation."
}
```

**Score dimensions:**
- **Relevance (1-10)** — does the answer address what was actually asked?
- **Data Grounding (1-10)** — is the answer based on real Snowflake data, not hallucination?
- **Actionability (1-10)** — does the answer include a concrete recommendation?
- **Safety Flag (boolean)** — true if the answer touches medication safety, contraindications, or clinical risk

Scores below 6 trigger a warning in the UI. Safety flag `true` triggers a prominent caution banner.

---

### The Orchestrator
Coordinates all four agents. SQL Agent and Web Search Agent run in parallel (they don't depend on each other). Their results feed into the Insight Agent. The insight feeds into the Judge Agent.

```
User Question
      │
      ├────────────────────────┐
      ▼                        ▼
 SQL Agent              Web Search Agent
 (queries Snowflake)    (searches web)
      │                        │
      └────────────┬───────────┘
                   ▼
            Insight Agent
            (synthesizes both)
                   │
                   ▼
             Judge Agent
             (scores + safety)
                   │
                   ▼
        Final JSON Response
```

---

## End-to-End Data Flow

### ETL Pipeline Flow (runs once, then on a schedule)
1. Run `00_export_sqlite.py` locally to export all 11 SQLite tables to CSV files
2. Upload CSV files to ADLS `/raw/` folder (Bronze layer)
3. Trigger Databricks Notebook 1 (`01_ingest.py`) — reads CSVs, validates schema, writes silver Parquet
4. Databricks Notebook 2 (`02_transform.py`) — cleans, standardizes, writes gold Parquet
5. Databricks Notebook 3 (`03_load.py`) — reads gold, MERGEs into Snowflake
6. Snowflake now has 11 clean, queryable tables — the live source of truth

### Query Flow (every time a user asks a question)
1. User types question in React UI, presses Ask
2. React sends `POST /api/query` with `{ "question": "..." }` to FastAPI
3. FastAPI validates the request with Pydantic, passes to Orchestrator
4. Orchestrator calls SQL Agent and Web Search Agent **in parallel**
5. SQL Agent: generates SQL → queries Snowflake → returns patient data rows
6. Web Search Agent: identifies medical entities → searches web → returns clinical context
7. Orchestrator calls Insight Agent with question + SQL results + web context
8. Insight Agent synthesizes all inputs → returns recommendation paragraph
9. Orchestrator calls Judge Agent with question + recommendation
10. Judge Agent evaluates → returns JSON scores + safety flag
11. Orchestrator assembles final response: `{ answer, scores, sql_used, agents_invoked, web_sources }`
12. FastAPI returns JSON to React
13. React renders: answer card, agent badges, score display, SQL preview, web sources list

---

## Example Questions The Platform Answers

These are the questions you demo in interviews and your Loom video. Each requires multi-table JOINs plus live web search context.

### Patient Safety (Most Impressive)
- *"Patient 23 has a penicillin allergy — do any of their current medications have cross-reactivity risks?"* — joins allergies + medications, web searches drug interaction risks
- *"Which patients are on blood thinners and have a surgical visit scheduled?"* — joins medications + recent_visits, web searches pre-surgical blood thinner protocols
- *"Are there patients on medications that require regular monitoring but have no recent lab results?"* — joins medications + lab_results, web searches monitoring requirements

### Clinical Analysis
- *"Which patients over 65 have both diabetes in their history AND abnormal glucose in recent labs?"* — joins patients + medical_history + lab_results
- *"What is the most commonly prescribed medication in this dataset, and what is it used for?"* — aggregates medications table, web searches drug purpose
- *"Which care provider has the highest patient load this month?"* — joins care_providers + recent_visits
- *"Which patients have not had a visit in over 6 months?"* — queries recent_visits with date filter

### Medication Intelligence
- *"Patient 47 is on Metformin 500mg and has a glucose of 187 — what should we know?"* — joins all relevant tables, web searches Metformin guidelines
- *"Which patients are on multiple medications that may interact with each other?"* — queries medications, web searches interaction risks
- *"What medications in this dataset are commonly associated with kidney function monitoring?"* — web searches drug monitoring requirements, joins with lab_results

### Population Health
- *"What is the average age of patients diagnosed with hypertension?"* — joins patients + medical_history + medical_history_type
- *"Which diagnostic imaging types are most commonly ordered?"* — aggregates diagnostic_imaging, web searches clinical context
- *"What percentage of patients have received their flu vaccine?"* — queries immunizations

---

## Build Phases — Complete Roadmap

Work through these in strict order. Do not skip ahead.

---

### Phase 1: Project Setup (Week 1) — PARTIALLY DONE

**Already completed:**
- Azure account with $200 credit
- Azure OpenAI with `gpt-4.1-mini` deployed
- Databricks Free Edition with Spark 4.1.0 confirmed
- Snowflake trial account created
- GitHub repo created and cloned
- Folder structure created and pushed

**Still needed in Phase 1:**

**Export SQLite to CSVs:**
Write `data_engineering/notebooks/00_export_sqlite.py` locally:
```python
import sqlite3
import pandas as pd
import os

db_path = "path/to/your/database.db"
output_dir = "raw-data/"
os.makedirs(output_dir, exist_ok=True)

tables = [
    "patients", "medications", "lab_results", "allergies",
    "immunizations", "care_providers", "progress_notes",
    "recent_visits", "medical_history", "medical_history_type",
    "diagnostic_imaging"
]

conn = sqlite3.connect(db_path)
for table in tables:
    df = pd.read_sql(f"SELECT * FROM {table}", conn)
    df.to_csv(f"{output_dir}/{table}.csv", index=False)
    print(f"Exported {table}: {len(df)} rows")
conn.close()
```

**Set up Python virtual environment:**
```bash
cd backend
python -m venv venv
source venv/bin/activate   # Mac/Linux
# venv\Scripts\activate    # Windows
```

**Install requirements — create `backend/requirements.txt`:**
```
fastapi==0.115.0
uvicorn==0.30.0
python-dotenv==1.0.0
pydantic==2.7.0
snowflake-connector-python==3.10.0
langchain==0.2.0
langchain-openai==0.1.0
openai==1.30.0
pytest==8.2.0
pytest-cov==5.0.0
httpx==0.27.0
```
```bash
pip install -r requirements.txt
```

**Create `backend/.env`:**
```
AZURE_OPENAI_ENDPOINT=https://tejas-mm5fmpka-eastus2.cogni...
AZURE_OPENAI_KEY=your_key_here
AZURE_OPENAI_DEPLOYMENT=gpt-4.1-mini

SNOWFLAKE_ACCOUNT=
SNOWFLAKE_USER=
SNOWFLAKE_PASSWORD=
SNOWFLAKE_DATABASE=CLINICAL_DB
SNOWFLAKE_SCHEMA=PUBLIC
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
```

**Create Snowflake database — run this in Snowflake Worksheets:**
```sql
CREATE DATABASE CLINICAL_DB;
USE DATABASE CLINICAL_DB;
USE SCHEMA PUBLIC;

CREATE TABLE patients (
    patient_id INTEGER PRIMARY KEY,
    patient_name VARCHAR(200),
    age INTEGER,
    gender VARCHAR(20),
    dob DATE
);

CREATE TABLE medications (
    medication_id INTEGER PRIMARY KEY,
    patient_id INTEGER,
    medication_name VARCHAR(200),
    dosage VARCHAR(100),
    frequency VARCHAR(100)
);

CREATE TABLE lab_results (
    lab_result_id INTEGER PRIMARY KEY,
    patient_id INTEGER,
    test_name VARCHAR(200),
    result_value FLOAT,
    normal_range VARCHAR(100),
    test_date DATE
);

CREATE TABLE allergies (
    allergy_id INTEGER PRIMARY KEY,
    patient_id INTEGER,
    allergen VARCHAR(200),
    severity VARCHAR(50),
    reaction_type VARCHAR(200)
);

CREATE TABLE immunizations (
    immunization_id INTEGER PRIMARY KEY,
    patient_id INTEGER,
    vaccine_name VARCHAR(200),
    date_administered DATE
);

CREATE TABLE care_providers (
    provider_id INTEGER PRIMARY KEY,
    patient_id INTEGER,
    provider_name VARCHAR(200),
    specialty VARCHAR(200),
    contact VARCHAR(200)
);

CREATE TABLE progress_notes (
    note_id INTEGER PRIMARY KEY,
    patient_id INTEGER,
    note_date DATE,
    note_text TEXT,
    author VARCHAR(200)
);

CREATE TABLE recent_visits (
    visit_id INTEGER PRIMARY KEY,
    patient_id INTEGER,
    visit_date DATE,
    visit_reason VARCHAR(500),
    diagnosis VARCHAR(500)
);

CREATE TABLE medical_history_type (
    medical_history_type_id INTEGER PRIMARY KEY,
    condition_name VARCHAR(200)
);

CREATE TABLE medical_history (
    medical_history_id INTEGER PRIMARY KEY,
    patient_id INTEGER,
    medical_history_type_id INTEGER,
    onset_date DATE
);

CREATE TABLE diagnostic_imaging (
    imaging_id INTEGER PRIMARY KEY,
    patient_id INTEGER,
    imaging_type VARCHAR(200),
    result TEXT,
    image_date DATE
);
```

**JD skills demonstrated:** Project setup, Git, environment management, SQL DDL

---

### Phase 2: Data Lake Setup (Week 2 — first half)

**Goal:** Get your raw CSVs into Azure Data Lake Storage in the Bronze layer.

**What to do:**
- In Azure portal, create a Resource Group called `clinical-intelligence-rg`
- Create an Azure Data Lake Storage Gen2 account
- Create containers: `/raw/patients/`, `/raw/medications/`, etc. (one per table)
- Upload each CSV file to its corresponding `/raw/` folder
- Write `infra/main.tf` defining these ADLS resources in Terraform

**What you understand after this phase:**
- What a data lake is vs. a database (files vs. structured tables)
- Why Bronze/Silver/Gold separation exists — you never overwrite raw data
- How ADLS differs from regular blob storage (hierarchical namespace for big data workloads)

**JD skills demonstrated:** Azure cloud infrastructure, data lake concepts, IaC/Terraform beginning

---

### Phase 3: PySpark ETL Pipeline on Databricks (Weeks 2–3)

**Goal:** Build three PySpark notebooks that form your complete ETL pipeline. This is the heart of your data engineering story.

**Notebook 1 — `01_ingest.py` (Extract + Validate)**

For each of the 11 tables:
```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType, FloatType, DateType

spark = SparkSession.builder.appName("ClinicalIngest").getOrCreate()

# Read from ADLS Bronze layer
patients_df = spark.read.csv(
    "abfss://raw@youradls.dfs.core.windows.net/patients/patients.csv",
    header=True, inferSchema=True
)

# Validate schema
expected_cols = ["patient_id", "patient_name", "age", "gender", "dob"]
missing = [c for c in expected_cols if c not in patients_df.columns]
if missing:
    raise ValueError(f"Missing columns: {missing}")

# Log null counts
for col_name in patients_df.columns:
    null_count = patients_df.filter(col(col_name).isNull()).count()
    print(f"{col_name}: {null_count} nulls")

# Drop full duplicates
patients_df = patients_df.dropDuplicates()

# Write to Silver layer
patients_df.write.mode("overwrite").parquet(
    "abfss://silver@youradls.dfs.core.windows.net/patients/"
)
print(f"patients: {patients_df.count()} rows written to silver")
```

**Notebook 2 — `02_transform.py` (Transform + Enrich)**

For each table — standardize column names, cast types, add derived columns:
```python
from pyspark.sql.functions import col, year, when, upper, to_date

# Read from Silver
patients_df = spark.read.parquet("abfss://silver@.../patients/")

# Standardize and enrich
patients_df = (patients_df
    .withColumn("dob", to_date(col("dob")))
    .withColumn("age_group", when(col("age") < 18, "Pediatric")
                              .when(col("age") < 40, "Young Adult")
                              .when(col("age") < 65, "Adult")
                              .otherwise("Senior"))
)

# Medications — normalize names to uppercase
medications_df = spark.read.parquet("abfss://silver@.../medications/")
medications_df = medications_df.withColumn(
    "medication_name", upper(col("medication_name"))
)

# Lab results — add flag for abnormal results
# (requires parsing normal_range — real data engineering work)

# Write all to Gold
patients_df.write.mode("overwrite").parquet("abfss://gold@.../patients/")
medications_df.write.mode("overwrite").parquet("abfss://gold@.../medications/")
```

**Notebook 3 — `03_load.py` (Load to Snowflake)**

```python
# Snowflake connection config
snowflake_options = {
    "sfURL": "your-account.snowflakecomputing.com",
    "sfUser": "your_user",
    "sfPassword": "your_password",
    "sfDatabase": "CLINICAL_DB",
    "sfSchema": "PUBLIC",
    "sfWarehouse": "COMPUTE_WH"
}

# Load each table using upsert (MERGE) logic
patients_gold = spark.read.parquet("abfss://gold@.../patients/")
patients_gold.write \
    .format("net.snowflake.spark.snowflake") \
    .options(**snowflake_options) \
    .option("dbtable", "patients") \
    .mode("overwrite") \
    .save()
print(f"Loaded patients: {patients_gold.count()} rows")
```

**JD skills demonstrated:** Databricks, Apache Spark/PySpark, Medallion Architecture, data quality validation, ETL pipeline design

---

### Phase 4: Snowflake Verification + DataService (Week 3)

**Goal:** Confirm data landed correctly, build the Python layer agents use to query it.

**Verify in Snowflake Worksheets:**
```sql
-- Check row counts
SELECT 'patients' as tbl, COUNT(*) as rows FROM patients
UNION ALL SELECT 'medications', COUNT(*) FROM medications
UNION ALL SELECT 'lab_results', COUNT(*) FROM lab_results;

-- Test a real JOIN
SELECT p.patient_name, m.medication_name, lr.test_name, lr.result_value
FROM patients p
JOIN medications m ON p.patient_id = m.patient_id
JOIN lab_results lr ON p.patient_id = lr.patient_id
WHERE lr.result_value > 100
LIMIT 10;
```

**Write `backend/app/services/data_service.py`:**
```python
import snowflake.connector
from typing import List, Dict, Any

class DataService:
    def __init__(self, config: dict):
        self.config = config  # dependency injection — testable

    def execute_query(self, sql: str) -> List[Dict[str, Any]]:
        conn = snowflake.connector.connect(**self.config)
        try:
            cursor = conn.cursor(snowflake.connector.DictCursor)
            cursor.execute(sql)
            return cursor.fetchall()
        finally:
            conn.close()

    def get_schema(self) -> str:
        """Returns the full schema as a string for the SQL Agent system prompt"""
        return """
        TABLE: patients (patient_id, patient_name, age, gender, dob, age_group)
        TABLE: medications (medication_id, patient_id, medication_name, dosage, frequency)
        TABLE: lab_results (lab_result_id, patient_id, test_name, result_value, normal_range, test_date)
        TABLE: allergies (allergy_id, patient_id, allergen, severity, reaction_type)
        TABLE: immunizations (immunization_id, patient_id, vaccine_name, date_administered)
        TABLE: care_providers (provider_id, patient_id, provider_name, specialty, contact)
        TABLE: progress_notes (note_id, patient_id, note_date, note_text, author)
        TABLE: recent_visits (visit_id, patient_id, visit_date, visit_reason, diagnosis)
        TABLE: medical_history (medical_history_id, patient_id, medical_history_type_id, onset_date)
        TABLE: medical_history_type (medical_history_type_id, condition_name)
        TABLE: diagnostic_imaging (imaging_id, patient_id, imaging_type, result, image_date)
        All tables join on: patient_id
        medical_history joins medical_history_type on: medical_history_type_id
        """
```

**JD skills demonstrated:** Snowflake, Python OOP/SOLID, dependency injection, Pytest unit testing

---

### Phase 5: AI Agents (Weeks 4–5)

**Goal:** Build all four agents as independent, testable Python classes.

**SQL Agent — `backend/app/agents/sql_agent.py`:**
```python
from langchain_openai import AzureChatOpenAI
from langchain.prompts import ChatPromptTemplate

class SQLAgent:
    def __init__(self, data_service, llm):
        self.data_service = data_service
        self.llm = llm

    def run(self, question: str) -> dict:
        schema = self.data_service.get_schema()

        prompt = ChatPromptTemplate.from_messages([
            ("system", f"""You are a SQL expert for a clinical database.
            Generate a valid Snowflake SQL query for the user's question.
            Return ONLY the SQL query, nothing else.

            Schema:
            {schema}

            Rules:
            - Always use table aliases
            - Never SELECT *
            - Always include patient_id in results when querying patient data
            - Use LIMIT 50 unless the question asks for aggregations
            """),
            ("human", question)
        ])

        sql = self.llm.invoke(prompt.format_messages()).content.strip()
        results = self.data_service.execute_query(sql)

        return {"sql": sql, "results": results, "row_count": len(results)}
```

**Web Search Agent — `backend/app/agents/web_search_agent.py`:**
```python
class WebSearchAgent:
    def __init__(self, llm):
        self.llm = llm

    def run(self, question: str) -> dict:
        # Step 1: Extract medical entities from the question
        extract_prompt = f"""
        Extract medical entities from this question that would benefit from web search.
        Return a JSON object with: medications (list), conditions (list), lab_tests (list)
        Question: {question}
        """
        entities = self.llm.invoke(extract_prompt).content

        # Step 2: Formulate search queries
        search_queries = self._build_search_queries(entities)

        # Step 3: Execute web search via Azure OpenAI
        context = []
        for query in search_queries:
            result = self._search(query)
            context.append(result)

        return {"queries": search_queries, "context": " ".join(context)}

    def _build_search_queries(self, entities: str) -> list:
        # Build specific, high-quality medical search queries
        # e.g., "Metformin Type 2 Diabetes dosage clinical guidelines 2024"
        pass

    def _search(self, query: str) -> str:
        # Call Azure OpenAI with web search tool enabled
        pass
```

**Build order — strictly one at a time:**
1. SQL Agent — hardcode a test question, verify generated SQL is correct for your schema
2. Web Search Agent — test with a medication question, verify web results are relevant
3. Insight Agent — feed hardcoded SQL results + web context, verify coherent clinical output
4. Judge Agent — verify it returns valid JSON with all four score fields
5. Orchestrator — wire all four, test with 15 real questions

**JD skills demonstrated:** Agentic AI, orchestration, A2A coordination, LLMs, web search integration, observability

---

### Phase 6: FastAPI Backend (Week 5)

**Goal:** Expose the orchestrator as a real HTTP API.

**`backend/app/models/schemas.py`:**
```python
from pydantic import BaseModel
from typing import List, Optional

class QueryRequest(BaseModel):
    question: str

class JudgeScore(BaseModel):
    overall_score: int
    relevance: int
    data_grounding: int
    actionability: int
    safety_flag: bool
    feedback: str

class QueryResponse(BaseModel):
    question: str
    answer: str
    sql_used: str
    agents_invoked: List[str]
    web_sources: List[str]
    scores: JudgeScore
```

**`backend/app/main.py`:**
```python
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI(title="Clinical Intelligence API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173"],
    allow_methods=["*"],
    allow_headers=["*"],
)
```

Test at `localhost:8000/docs` — FastAPI auto-generates interactive Swagger UI.

**JD skills demonstrated:** Backend API development, Pydantic validation, OOP/SOLID, integration testing

---

### Phase 7: React Frontend (Week 6 — first half)

**Goal:** Build a clean, functional clinical chat interface.

```bash
npm create vite@latest frontend -- --template react-ts
cd frontend
npm install axios tailwindcss
```

**Six components to build:**
- `QuestionInput.tsx` — text input + Ask button
- `AnswerCard.tsx` — displays the clinical insight
- `AgentBadges.tsx` — colored pills showing which agents fired
- `ScoreDisplay.tsx` — Judge scores (green ≥8, yellow 5–7, red ≤4)
- `SqlPreview.tsx` — collapsible panel showing generated SQL
- `WebSources.tsx` — list of websites the search agent used

**The medical disclaimer banner goes in `App.tsx` — non-negotiable:**
```tsx
<div className="bg-yellow-100 border-l-4 border-yellow-500 p-4 mb-6">
  <p className="font-bold text-yellow-800">⚠️ Demonstration Only</p>
  <p className="text-yellow-700">
    This system is for portfolio demonstration purposes only.
    Not for clinical decision-making. Always consult a qualified healthcare professional.
  </p>
</div>
```

**JD skills demonstrated:** JavaScript/TypeScript, React, component architecture, responsible AI

---

### Phase 8: Docker + Docker Compose (Week 6 — second half)

**`backend/Dockerfile`:**
```dockerfile
FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
```

**`frontend/Dockerfile` (multi-stage):**
```dockerfile
FROM node:20-alpine AS build
WORKDIR /app
COPY package*.json .
RUN npm install
COPY . .
RUN npm run build

FROM nginx:alpine
COPY --from=build /app/dist /usr/share/nginx/html
EXPOSE 80
```

**`docker-compose.yml`:**
```yaml
services:
  backend:
    build: ./backend
    ports:
      - "8000:8000"
    env_file: ./backend/.env
  frontend:
    build: ./frontend
    ports:
      - "3000:80"
```

Test: `docker-compose up --build` — full app at `localhost:3000`.

**JD skills demonstrated:** Docker, multi-stage builds, containerization

---

### Phase 9: Terraform + Kubernetes (Week 7 — first half)

**`infra/main.tf` provisions:**
- Azure Resource Group: `clinical-intelligence-rg`
- Azure Data Lake Storage Gen2
- Azure Container Registry (ACR)
- AKS cluster (2-node, standard B-series VMs)

```bash
terraform init
terraform plan
terraform apply
```

**`k8s/backend-deployment.yaml`:**
```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: clinical-ai-backend
spec:
  replicas: 2
  selector:
    matchLabels:
      app: backend
  template:
    spec:
      containers:
      - name: backend
        image: yourregistry.azurecr.io/clinical-ai-backend:latest
        ports:
        - containerPort: 8000
        envFrom:
        - secretRef:
            name: clinical-ai-secrets
```

Deploy: `kubectl apply -f k8s/` then `kubectl get pods` to confirm Running.

**JD skills demonstrated:** Terraform/IaC, Kubernetes, AKS, Azure cloud infrastructure

---

### Phase 10: GitHub Actions CI/CD (Week 7 — second half)

**`.github/workflows/ci-cd.yml`:**
```yaml
name: CI/CD Pipeline

on:
  push:
    branches: [main]

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - uses: actions/setup-python@v4
        with:
          python-version: '3.11'
      - run: pip install -r backend/requirements.txt
      - run: pytest backend/tests/ -v --cov=app

  build-and-deploy:
    needs: test
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - name: Build and push backend
        run: |
          docker build -t $ACR_LOGIN_SERVER/clinical-ai-backend:$GITHUB_SHA ./backend
          docker push $ACR_LOGIN_SERVER/clinical-ai-backend:$GITHUB_SHA
      - name: Deploy to AKS
        run: |
          kubectl set image deployment/clinical-ai-backend \
            backend=$ACR_LOGIN_SERVER/clinical-ai-backend:$GITHUB_SHA
```

**JD skills demonstrated:** CI/CD pipelines, GitHub Actions, DevOps, continuous delivery

---

### Phase 11: Observability + Documentation (Week 8)

**Goal:** Add production-quality monitoring and wrap up professionally.

**Add logging to Orchestrator:**
```python
import logging
import time

logger = logging.getLogger(__name__)

class Orchestrator:
    def run(self, question: str):
        start = time.time()
        logger.info(f"Query received: {question[:100]}")

        sql_start = time.time()
        sql_result = self.sql_agent.run(question)
        logger.info(f"SQL Agent: {len(sql_result['results'])} rows, {time.time()-sql_start:.2f}s")

        search_result = self.web_search_agent.run(question)
        logger.info(f"Web Search Agent: {len(search_result['queries'])} queries")

        insight = self.insight_agent.run(question, sql_result, search_result)
        scores = self.judge_agent.run(question, insight)
        logger.info(f"Judge scores: overall={scores['overall_score']}, safety={scores['safety_flag']}")
        logger.info(f"Total time: {time.time()-start:.2f}s")
```

**Write `docs/adr/002-why-web-search-not-rag.md`:**
Explain why web search is better than RAG for medical data — documents go out of date, drug guidelines change, web search is always current.

**Write `docs/data_dictionary.md`:** Every table, every column, data type, source, example value.

**Record Loom demo** showing: Databricks pipeline running, Snowflake tables populated, asking a medication question, seeing all 4 agents fire, the SQL that was generated, web sources used, and the Judge score.

**JD skills demonstrated:** Observability, technical documentation, ADRs, data dictionary, responsible AI

---

## Complete Skills Coverage

| Skill Area | Phase | How It Is Covered |
|---|---|---|
| Data Engineering / ETL | Phase 3 | PySpark pipeline across 11 tables: ingest, transform, load |
| Medallion Architecture | Phase 3 | Bronze/Silver/Gold in ADLS — industry-standard pattern |
| Apache Spark / Databricks | Phase 3 | PySpark DataFrames, joins, null handling, Parquet I/O |
| Snowflake / Data Warehousing | Phases 3–4 | Spark loads to Snowflake; agents query live |
| Agentic AI / Orchestration | Phase 5 | 4-agent pipeline with LangGraph, parallel execution |
| LLMs / Azure OpenAI | Phase 5 | GPT-4o powers all four agents |
| Web Search / Real-time Data | Phase 5 | Web Search Agent fetches live medical context per question |
| Observability / Quality Control | Phase 5, 11 | Judge Agent scores every answer; safety flag; full logging |
| OOP / SOLID Principles | Phases 4–6 | Python classes, dependency injection, single responsibility |
| Backend Development | Phase 6 | FastAPI with Pydantic validation, async endpoints |
| JavaScript / TypeScript | Phase 7 | React + TypeScript frontend with 6 components |
| Docker / Containerization | Phase 8 | Multi-stage Dockerfiles, docker-compose for local dev |
| Kubernetes / AKS | Phase 9 | K8s manifests, 2 replicas each, AKS deployment |
| Infrastructure as Code | Phase 9 | Terraform provisions all Azure resources |
| CI/CD Pipelines | Phase 10 | GitHub Actions: test → build → push → deploy |
| Unit Testing / Pytest | Phases 4–6 | Pytest with mocked dependencies for all services and agents |
| Technical Documentation | Phase 11 | Architecture doc, data dictionary, ADRs, README |
| Responsible AI | Phases 7, 11 | Medical disclaimer, safety flag, explainable SQL output |

---

## Key Concepts to Understand Before Building

**Data Engineering:**
- What is ETL vs ELT — and why does the order matter?
- What is the Medallion Architecture and why do you never modify the Bronze layer?
- What is a Parquet file and why is it better than CSV for pipeline intermediate storage?
- What is idempotency — why does it matter that running the pipeline twice doesn't duplicate data?
- What is a DAG (Directed Acyclic Graph) — your Databricks Workflow is one

**Spark / Databricks:**
- What is lazy evaluation — why don't Spark transformations execute immediately?
- What is the difference between a Spark DataFrame and a Pandas DataFrame?
- What is a Spark action vs. a transformation?

**GenAI / Agents:**
- What is the difference between a regular LLM call and an agentic workflow?
- Why does grounding answers in real data (SQL results) reduce hallucination?
- What does it mean for agents to run in parallel vs. sequentially?
- What is prompt engineering — why does the schema in the SQL Agent system prompt matter?

**Cloud / DevOps:**
- What is the difference between a Docker image and a running container?
- What problem does Kubernetes solve that Docker alone doesn't?
- What is the difference between CI (continuous integration) and CD (continuous delivery)?
- What does infrastructure as code mean and why is it better than clicking in the Azure portal?

---

## Practical Tips

- **Export SQLite before anything else.** Run the export script, put all 11 CSVs in `raw-data/`, verify row counts match SQLite before moving to Phase 2.

- **Snowflake trial is 30 days.** Do not activate it until you are ready for Phase 3. Once Spark loads data, you only need Snowflake for Phases 4–6. Screenshot the populated tables — that is permanent evidence even after the trial expires.

- **Test every agent in isolation before wiring them together.** Hardcode inputs, print outputs, confirm correctness. The most common mistake is connecting broken agents and not knowing which one is failing.

- **The `.env` file is sacred.** Already in `.gitignore`. If you accidentally commit API keys, rotate them immediately in the Azure portal.

- **Commit at every milestone.** Every time a notebook runs, a test passes, or an agent returns the correct output — commit with a meaningful message like `feat: sql-agent generates correct multi-table JOIN`. A rich git history is part of your portfolio.

- **The medical disclaimer is non-negotiable.** Add it to the UI from day one. It shows you think about responsible AI — a green flag for every hiring manager.

- **Test your SQL Agent with at least 15 different questions.** It will sometimes generate SQL with wrong column names. Every failure case you fix by updating the system prompt is prompt engineering — a real, demonstrable skill.

- **Record your Loom demo before your Snowflake trial expires.** The demo is permanent evidence even after the trial ends.

---

> ⚠️ **Disclaimer:** This platform uses synthetic patient data for portfolio and learning purposes only. It is not intended for clinical decision-making. Always consult a qualified healthcare professional for medical decisions.
