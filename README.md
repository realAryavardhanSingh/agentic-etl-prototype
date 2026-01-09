# Agentic ETL Prototype: Self-Healing Data Infrastructure 🧠⚡

> **A prototype demonstrating Autonomic Data Systems using Control Theory and AI Agents.**

This project implements a "Self-Healing" data pipeline. Instead of crashing when data quality issues or schema changes occur, the system detects the anomaly, analyzes it using an Agentic workflow, and automatically applies a fix (e.g., Schema Evolution or Quarantine).

## 🏗 Architecture Overview

The system is designed in two phases. Currently, **Phase 1 (Local Simulation)** is fully operational. **Phase 2 (Cloud-Native)** code is written and awaiting AWS infrastructure provisioning.

### 🟢 Phase 1: Local Agent Simulation (Current Status)
To demonstrate the *logic* without incurring cloud compute costs, the "Control Plane" runs locally:
1.  **The Plant (Chaos Monkey):** A Python script pushes JSON data to **AWS S3**, randomly injecting "Poison Pill" records (Schema Mismatches & Data Type errors).
2.  **The Sensor (Local Monitor):** A `boto3` script watches S3 for new files and scans them against a contract.
3.  **The Controller (Mock LLM):** An `ArchitectAgent` class simulates LLM reasoning to generate SQL fixes or Quarantine decisions.

### 🟡 Phase 2: Databricks Lakehouse (Future Direction)
*Code is implemented in `databricks_jobs/` and ready for deployment.*
1.  **Ingestion:** **Delta Live Tables (DLT)** replaces the local monitor for enterprise-grade Auto-Loading.
2.  **Observability:** The Agent monitors the **DLT Event Log** (system tables) instead of raw S3 files.
3.  **The Brain:** The Mock Agent is replaced by **Databricks Model Serving (DBRX/Llama 3)** for real-time decision making.
4.  **Actuation:** **Unity Catalog** is used to apply `ALTER TABLE` commands safely.

---

## 🚀 How to Run (Phase 1 Demo)

You can run the full simulation right now on a local machine.

### Prerequisites
*   Python 3.8+
*   AWS Credentials configured (for S3 access)

### 1. Start the "Chaos Monkey" 🐒
This script acts as the data source, generating valid data and occasional errors.
```
python data_generator/chaos_monkey.py
```

### 2. Start the "Agent" 🕵️‍♂️
In a separate terminal, run the monitoring agent. Watch it detect errors and generate SQL fixes in real-time.

```
python databricks_jobs/local_agent.py
```

## 📂 Project Structure
```
agentic-etl-prototype/
├── data_generator/
│   └── chaos_monkey.py        # Generates stream of Good + Bad data to S3
├── databricks_jobs/
│   ├── 01_dlt_pipeline.py     # Cloud Native: Delta Live Tables definition (Bronze/Silver)
│   ├── local_agent.py         # Local Simulation: The detection and logic engine
│   └── ...
├── README.md                  # Project Documentation
└── requirements.txt           # Python dependencies
```

## 🛠 Tech Stack
- Core: Python, SQL
- Cloud: AWS S3, Databricks (Delta Live Tables, Unity Catalog)
- Concepts: Control Theory, Agentic Workflows, Medallion Architecture