# Enterprise Logistics DataOps & Analytics Engine 🚛

## Executive Summary
Mid-sized freight operations lose significant revenue to fragmented telematics drops, untracked fuel expenditures, and unstructured driver maintenance logs. This project is an automated, end-to-end Analytics Engineering pipeline that ingests raw Class 8 trucking data, cleans broken sensor timestamps, and utilizes Natural Language Processing (NLP) to categorize unstructured fleet maintenance logs for executive Business Intelligence reporting.

## The Business Value & KPIs
This pipeline transforms messy operational exhaust into a pristine Star Schema to track three critical logistics metrics:
1. **On-Time Delivery (SLA Compliance):** Accurate to the minute, actively preserving financial records during telematics sensor drops via "Ghost Dimension" imputation.
2. **Cost-to-Serve (CPM):** 100% referential integrity on fuel expenses and maintenance costs.
3. **Delay Attribution:** NLP-categorized breakdown of operational bottlenecks (e.g., Mechanical Failure vs. Weather Delay) extracted directly from raw driver logs.

## Architecture & Tech Stack
* **Extraction & NLP:** Python, Pandas, spaCy (Dynamic CSV ingestion, datetime standardization, rule-based NLP categorization).
* **Data Warehouse:** PostgreSQL (Transitioning raw OLTP data into an analytical OLAP Star Schema).
* **Data Modeling:** SQL DDL/DML, Common Table Expressions (CTEs), Window Functions.
* **Business Intelligence:** Power BI / DAX (Executive Dashboards - *Pending integration*).

## Data Engineering Highlights
* **Exception Flagging:** Programmatic flagging (`is_telematics_drop=True`) to quantify sensor failures without executing destructive `dropna()` commands on financial data.
* **The "Ghost Dimension" Pattern:** Dynamically resolving lost upstream identifiers to explicit entities (`UNKNOWN_DRIVER`, `UNALLOCATED_ASSET`) to guarantee accurate Power BI aggregations.
* **Structural Missing Data (MNAR):** Engineering new boolean features (like `is_active_driver`) directly from missing termination dates without mutating raw chronological integrity.
* **OLTP to OLAP Transformation:** Automated SQL scripts to migrate normalized raw tables into a centralized `Fact_Shipments` table surrounded by optimized dimensions (`Dim_Truck`, `Dim_Driver`, `Dim_Route`, `Dim_Delay_Reason`).

## Repository Structure
* `/src/`: Python ETL and NLP processing modules (`extractor.py`, `transformer.py`, `nlp_processor.py`, `load.py`).
* `main.py`: The object-oriented pipeline orchestrator.
* `/sql/`: DDL and DML scripts for the Star Schema data warehouse build.
* `/sql/analytics/`: Advanced SQL queries answering C-Suite business questions.
* `/data/samples/`: Sanitized 50-row extracts of raw vs. processed datasets for quick schema verification.
* `requirements.txt` & `.env`: Dependency and local database security configurations.
## How to Run Locally
1. Clone the repository and configure your virtual environment: `python -m venv venv && source venv/bin/activate`
2. Install dependencies: `pip install -r requirements.txt`
3. Download the NLP language model: `python -m spacy download en_core_web_sm`
4. Update the `.env` file with your local PostgreSQL credentials.
5. Execute the pipeline: `python main.py`