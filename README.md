# LedgerLink — Real-Time Data Engineering Platform

LedgerLink is a modern data engineering platform designed to process streaming and batch data using **Apache Kafka, Apache Spark Structured Streaming, Azure SQL, Databricks Lakeflow, and Delta Lake**. It follows the Medallion Architecture to transform raw data into reliable, analytics-ready datasets for reporting and consumption.

## Architecture

![LedgerLink Data Pipeline Architecture](./LedgerLink%20Data%20Pipeline%20Architecture.png)

## Technology Stack

| Component | Technology | Purpose |
|---|---|---|
| Streaming Sources | Apache Kafka, JSON Files | Ingest streaming events and file-based data |
| Streaming Ingestion | Apache Spark Structured Streaming | Process incoming streaming data |
| Batch Source | Azure SQL | Provide relational source data |
| Batch Ingestion | Lakeflow Connect | Ingest batch data |
| Data Processing | Databricks Lakeflow Declarative Pipelines | Implement streaming joins, static joins, and fraud detection logic |
| Storage | Delta Lake | Store reliable, versioned datasets |
| Data Architecture | Medallion Architecture | Organize data into Bronze, Silver, and Gold layers |
| Orchestration | Lakeflow Jobs | Schedule and orchestrate pipeline execution |
| Governance | Unity Catalog | Manage data access and governance |
| Reporting | Databricks Dashboards, Microsoft Power BI | Enable analytics and reporting |

## Data Pipeline

### 1. Data Ingestion

LedgerLink supports both streaming and batch ingestion.

- **Streaming:** Apache Kafka and JSON files provide event data processed using Spark Structured Streaming.
- **Batch:** Azure SQL provides relational data ingested through Lakeflow Connect.

### 2. Data Processing

Databricks Lakeflow Declarative Pipelines handle transformations, supporting:

- Stream-to-stream joins.
- Stream-to-static joins.
- Fraud detection and related business rules.

### 3. Medallion Architecture

Delta Lake provides the storage foundation for the three processing layers.

- **Bronze:** Stores ingested source data.
- **Silver:** Contains cleaned, validated, and transformed data.
- **Gold:** Provides curated, analytics-ready datasets for reporting.

### 4. Orchestration and Governance

Lakeflow Jobs orchestrate pipeline execution and dependencies. Unity Catalog provides centralized data governance, discovery, and access management.

### 5. Reporting and Consumption

Gold-layer datasets serve as the primary consumption layer for Databricks Dashboards and Microsoft Power BI, enabling business reporting and analytical insights.

## Key Capabilities

- Streaming and batch data ingestion.
- Stream-to-stream and stream-to-static joins.
- Delta Lake Medallion Architecture.
- Data transformation and fraud detection logic.
- Pipeline orchestration and centralized governance.
- Analytics-ready datasets for business intelligence.

## Project Objective

LedgerLink demonstrates a modern data engineering architecture that integrates streaming and batch sources, transforms data through governed Delta Lake layers, and delivers curated datasets for analytics and reporting.

