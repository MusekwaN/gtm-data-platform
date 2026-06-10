\# Real-Time GTM Data Engineering Platform



A production-grade, end-to-end data engineering platform for B2B SaaS 

Go-To-Market operations. Built with modern data stack technologies including 

Kafka, Snowflake, dbt, AI agents, and full observability.



\## Architecture

Apollo.io / HubSpot / Salesforce

│

Kafka Streams

│

Spark/Python Pipeline

│

Bronze → Silver → Gold

│

Snowflake + dbt Models

│

┌─────────┴──────────┐

│                    │

AI Agents          Reverse ETL

(LangGraph)        (n8n + Slack)

│                    │

└─────────┬──────────┘

│

Grafana Dashboard

## Tech Stack



| Layer | Technology |

|---|---|

| Streaming | Apache Kafka |

| Processing | Python / Apache Spark |

| Warehouse | Snowflake |

| Transformations | dbt |

| Orchestration | Dagster |

| AI Agents | LangGraph + OpenAI |

| Reverse ETL | n8n |

| Observability | Prometheus + Grafana |

| Metadata | OpenMetadata |

| IaC | Terraform |

| CI/CD | GitHub Actions |

| Containerization | Docker |



\## What It Does



\### Real-Time Lead Intelligence

\- Ingests leads from Apollo.io, HubSpot and Salesforce via Kafka

\- Processes events through Bronze → Silver → Gold medallion architecture

\- Deduplicates and standardizes 150+ leads into clean unified profiles



\### Identity Resolution Engine

\- Matches same person across multiple systems using exact, fuzzy and domain matching

\- Generates stable Global IDs for unified customer profiles

\- Reduces duplicate records by up to 33%



\### AI Lead Scoring

\- Enrichment Agent — researches company context

\- Intent Agent — detects buying signals and scores 0-100

\- Outreach Agent — generates personalized email and LinkedIn messages

\- Full agent pipeline runs in under 60 seconds per batch



\### Reverse ETL + Automation

\- Pushes AI insights back into Slack, Salesforce and Apollo

\- n8n workflows trigger automated outreach sequences

\- Hot leads (score 80+) get instant Slack alerts with AI-generated messaging



\### Observability

\- Prometheus metrics on all pipeline layers

\- Grafana dashboard showing live lead scoring distribution

\- Great Expectations data quality checks on every run

\- Pipeline health report with pass/fail status



\### Infrastructure as Code

\- Terraform manages all Snowflake resources

\- Docker Compose for local development

\- GitHub Actions CI/CD with automated testing on every push



\## Project Structure

gtm-data-platform/

├── ingestion/          # API connectors (Apollo, HubSpot)

├── streaming/          # Spark streaming jobs

├── transformations/    # dbt models (Bronze/Silver/Gold)

├── ai\_agents/          # Lead scoring AI agents

├── reverse\_etl/        # Reverse ETL engine

├── monitoring/         # Grafana, Prometheus, quality checks

├── infrastructure/     # Docker, Terraform

└── .github/workflows/  # CI/CD pipelines

## Quick Start



\### Prerequisites

\- Docker Desktop

\- Python 3.11+

\- Snowflake account (free trial)



\### Setup



```bash

\# Clone the repo

git clone https://github.com/MusekwaN/gtm-data-platform.git

cd gtm-data-platform



\# Create virtual environment

python -m venv .venv

.venv\\Scripts\\activate  # Windows



\# Install dependencies

pip install -r requirements.txt



\# Configure environment

cp .env.example .env

\# Edit .env with your API keys



\# Start Docker services

docker compose up -d



\# Create Kafka topics

docker exec gtm-data-platform-kafka-1 kafka-topics --create \\

&#x20; --topic apollo\_leads --bootstrap-server localhost:9092 \\

&#x20; --partitions 3 --replication-factor 1



\# Run the full pipeline

python test\_data\_injector.py

python simple\_pipeline.py

python ai\_agents/lead\_scoring/ai\_pipeline.py

python reverse\_etl/reverse\_etl\_engine.py

```



\### View Results



| Service | URL |

|---|---|

| Kafka UI | http://localhost:8080 |

| Grafana Dashboard | http://localhost:3001 |

| Prometheus | http://localhost:9090 |

| n8n Workflows | http://localhost:5678 |

| Pipeline Metrics | http://localhost:8000/metrics |



\## Pipeline Results

Bronze records  : 150

Silver records  : 143  (deduplicated)

Gold records    : 143  (scored)

Unified IDs     : 96   (identity resolved)

Hot leads       : 77   (score 80+)

Avg lead score  : 76.6

## CI/CD



Every push to main triggers:

1\. Code quality checks

2\. Pipeline unit tests  

3\. dbt model compilation and tests

4\. Slack notification on pass/fail



\## Skills Demonstrated



\- \*\*Streaming systems\*\* — Kafka event-driven architecture

\- \*\*Data modeling\*\* — medallion lakehouse (Bronze/Silver/Gold)

\- \*\*Warehouse engineering\*\* — Snowflake + dbt transformations

\- \*\*AI engineering\*\* — LangGraph agents with OpenAI

\- \*\*Platform engineering\*\* — end-to-end observability

\- \*\*DevOps/DataOps\*\* — Docker, Terraform, GitHub Actions CI/CD

\- \*\*Reverse ETL\*\* — operational data activation

\- \*\*Identity resolution\*\* — cross-system entity matching



\## Author



Built by MusekwaN as a data engineering portfolio project.

Demonstrates real-world GTM data platform architecture used at

modern B2B SaaS companies.


