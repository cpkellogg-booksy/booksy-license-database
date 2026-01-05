# Booksy License Database (ETL & Mapping Pipeline)

This repository hosts a "headless" ETL (Extract, Transform, Load) pipeline that automatically aggregates professional license data. It currently pulls **Florida** Cosmetology and Barber licenses, cleans the addresses using AI, segments them into Commercial vs. Residential locations, and generates an interactive map file.

The system is designed to be **free, open-source, and scalable**, running entirely on GitHub Actions and CockroachDB Serverless.

## 🏗 Architecture

* **Sources:**
    * [Florida DBPR Cosmetology Extract](https://www2.myfloridalicense.com/sto/file_download/extracts/COSMETOLOGYLICENSE_1.csv) (CSV)
    * [Florida DBPR Barber Extract](https://www2.myfloridalicense.com/sto/file_download/extracts/lic03bb.csv) (CSV)
* **Orchestration:** **GitHub Actions** (Runs automatically every day at 8:00 AM UTC).
* **Processing:** **Python 3.9** (Pandas, SQLAlchemy, USAddress, Requests).
* **Geocoding:** **Hybrid Turbo Engine** (US Census Bureau Batch API + Mapbox API) with **Smart Caching**.
* **Storage:** **CockroachDB Serverless** (PostgreSQL-compatible).

## 🚀 Automation Flow

### Stage 1: The "Factory" (`etl.py`)
1.  **Extract:** Downloads raw CSVs for Florida specialists and establishments.
2.  **Transform:**
    * **Universal Adapter:** Normalizes schemas using exact Florida Board codes (e.g., `BB` for Barbers, `CE` for Salons, `FS` for Full Specialists).
    * **AI Cleaning:** Uses `usaddress` to standardize addresses and fix "Ghost Data."
    * **Filtering:** Automatically removes **PO Box** addresses to ensure map accuracy.
    * **Segmentation:** Classifies locations as **Commercial** or **Residential** based on license density and unit identifiers.
3.  **Load:** Overwrites the `address_insights_gold` table in CockroachDB with fresh daily data.

### Stage 2: The "Mapper" (`map_gen.py`)
1.  **Fetch:** Reads Florida-only records from the gold table for processing.
2.  **Hybrid Routing:**
    * **Fast Lane:** Used for small daily updates.
    * **Bulk Lane:** Used for large backfills.
3.  **Parallel Processing:** Uses multi-threading to maximize geocoding speed.
4.  **Checkpointing:** Saves results to the database incrementally to prevent data loss from timeouts.
5.  **Spatial Filtering:** Final coordinates are checked against Florida's geographic boundaries to remove out-of-state mailing addresses.
6.  **Artifact:** Uploads a final `Booksy_License_Database.csv` ready for visualization.

## 🛠 Setup & Deployment

### 1. Database Setup (CockroachDB)
Create a free Serverless cluster at cockroachlabs.com and retrieve your connection string.

### 2. Local Development
```bash
# Install dependencies
pip install -r requirements.txt

# Set environment variables
