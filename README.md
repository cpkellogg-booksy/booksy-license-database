# Booksy License Database (Multistate ETL & Mapping)

This repository hosts a "headless" ETL pipeline that automatically aggregates professional license data from multiple states. It processes **Florida (DBPR)** and **Texas (TDLR)** data, standardizes addresses, and generates interactive maps.

## 🏗 Architecture

* **Sources:**
    * **Florida:** DBPR Cosmetology & Barber extracts (CSV).
    * **Texas:** TDLR Barbering & Cosmetology program extracts (CSV/API).
* **Orchestration:** **GitHub Actions** (Daily refresh at 8:00 AM UTC).
* **Storage:** **CockroachDB Serverless** (PostgreSQL-compatible).
* **Geocoding:** Hybrid Engine (US Census Bureau + Mapbox API) with spatial boundary filtering.

## 🚀 Automation Flow

### Stage 1: The "Factories" (`etl_fl.py`, `etl_tx.py`)
1.  **Extract:** Downloads raw data for practitioners and establishments.
2.  **Transform:**
    * **Florida:** Maps positional columns and filters for **Current (C)** and **Active (A)** status codes.
    * **Texas:** Maps explicit headers and filters using **License Subtypes** (e.g., `BA` for Barber, `CS` for Salon).
    * **Cleaning:** Removes **PO Box** addresses and standardizes physical locations using AI address parsing.
3.  **Load:** Saves cleaned data to state-specific "Gold" tables (`address_insights_fl_gold`, `address_insights_tx_gold`).

### Stage 2: The "Mappers" (`map_gen_fl.py`, `map_gen_tx.py`)
1.  **Geocoding:** Uses a shared `geo_cache` to minimize API costs.
2.  **Spatial Filtering:** Verifies coordinates against state-specific bounding boxes to remove out-of-state mailing addresses.
3.  **Artifacts:** Generates `Booksy_FL_Licenses.csv` and `Booksy_TX_Licenses.csv` for visualization in Kepler.gl.

## 📊 Unified Mapping Logic

| Grouping | Florida Code | Texas Subtype |
| :--- | :--- | :--- |
| **Barbers** | BB, BR, BA | BA, BT, TE, BR |
| **Cosmetologists** | CL, FV, FB, FS | OP, FA, MA, HW, WG, SH, OR, MR |
| **Establishments** | CE, MCS, BS | CS, MS, FS, HS, FM, WS, BS, DS |
| **Schools** | PROV, PVDR | BC, VS, JC, PS |

## 🛠 Setup
1. Create a **CockroachDB Serverless** cluster.
2. Add your connection string and Mapbox token to **GitHub Secrets**.
3. Enable the **Daily Refresh** workflow.
