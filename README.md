# Booksy License Database (ETL Pipeline)

This repository hosts a "headless" ETL (Extract, Transform, Load) pipeline that automatically aggregates professional license data. Currently, it pulls **Florida Cosmetology** licenses, cleans the data, and stores it in a **CockroachDB Serverless** database.

The system is designed to be **free, open-source, and scalable** to handle additional states (e.g., Texas) and license types in the future.

## 🏗 Architecture

* **Source:** [Florida DBPR CSV Extracts](https://www2.myfloridalicense.com/sto/file_download/extracts/COSMETOLOGYLICENSE_1.csv) (Updated periodically by the state).
* **Orchestration:** **GitHub Actions** (Runs automatically every day at 8:00 AM UTC).
* **Processing:** **Python 3.9** (Pandas, SQLAlchemy).
* **Storage:** **CockroachDB Serverless** (PostgreSQL-compatible).

## 🚀 Automation Flow
1.  **Trigger:** GitHub Actions wakes up daily (defined in `.github/workflows/daily_refresh.yml`).
2.  **Extract:** The `etl.py` script streams the CSV from the Florida government website.
3.  **Transform:**
    * Applies clean column headers (standardized snake_case).
    * Fixes encoding (ISO-8859-1).
    * Handles connection security (SSL/Certifi).
4.  **Load:**
    * **First Batch:** Replaces the existing table (`if_exists='replace'`) to ensure a fresh sync.
    * **Subsequent Batches:** Appends data in chunks of 10,000 rows to manage memory.

## 🛠 Setup & Installation

### 1. Prerequisites
* A **CockroachDB Serverless** cluster.
* A **GitHub Repository** (Private recommended).

### 2. Environment Variables
This project uses **GitHub Secrets** to protect database credentials.
* Go to **Settings > Secrets and variables > Actions**.
* Add a secret named `DB_CONNECTION_STRING`.
* **Value Format:** `postgresql://user:password@host:port/defaultdb...`
    * *Note: The script automatically converts this to `cockroachdb://` and attaches the correct SSL certificates at runtime.*

### 3. Local Development
To run this script on your local machine:

1.  Clone the repo:
    ```bash
    git clone [https://github.com/cpkellogg-booksy/booksy-license-database.git](https://github.com/cpkellogg-booksy/booksy-license-database.git)
    cd booksy-license-database
    ```

2.  Install dependencies:
    ```bash
    pip install -r requirements.txt
    ```

3.  Set the environment variable and run:
    ```bash
    export DB_CONNECTION_STRING="your_cockroach_db_string"
    python etl.py
    ```

## 📊 Data Schema
The current schema for the `florida_cosmetology` table includes:

| Column Name | Description |
| :--- | :--- |
| `licensee_name` | Name of the professional or business |
| `license_number` | Unique ID (e.g., CL12345) |
| `city` / `state` / `zip` | Location data |
| `expiration_date` | License expiry date |
| `primary_status` | Status (Current, Delinquent, Null and Void) |
| *(and 17 other columns)* | |

## 🔎 Verification
To check the data, log in to the CockroachDB SQL Shell and run:

```sql
-- Check total records (should be ~300k+)
SELECT count(*) FROM florida_cosmetology;

-- Preview data
SELECT licensee_name, city, expiration_date FROM florida_cosmetology LIMIT 5;
