# Booksy License Database (ETL & Mapping Pipeline)

This repository hosts a "headless" ETL (Extract, Transform, Load) pipeline that automatically aggregates professional license data. It currently pulls **Florida and Texas** Cosmetology and Barber licenses, cleans the addresses using AI, segments them into Commercial vs. Residential locations, and generates an interactive map file.

The system is designed to be **free, open-source, and scalable**, running entirely on GitHub Actions and CockroachDB Serverless.

## 🏗 Architecture

* **Sources:**
    * [Florida DBPR Cosmetology Extract](https://www2.myfloridalicense.com/sto/file_download/extracts/COSMETOLOGYLICENSE_1.csv) (CSV)
    * [Florida DBPR Barber Extract](https://www2.myfloridalicense.com/sto/file_download/extracts/lic03bb.csv) (CSV)
    * [Texas TDLR Open Data API](https://data.texas.gov/resource/7358-krk7.json) (Socrata API)
* **Orchestration:** **GitHub Actions** (Runs automatically every day at 8:00 AM UTC).
* **Processing:** **Python 3.9** (Pandas, SQLAlchemy, USAddress, Requests).
* **Geocoding:** **US Census Bureau Batch API** (Free & High Volume) + **Smart Caching**.
* **Storage:** **CockroachDB Serverless** (PostgreSQL-compatible).

## 🚀 Automation Flow

The pipeline consists of two distinct stages that run sequentially:

### Stage 1: The "Factory" (`etl.py`)
1.  **Extract:**
    * Downloads raw CSVs for Florida Cosmetology and Barbers.
    * Streams active license data from the Texas API (filtering for Barbers, Cosmetologists, Salons, and Shops).
2.  **Transform:**
    * **Universal Adapter:** Normalizes the different schemas (FL CSV columns vs. TX API JSON) into a unified format.
    * **AI Cleaning:** Uses `usaddress` and custom Regex to fix "Ghost Data" (duplicate street names) and "Floating Suites."
    * **Segmentation:** Classifies every location as **Commercial** (Salons, Suites, Malls) or **Residential** (Home-based) based on license density, explicit keywords, and unit types.
3.  **Load:** Merges all data into a single `address_insights_gold` table in CockroachDB.

### Stage 2: The "Mapper" (`map_gen.py`)
1.  **Fetch:** Reads the clean `address_insights_gold` data from the database.
2.  **Smart Cache Check:** Checks a permanent `geo_cache` table to see if the address has already been geocoded in a previous run.
3.  **Geocode (Incremental):** Sends *only* new, unseen addresses to the **US Census Bureau Batch Geocoder** to retrieve Latitude/Longitude coordinates.
4.  **Save:** Updates the cache with new results and generates the final output.
5.  **Artifact:** Uploads a `Booksy_License_Database.csv` file to GitHub Actions (ready for [Kepler.gl](https://kepler.gl)).

## 🛠 Setup & Deployment

### 1. Database Setup (CockroachDB)
Create a free Serverless cluster at [cockroachlabs.com](https://cockroachlabs.com). Get your connection string.

### 2. GitHub Secrets
Go to **Settings > Secrets and variables > Actions**.
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
    pip install pandas sqlalchemy psycopg2-binary sqlalchemy-cockroachdb certifi usaddress requests
    ```

3.  Set the environment variable and run:
    ```bash
    # Mac/Linux
    export DB_CONNECTION_STRING="your_cockroach_db_string"
    
    # Windows
    $env:DB_CONNECTION_STRING="your_cockroach_db_string"

    # Run the ETL (Factory)
    python etl.py
    
    # Run the Map Generator (Mapper)
    python map_gen.py
    ```

## 📊 Data Schema (`address_insights_gold`)

The final output table aggregates licenses by physical location:

| Column Name | Description |
| :--- | :--- |
| `address_clean` | The AI-standardized street address |
| `city_clean` | Standardized City |
| `state` | State Code (FL or TX) |
| `address_type` | **Commercial** or **Residential** |
| `total_licenses` | Total active licenses at this location |
| `count_barber` | Number of Barbers |
| `count_cosmetologist` | Number of Cosmetologists |
| `count_salon` | Number of Salon Licenses |
| `count_barbershop` | Number of Barber Shop Licenses |
| `count_owner` | Number of Owner Licenses |

## 🗺 Visualization

To visualize the data:
1.  Go to the **Actions** tab in this repository.
2.  Click on the latest **Daily Data Refresh** run.
3.  Scroll down to **Artifacts** and download **Booksy_License_Database**.
4.  Unzip the file and drag the CSV into [Kepler.gl](https://kepler.gl/demo).
