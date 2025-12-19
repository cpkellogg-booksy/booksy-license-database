import os
import pandas as pd
from sqlalchemy import create_engine

# 1. SETUP
# Securely get the database password from GitHub Secrets
db_string = os.environ['DB_CONNECTION_STRING']

# URL for Florida Cosmetology (We can add Texas/US here later)
csv_url = "https://www2.myfloridalicense.com/sto/file_download/extracts/COSMETOLOGYLICENSE_1.csv"

# 2. DEFINE HEADERS (Specific to Florida Cosmetology file)
custom_headers = [
    "board_number", "occupation_code", "licensee_name", "doing_business_as_name",
    "class_code", "address_line_1", "address_line_2", "address_line_3",
    "city", "state", "zip", "county_code", "license_number",
    "primary_status", "secondary_status", "original_licensure_date",
    "effective_date", "expiration_date", "blank_column", "renewal_period",
    "alternate_lic_number", "ce_exemption"
]

# 3. CONNECT & UPLOAD
try:
    print("Connecting to CockroachDB...")
    engine = create_engine(db_string)

    print(f"Downloading data from {csv_url}...")
    chunk_size = 10000
    first_chunk = True

    for chunk in pd.read_csv(csv_url, 
                             chunksize=chunk_size, 
                             header=None,
                             names=custom_headers,
                             storage_options={'User-Agent': 'Mozilla/5.0'},
                             encoding='ISO-8859-1',
                             on_bad_lines='skip'):

        # 'replace' wipes the table clean on the first chunk to prevent duplicates
        if first_chunk:
            chunk.to_sql('florida_cosmetology', engine, if_exists='replace', index=False)
            first_chunk = False
        else:
            chunk.to_sql('florida_cosmetology', engine, if_exists='append', index=False)

        print(f"Processed batch...")

    print("Success! Data refreshed.")

except Exception as e:
    print(f"Error: {e}")
    exit(1) # Marks the GitHub Action as failed
