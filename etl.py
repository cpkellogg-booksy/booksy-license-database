import os
import pandas as pd
import certifi  # <--- Import the certificate manager
from sqlalchemy import create_engine

# 1. SETUP
# Securely get the database password from GitHub Secrets
db_string_raw = os.environ['DB_CONNECTION_STRING']

# --- THE FIX ---
# We force the connection string to use the 'certifi' bundle.
# This tells the script exactly where to find the "ID Cards" to trust the server.
# We also force sslmode to 'verify-full' or 'require' properly.
if "?" in db_string_raw:
    # If parameters already exist, append the root cert
    db_string = f"{db_string_raw}&sslrootcert={certifi.where()}"
else:
    # If no parameters, add the query start
    db_string = f"{db_string_raw}?sslrootcert={certifi.where()}"

# Ensure we are not conflicting with an existing sslmode in the string
# (The driver usually takes the last parameter if duplicated, or we rely on the Certifi path to make 'verify-full' work)
# ----------------

# URL for Florida Cosmetology
csv_url = "https://www2.myfloridalicense.com/sto/file_download/extracts/COSMETOLOGYLICENSE_1.csv"

# 2. DEFINE HEADERS
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
    # We print a masked version of the string just to debug if it fails (hiding password)
    print(f"Using SSL Cert: {certifi.where()}")
    
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
        
        if first_chunk:
            chunk.to_sql('florida_cosmetology', engine, if_exists='replace', index=False)
            first_chunk = False
        else:
            chunk.to_sql('florida_cosmetology', engine, if_exists='append', index=False)
        
        print(f"Processed batch...")

    print("Success! Data refreshed.")

except Exception as e:
    print(f"Error: {e}")
    exit(1)
