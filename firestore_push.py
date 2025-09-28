import csv
import firebase_admin
from firebase_admin import credentials, firestore

# Path to your service account key
SERVICE_ACCOUNT_FILE = "newservice.json"

# Initialize Firestore DB
cred = credentials.Certificate(SERVICE_ACCOUNT_FILE)
firebase_admin.initialize_app(cred)
db = firestore.client()

# Path to your CSV file
CSV_FILE = "token_last12months_updated.csv"

# Firestore collection name
COLLECTION_NAME = "crypto_data"

def upload_csv_to_firestore():
    with open(CSV_FILE, mode="r", encoding="utf-8") as file:
        reader = csv.DictReader(file)  # Reads rows as dicts
        for row in reader:
            # Add each row as a new document (auto-generated ID)
            db.collection(COLLECTION_NAME).add(row)
            print(f"Uploaded: {row}")

if __name__ == "__main__":
    upload_csv_to_firestore()
    print("CSV data uploaded successfully to Firestore.")
