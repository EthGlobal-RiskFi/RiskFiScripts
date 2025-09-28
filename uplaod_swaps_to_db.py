import pandas as pd
import firebase_admin
from firebase_admin import credentials, firestore
from datetime import datetime, timezone
import sys
import json
import math
from typing import Any, Dict
import time
import random

# Configure UTF-8 output
sys.stdout.reconfigure(encoding='utf-8')

class FirestoreUploader:
    def __init__(self, service_account_path: str, collection_name: str = "swaps"):
        """
        Initialize Firestore uploader
        
        Args:
            service_account_path: Path to your Firebase service account JSON file
            collection_name: Name of the Firestore collection to store swaps
        """
        self.collection_name = collection_name
        self.db = None
        self.batch_size = 100  # Reduced batch size for better reliability
        self.max_retries = 3
        self.base_delay = 1.0
        
        try:
            # Initialize Firebase Admin SDK
            cred = credentials.Certificate(service_account_path)
            firebase_admin.initialize_app(cred)
            self.db = firestore.client()
            print(f"✅ Connected to Firestore successfully")
        except Exception as e:
            print(f"❌ Failed to initialize Firestore: {e}")
            sys.exit(1)
    
    def clean_data(self, value: Any) -> Any:
        """Clean data for Firestore compatibility"""
        if pd.isna(value) or value is None:
            return None
        elif isinstance(value, float):
            if math.isnan(value) or math.isinf(value):
                return None
            return value
        elif isinstance(value, pd.Timestamp):
            return value.to_pydatetime()
        elif isinstance(value, str):
            return value.strip() if value.strip() else None
        else:
            return value
    
    def prepare_document(self, row: pd.Series) -> Dict[str, Any]:
        """Convert pandas row to Firestore document"""
        doc = {}
        
        # Core swap information
        doc['id'] = self.clean_data(row['id'])
        doc['timestamp'] = self.clean_data(row['timestamp'])
        doc['tx'] = self.clean_data(row['tx'])
        doc['logIndex'] = int(self.clean_data(row['logIndex'])) if pd.notna(row['logIndex']) else None
        
        # Participants
        doc['sender'] = self.clean_data(row['sender'])
        doc['recipient'] = self.clean_data(row['recipient'])
        doc['origin'] = self.clean_data(row['origin'])
        
        # Token information
        doc['token0'] = {
            'symbol': self.clean_data(row['token0_symbol']),
            'address': self.clean_data(row['token0_id'])
        }
        doc['token1'] = {
            'symbol': self.clean_data(row['token1_symbol']),
            'address': self.clean_data(row['token1_id'])
        }
        
        # Amounts (convert to float, handle scientific notation)
        doc['amounts'] = {
            'amount0': float(self.clean_data(row['amount0'])) if pd.notna(row['amount0']) else None,
            'amount1': float(self.clean_data(row['amount1'])) if pd.notna(row['amount1']) else None,
            'amountUSD': float(self.clean_data(row['amountUSD'])) if pd.notna(row['amountUSD']) else None
        }
        
        # Price and tick information
        doc['priceInfo'] = {
            'sqrtPriceX96': self.clean_data(row['sqrtPriceX96']),
            'tick': int(self.clean_data(row['tick'])) if pd.notna(row['tick']) else None
        }
        
        # Pool information
        doc['pool'] = {
            'id': self.clean_data(row['pool_id']),
            'liquidity': self.clean_data(row['pool_liquidity']),
            'volumeUSD': float(self.clean_data(row['pool_volumeUSD'])) if pd.notna(row['pool_volumeUSD']) else None,
            'txCount': int(self.clean_data(row['pool_txCount'])) if pd.notna(row['pool_txCount']) else None
        }
        
        # Add metadata
        doc['uploadedAt'] = datetime.now(datetime.UTC)
        doc['tokenPair'] = f"{doc['token0']['symbol']}-{doc['token1']['symbol']}"
        
        return doc
    
    def upload_batch_with_retry(self, documents: list, batch_num: int) -> bool:
        """Upload a batch with exponential backoff retry logic"""
        for attempt in range(self.max_retries):
            try:
                batch = self.db.batch()
                
                for doc_data in documents:
                    # Use the swap ID as document ID for uniqueness
                    doc_id = doc_data['id']
                    doc_ref = self.db.collection(self.collection_name).document(doc_id)
                    batch.set(doc_ref, doc_data)
                
                batch.commit()
                print(f"  ✅ Batch {batch_num}: Uploaded {len(documents)} documents")
                return True
                
            except Exception as e:
                error_msg = str(e)
                print(f"  ⚠️  Batch {batch_num}, attempt {attempt + 1}/{self.max_retries} failed: {error_msg}")
                
                if attempt < self.max_retries - 1:  # Don't wait after last attempt
                    # Exponential backoff with jitter
                    delay = self.base_delay * (2 ** attempt) + random.uniform(0, 1)
                    print(f"  ⏳ Retrying in {delay:.1f} seconds...")
                    time.sleep(delay)
                else:
                    print(f"  ❌ Batch {batch_num} permanently failed after {self.max_retries} attempts")
                    # Try individual documents if batch fails
                    return self.upload_documents_individually(documents, batch_num)
        
        return False
    
    def upload_documents_individually(self, documents: list, batch_num: int) -> bool:
        """Fallback: Upload documents one by one if batch fails"""
        print(f"  🔄 Attempting individual uploads for batch {batch_num}...")
        successful = 0
        
        for i, doc_data in enumerate(documents):
            try:
                doc_id = doc_data['id']
                doc_ref = self.db.collection(self.collection_name).document(doc_id)
                doc_ref.set(doc_data)
                successful += 1
                
                # Small delay between individual uploads
                if i % 10 == 0:  # Every 10 documents
                    time.sleep(0.1)
                    
            except Exception as e:
                print(f"    ❌ Failed to upload document {doc_data.get('id', 'unknown')}: {e}")
        
        print(f"  📊 Individual upload: {successful}/{len(documents)} documents succeeded")
        return successful > 0
    
    def upload_csv(self, csv_file_path: str, dry_run: bool = False) -> None:
        """
        Upload CSV data to Firestore
        
        Args:
            csv_file_path: Path to the CSV file
            dry_run: If True, only validate data without uploading
        """
        print(f"Reading CSV file: {csv_file_path}")
        
        try:
            # Read CSV with proper data types
            df = pd.read_csv(csv_file_path, encoding='utf-8')
            
            # Convert timestamp column to datetime
            if 'timestamp' in df.columns:
                df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True)
            
            print(f"📊 Loaded {len(df)} records from CSV")
            print(f"📅 Date range: {df['timestamp'].min()} to {df['timestamp'].max()}")
            
        except Exception as e:
            print(f"❌ Failed to read CSV: {e}")
            return
        
        if dry_run:
            print("🧪 DRY RUN MODE - Validating data structure...")
            sample_doc = self.prepare_document(df.iloc[0])
            print("Sample document structure:")
            print(json.dumps(sample_doc, indent=2, default=str))
            return
        
        # Prepare documents
        print("🔄 Preparing documents for upload...")
        documents = []
        failed_rows = []
        
        for idx, row in df.iterrows():
            try:
                doc = self.prepare_document(row)
                documents.append(doc)
            except Exception as e:
                failed_rows.append((idx, str(e)))
                print(f"⚠️  Failed to prepare row {idx}: {e}")
        
        if failed_rows:
            print(f"⚠️  {len(failed_rows)} rows failed preparation")
        
        print(f"📤 Uploading {len(documents)} documents to Firestore collection '{self.collection_name}'...")
        
        # Upload in batches
        successful_batches = 0
        failed_batches = 0
        total_uploaded = 0
        
        print(f"📦 Using batch size: {self.batch_size} documents")
        print(f"🔁 Max retries per batch: {self.max_retries}")
        
        for i in range(0, len(documents), self.batch_size):
            batch_docs = documents[i:i + self.batch_size]
            batch_num = (i // self.batch_size) + 1
            total_batches = math.ceil(len(documents) / self.batch_size)
            
            print(f"📦 Processing batch {batch_num}/{total_batches} ({len(batch_docs)} docs)...")
            
            if self.upload_batch_with_retry(batch_docs, batch_num):
                successful_batches += 1
                total_uploaded += len(batch_docs)
            else:
                failed_batches += 1
            
            # Progressive delay - longer delays for later batches to avoid rate limiting
            if batch_num % 10 == 0:  # Every 10 batches
                delay = min(2.0, 0.5 + (batch_num / 100))  # Max 2 seconds
                print(f"  ⏳ Rate limiting break: {delay:.1f}s...")
                time.sleep(delay)
            else:
                time.sleep(0.2)  # Small delay between batches
        
        # Summary
        print(f"\n📈 Upload Summary:")
        print(f"  Total records processed: {len(df)}")
        print(f"  Successfully uploaded: {total_uploaded}")
        print(f"  Successful batches: {successful_batches}")
        print(f"  Failed batches: {failed_batches}")
        print(f"  Failed row preparations: {len(failed_rows)}")
        
        if total_uploaded > 0:
            print(f"✅ Upload completed! Check your Firestore console.")
        else:
            print(f"❌ No data was uploaded.")

def main():
    # Configuration
    SERVICE_ACCOUNT_PATH = "service.json"  # Update this path
    CSV_FILE_PATH = "target_token_swaps_last6months.csv"  # Update if your filename is different
    COLLECTION_NAME = "token_swaps"  # Firestore collection name
    DRY_RUN = False  # Set to True to test without uploading
    
    print("🔥 Firestore CSV Uploader")
    print("=" * 50)
    
    # Validate inputs
    if SERVICE_ACCOUNT_PATH == "path/to/your/firebase-service-account.json":
        print("❌ Please update SERVICE_ACCOUNT_PATH with your actual Firebase service account JSON file path")
        return
    
    try:
        # Initialize uploader
        uploader = FirestoreUploader(SERVICE_ACCOUNT_PATH, COLLECTION_NAME)
        
        # Start upload automatically
        print(f"📋 Starting upload to collection '{COLLECTION_NAME}'...")
        uploader.upload_csv(CSV_FILE_PATH, dry_run=DRY_RUN)
            
    except KeyboardInterrupt:
        print("\n🛑 Upload interrupted by user")
    except Exception as e:
        print(f"❌ Unexpected error: {e}")

if __name__ == "__main__":
    main()