import os
import logging
import psycopg2
import pandas as pd
import re
from dotenv import load_dotenv
from tqdm import tqdm

load_dotenv()

logging.basicConfig(
    filename='YT_pipeline_log_file.log',
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

# --- CONFIGURATION: Define therapy area configs ---
THERAPY_CONFIGS = {
    1: {  # batch_id 1 = Oncology
        'keyword_file': 'oncology_keywords.csv'
    },
    2: {  # batch_id 2 = Dermatology
        'keyword_file': 'dermatology_keywords.csv'
    },
    3: {  # batch_id 3 = Neurology
        'keyword_file': 'neurology_keywords.csv'
    },
    4: {  # batch_id 4 = Cardiology
        'keyword_file': 'cardiology_keywords.csv'
    },
    5: {  # batch_id 5 = Endocrinology
        'keyword_file': 'endocrinology_keywords.csv'
    }
}

def connect_to_db():
    """DB connection using environment variables with validation."""
    db_config = {
        'database': os.getenv('DB_NAME'),
        'user': os.getenv('DB_USER'),
        'password': os.getenv('DB_PASSWORD'),
        'host': os.getenv('DB_HOST'),
        'port': os.getenv('DB_PORT'),
    }
    try:
        port = int(db_config['port'])
    except (TypeError, ValueError):
        logging.warning(f"Invalid or missing DB_PORT: {db_config['port']}, defaulting to 5432")
        port = 5432

    conn_str = (
        f"dbname='{db_config['database']}' "
        f"user='{db_config['user']}' "
        f"password='{db_config['password']}' "
        f"host='{db_config['host']}' "
        f"port='{port}'"
    )
    logging.info("Connecting to DB")
    return psycopg2.connect(conn_str)

def load_data(conn, batch_id):
    """Fetch raw youtube data from DB for specific batch_id."""
    # query = f"""
    # SELECT * FROM auxo_master.youtube_tagged_data_v1 WHERE batch_id = {batch_id};
    # """
    query = f"""
    SELECT * FROM recycle_bin.youtube_scraped_data;
    """
    df = pd.read_sql(query, conn)
    logging.info(f"Fetched {len(df)} rows from DB for batch_id={batch_id}")
    print(f"Data loaded: {len(df)} rows for batch_id={batch_id}")
    return df

def load_keywords(filename):
    """Load keywords from CSV segregated into categories."""
    keywords_df = pd.read_csv(filename)
    generic_terms = keywords_df['Generic'].dropna().str.split(',').explode().str.strip().str.lower().tolist()
    specialised_terms = keywords_df['Specialised'].dropna().str.split(',').explode().str.strip().str.lower().tolist()
    abbreviations = keywords_df['Abbreviations'].dropna().str.split(',').explode().str.strip().tolist()
    logging.info(f"Loaded keywords from {filename}")
    return generic_terms, specialised_terms, abbreviations

def calculate_tag_score(text, generic_terms, specialised_terms, abbreviations):
    """
    Return tag score and matched keywords based on updated logic:
    1 = only generic found (less than 3),
    2 = generic + specialised OR generic + abbreviation OR only specialised found,
    3 = only abbreviation found,
    4 = only generic found and >= 3 generic keywords,
    0 = none found.
    Also returns list of matched keywords from any category.
    """
    text_lower = text.lower()
    matched_keywords = []

    found_generic = any(term and (term in text_lower) for term in generic_terms)
    if found_generic:
        matched_generic = [term for term in generic_terms if term and term in text_lower]
        matched_keywords.extend(matched_generic)

    found_specialised = any(term and (term in text_lower) for term in specialised_terms)
    if found_specialised:
        matched_spec = [term for term in specialised_terms if term and term in text_lower]
        matched_keywords.extend(matched_spec)

    found_abbreviation = any(
        abbr and re.search(rf"\b{re.escape(abbr)}\b", text) for abbr in abbreviations
    )
    if found_abbreviation:
        matched_abbr = [abbr for abbr in abbreviations if abbr and re.search(rf"\b{re.escape(abbr)}\b", text)]
        matched_keywords.extend(matched_abbr)

    # Count matched generic keywords
    num_matched_generic = len(matched_generic) if found_generic else 0

    # Apply updated scoring logic
    if found_generic and not (found_specialised or found_abbreviation):
        if num_matched_generic >= 3:
            score = 4
        else:
            score = 1
    elif (found_generic and (found_specialised or found_abbreviation)) or (found_specialised and not found_generic and not found_abbreviation):
        score = 2
    elif found_abbreviation and not found_generic and not found_specialised:
        score = 3
    else:
        score = 0

    return score, matched_keywords

def process_batch(conn, batch_id, config):
    """Process a single batch with its specific configuration."""
    print(f"\n{'='*60}")
    print(f"Processing Batch ID: {batch_id}")
    print(f"Keyword File: {config['keyword_file']}")
    print(f"{'='*60}\n")
    
    # Load data and keywords
    df = load_data(conn, batch_id)
    generic_terms, specialised_terms, abbreviations = load_keywords(config['keyword_file'])

    # Combine text fields for keyword search
    df['combined_text'] = df[['title', 'description', 'tags']].fillna("").agg(" ".join, axis=1)

    tqdm.pandas(desc=f"Scoring videos (batch {batch_id})")
    
    # Apply new tagging logic, get score and matched keywords
    results = df['combined_text'].progress_apply(
        lambda x: calculate_tag_score(x, generic_terms, specialised_terms, abbreviations)
    )
    # Unpack score and matched keywords into two new columns
    df[['tag_score', 'matched_keywords']] = pd.DataFrame(results.tolist(), index=df.index)
    df['is_tagged'] = df['tag_score'] > 0

    # Print results for all videos including those with zero score
    for idx, row in df.iterrows():
        video_id = row['video_id'] if 'video_id' in row else idx
        print(f"Video ID: {video_id} - Score: {row['tag_score']}, Matched Keywords: {row['matched_keywords']}")

    print(f"\nBatch {batch_id} Results:")
    print(f"  Total videos: {len(df)}")
    print(f"  Tagged videos (score > 0): {len(df[df['is_tagged']])}")
    print(f"  Not tagged videos (score = 0): {len(df[df['tag_score'] == 0])}")

    # Save all results including keywords in CSV
    output_file = f"youtube_tagged_batch_{batch_id}_with_keywords.csv"
    df.to_csv(output_file, index=False)
    print(f"  Saved detailed results including keywords to: {output_file}\n")
    
    return df

def main():
    """Main pipeline"""
    logging.info("Pipeline started")
    conn = connect_to_db()

    batch_ids_to_process = [1]
    all_batches = []

    for batch_id in batch_ids_to_process:
        if batch_id not in THERAPY_CONFIGS:
            print(f"Warning: No configuration found for batch_id {batch_id}, skipping...")
            logging.warning(f"No configuration for batch_id {batch_id}")
            continue
        config = THERAPY_CONFIGS[batch_id]
        batch_df = process_batch(conn, batch_id, config)
        all_batches.append(batch_df)

    if all_batches:
        combined = pd.concat(all_batches, ignore_index=True)
        combined.to_csv("youtube_all_batches_tagged_with_keywords.csv", index=False)
        print(f"\nTotal tagged videos across all batches: {len(combined)}")
        logging.info(f"Total tagged videos: {len(combined)}")

    conn.close()
    logging.info("Pipeline completed successfully")

if __name__ == "__main__":
    main()
