import json
from smdt.store.standard_db import StandardDB

# Configuration
db_name = "twitter_ukraine_russia"  
SAMPLE_SIZE = 58_508
OUTPUT_DIR = "/chistera/CaseStudyOutputs/russia_ukraine_war/twitter/"

# Initialize database access
db = StandardDB(db_name=db_name, initialize=False)

for c in range(1, 31):
    output_path = f"{OUTPUT_DIR}russia_ukraine_war_posts_twitter_sample_{c}.json"
    
    query = f"""
        SELECT row_to_json(posts)
        FROM posts
        ORDER BY RANDOM()
        LIMIT {SAMPLE_SIZE};
    """
    
    result = db.execute(query) 
    
    with open(output_path, "w", encoding="utf-8") as out_file:
        for row in result:
            json_record = row[0] if isinstance(row, (tuple, list)) else row['row_to_json']
            out_file.write(json.dumps(json_record) + "\n")

    print(f"Sample {c}: {SAMPLE_SIZE} records extracted from Postgres 'posts' table and saved to {output_path}")