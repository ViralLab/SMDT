# Getting Started

This guide walks you through the initial steps of using the SMDT package.

## 1. Environment Verification

Ensure that you have installed the package and that your Python environment is active.

```bash
uv run python -c "import smdt; print('SMDT installed successfully')"
```

## 2. Basic Configuration

Make sure your `.env` file is set up correctly in the root of your project:

```env
DEFAULT_DB_NAME=smdt_db
DB_USER=postgres
DB_PASSWORD=your_password
DB_HOST=localhost
DB_PORT=5432
```

## 3. Using Standardizers

SMDT provides standardizers to clean and normalize data from various social media platforms. Here is an example using the `TwitterUSCStandardizer`.

```python
from smdt.standardizers import TwitterUSCStandardizer

# Initialize the standardizer
standardizer = TwitterUSCStandardizer()

# Example raw data (this will vary based on the platform)
raw_tweet = {
    "text": "Hello world! #SMDT @ViralLab",
    "created_at": "2023-10-27T10:00:00Z",
    "id": "1234567890"
}

# Standardize the data
# (Note: Specific usage depends on the standardizer implementation)
# standardized_data = standardizer.standardize(raw_tweet)
# print(standardized_data)
```

## 4. Connecting to the Database

You can connect to your TimescaleDB-enabled PostgreSQL database using `StandardDB`.

```python
from smdt.store.standard_db import StandardDB

# Initialize database connection
# It automatically reads configuration from environment variables
db = StandardDB()

try:
    # Check connection
    conn = db.connect() 
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
            print("Database connection successful!")
    finally:
        conn.close()
except Exception as e:
    print(f"Database connection failed: {e}")
```

## Next Steps

Explore the API documentation for more details on:
- [Standardizers](../api/standardizers/smdt.standardizers.base.md)
- [Database Models](../api/store/smdt.store.standard_db.md)
