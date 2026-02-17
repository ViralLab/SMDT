# Installation

This guide covers setting up the system requirements, database, and the Python environment for SMDT.

::: tip Quick Summary
SMDT requires **Python 3.11+**, **PostgreSQL 14.19+** (with TimescaleDB & PostGIS), and **uv**.
:::

## 1. Prerequisites & System Dependencies

Before installing the Python package, you must set up the database backend.

### Database Setup (PostgreSQL + Extensions)

SMDT relies on a PostgreSQL database with **TimescaleDB** and **PostGIS** extensions enabled.

::: details Click to expand OS-specific installation instructions (macOS, Linux)

#### macOS (Homebrew)
```bash
# Install PostgreSQL 14
brew install postgresql@14
brew services start postgresql@14
brew link --force postgresql@14

# Install Extensions
brew tap timescale/tap
brew install timescaledb
timescaledb-tune --quiet --yes

brew install postgis

# Restart Service
brew services restart postgresql@14
```

#### Linux (Ubuntu/Debian)
```bash
# Add Postgres Repo
sudo sh -c 'echo "deb http://apt.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg main" > /etc/apt/sources.list.d/pgdg.list'
wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | sudo apt-key add -
sudo apt update

# Install Postgres 14
sudo apt install postgresql-14
sudo systemctl start postgresql
sudo systemctl enable postgresql

# Install TimescaleDB
sudo add-apt-repository ppa:timescale/timescaledb-ppa
sudo apt-get update
sudo apt install timescaledb-2-postgresql-14
sudo timescaledb-tune --quiet --yes

# Install PostGIS
sudo apt install postgresql-14-postgis-3
sudo systemctl restart postgresql
```
:::

### Initialize the Database

Once PostgreSQL is running, create the database and user, and enable the required extensions.

1.  **Connect to Postgres**:
    ```bash
    psql -U postgres
    ```

2.  **Run SQL Setup**:
    ```sql
    -- 1. Create database and user
    CREATE DATABASE smdt_db;
    CREATE USER smdt_user WITH ENCRYPTED PASSWORD 'secure_password';
    GRANT ALL PRIVILEGES ON DATABASE smdt_db TO smdt_user;

    -- 2. Connect to the new database
    \c smdt_db

    -- 3. Enable Extensions (Order matters!)
    CREATE EXTENSION IF NOT EXISTS timescaledb;
    CREATE EXTENSION IF NOT EXISTS postgis;

    -- 4. Verify installation
    \dx
    ```

---

## 2. Project Installation

We use [uv](https://docs.astral.sh/uv/) for fast Python package management.

1.  **Clone the Repository**
    ```bash
    git clone https://github.com/ViralLab/SMDT.git
    cd SMDT
    ```

2.  **Initialize Environment**
    Run `uv sync` to create the virtual environment and install dependencies defined in `pyproject.toml`.
    
    ```bash
    uv sync
    ```

3.  **Activate the Environment**
    
    ```bash
    source .venv/bin/activate
    # On Windows: .venv\Scripts\activate
    ```

---

## 3. Configuration

SMDT reads configuration from environment variables.

1.  **Create `.env` file** in the project root:
    ```bash
    touch .env
    ```

2.  **Add Database Credentials**:
    
    ```bash
    # .env
    DEFAULT_DB_NAME=smdt_db
    DB_USER=smdt_user
    DB_PASSWORD=secure_password
    DB_HOST=localhost
    DB_PORT=5432
    ```

## 4. Verify Installation

To ensure everything is working, you can run a quick check:

```bash
uv run python -c "import smdt; print('SMDT installed successfully!')"
```

You are now ready to start using SMDT! Check out the [Recipes](../recipes/) to get started.

