# Installation

This guide covers setting up the system requirements, database, and the Python environment for SMDT.

::: tip Quick Summary
SMDT requires **Python 3.11+**, **PostgreSQL 14.19+** (with TimescaleDB & PostGIS), and **uv**.
:::

## 1. Prerequisites & System Dependencies

Before installing the Python package, you must set up the database backend.

### Database Setup (PostgreSQL + Extensions)

SMDT relies on a PostgreSQL database with **TimescaleDB** and **PostGIS** extensions enabled.

::: details Click to expand OS-specific installation instructions (macOS, Linux, Windows)

#### macOS (Homebrew)
```bash
# Install PostgreSQL 14
brew install postgresql@14
brew services start postgresql@14
brew link --force postgresql@14

# Install Extensions
# timescaledb
brew install cmake
git clone https://github.com/timescale/timescaledb.git
cd timescaledb
git checkout 2.19.3
./bootstrap -DPG_CONFIG=/opt/homebrew/opt/postgresql@14/bin/pg_config
cd build && make
make install

# postgis
brew install gdal geos proj protobuf-c json-c pkg-config pcre2
curl -O https://download.osgeo.org/postgis/source/postgis-3.4.2.tar.gz
tar -xvzf postgis-3.4.2.tar.gz
cd postgis-3.4.2
./configure --with-pgconfig=/opt/homebrew/opt/postgresql@14/bin/pg_config
sudo mkdir -p /usr/local/bin
sudo ln -s /opt/homebrew/opt/postgresql@14/bin/postgres /usr/local/bin/postgres
sudo make
sudo make install
sudo rm /usr/local/bin/postgres

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

#### Windows (Installer)

Since PostgreSQL on Windows is typically installed via an installer rather than a package manager, follow these steps:

1.  **Install PostgreSQL 14**:
    *   Download the installer from [EnterpriseDB](https://www.enterprisedb.com/downloads/postgres-postgresql-downloads).
    *   Run the installer. Keep the default settings and remember your superuser password.
    *   At the end, ensure **"Launch Stack Builder at exit"** is checked.

2.  **Install PostGIS**:
    *   In **Stack Builder**, select your PostgreSQL 14 installation.
    *   Expand `Spatial Extensions` and check **PostGIS 3.x Bundle for PostgreSQL 14**.
    *   Follow the prompts to install.

3.  **Install TimescaleDB**:
    *   Download the latest `.zip` release for Windows (e.g., `timescaledb-postgresql-14-windows-amd64.zip`) from [TimescaleDB Releases](https://github.com/timescale/timescaledb/releases/tag/2.19.3).
    *   Extract the zip archive.
    *   Run `setup.exe` as Administrator.
    *   Follow prompts to tune configuration.

4.  **Restart Service**:
    *   Open "Services" (Run `services.msc`).
    *   Restart the `postgresql-x64-14` service.

:::

### Initialize the Database

Once PostgreSQL is running, create the database and user, and enable the required extensions.

1.  **Connect to Postgres**:
    ```bash
    psql -U postgres
    ```

    ::: tip
    If the `psql` command is not recognized, ensure that the PostgreSQL `bin` directory is added to your system's `PATH`.
    :::

2.  **Run SQL Setup**:
    ```sql
    -- 1. Create database and a superuser
    CREATE DATABASE smdt_db;
    CREATE USER your_username WITH SUPERUSER PASSWORD 'your_secure_password';
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

