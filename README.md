# CarsnBids Auction Processor

**CarsnBids Auction Processor** is the second stage in a larger data engineering pipeline built around vehicle auction data from [carsandbids.com](https://carsandbids.com). This module is responsible for **processing raw auction data** stored in Amazon S3, transforming it into a clean, structured format, and uploading the results to both:

- A **processed S3 bucket** for long-term cloud storage

- A **PostgreSQL data warehouse** for querying, analytics, and dashboarding

This component builds upon the [`carsnbids_scraper`](https://github.com/BrianOyollo/carsnbids-scraper) project, which collects raw auction data and stores it in Amazon S3.

## Project Goals

- Transform raw, semi-structured JSON auction files into structured tabular data

- Standardize key attributes (e.g. mileage, price, drivetrain, transmission)

- Load cleaned data into a cloud-based PostgreSQL data warehouse

- Automate the entire process using Apache Airflow

## Tools & Technologies

This project uses the following tools to orchestrate, transform, and load auction data:

| Tool                                              | Purpose                                                                              |
| ------------------------------------------------- | ------------------------------------------------------------------------------------ |
| **[Apache Airflow](https://airflow.apache.org/)** | Orchestrates the ETL pipeline — triggers on new S3 files, manages tasks and DAGs     |
| **Pandas**                                        | Core data processing and transformation library                                      |
| **Psycopg2**                                      | PostgreSQL driver for Python                                                         |
| **Selenium** & **WebDriver Manager**              | (Used for scraping during re-scrape flow) — Automates browser tasks                  |
| **[uv](https://github.com/astral-sh/uv)**         | Fast Python package installer and runner, used in development & automation workflows |

## How It Works

1. **Airflow `S3Sensor`** monitors a specific **S3 prefix** for new raw auction files uploaded by the [scraper module](https://github.com/BrianOyollo/carsnbids-scraper).

2. When a new file is detected, a DAG kicks off the transformation process:
   
   - Reads the raw file (JSON)
   
   - Cleans and standardizes key fields such as:
     
     - `price`, `mileage`, `transmission`, `engine`, `drivetrain`, etc.
     
     - Nested or semi-structured fields like `modifications`, `seller_notes`, `issues`
   
   - **Identifies URLs that failed during scraping** and adds them to a retry queue (re-scraping DAG/task)
   
   - Writes the cleaned auction data as `.json` into the **processed S3 bucket**

3. A separate task  triggers a **re-scraping job** for URLs that previously failed or were incomplete, helping maintain data quality and completeness.

4. Once processed, the cleaned auction data is **loaded into a PostgreSQL data warehouse**, making it available for querying, reporting, and dashboarding.

## Project Structure

```bash
.
├── airflow/
│   ├── dags/
│   │   ├── etl_scripts/
│   │   │   ├── extract.py               # Extracts auction data from S3
│   │   │   ├── transform.py             # Cleans and standardizes raw auction data
│   │   │   └── load.py                  # Loads cleaned data into the PostgreSQL data warehouse
│   │   ├── scraper/
│   │   │   ├── scrape_auction_urls.py   # Scrapes auction listing URLs from carsandbids.com
│   │   │   ├── scrape_auction.py        # Scrapes detailed auction data for each URL
│   │   │   ├── save_auctions.py         # Uploads scraped data to S3
│   │   │   └── setup.py                 # WebDriver setup for scraping
│   │   ├── sql_scripts/
│   │   │   ├── create_tables.sql        # Creates all fact and dimension tables
│   │   │   ├── empty_staging.sql        # Empties the staging table before every load
│   │   │   ├── load_auction_states.sql  # Loads US and Canadian states into state_dim
│   │   │   └── load_tables.sql          # Loads transformed data from staging into fact & dim tables
│   │   └── etl_dag.py                   # Main Airflow DAG that orchestrates the ETL pipeline
│
├── src/
│   ├── notebooks/
│   │   ├── transform.ipynb              # Develop and test transformation logic
│   │   └── load.ipynb                   # Develop and test data loading logic
│   ├── airflow_connections.py          # Sets up Airflow and PostgreSQL connections
│   ├── start_airflow.sh                # Starts Airflow services (with tmux)
│   └── stop_airflow.sh                 # Stops Airflow services
│
├── main.py                             # uv file (ignore)
├── .python-version                     # Python version managed by uv
├── uv.lock                             # uv dependency lock file
```

## Installation

### Prerequisite: Install `uv`

This project uses [`uv`](https://github.com/astral-sh/uv) for fast dependency resolution and environment setup.  
Follow the official guide to install it:  
👉 [UV Installation Docs](https://docs.astral.sh/uv/getting-started/installation/)

### Setup Instructions

1. Fork the repository

```bash
git clone https://github.com/your-username/carsnbids-etl.git
cd carsnbids-etl
```

2. Install dependencies using `uv`

```bash
uv sync
```

3. Export Airflow home directory

```bash
export AIRFLOW_HOME=~/airflow
```

4. Activate the environment

```bash
source .venv/bin/activate
```

5. Create an Airflow user

```bash
 # example
 airflow db migrate
 
 # modify ~/airflow/airflow.cfg
 # commment out the existing auth manager line 
 # add the following below the line
 

 airflow users create \
    --username admin \
    --firstname Peter \
    --lastname Parker \
    --role Admin \
    --email spiderman@superhero.org
```

6. Create your `.env` file

Inside `~/airflow/`, create a file called `.env` and add the necessary values.

```bash
touch ~/airflow/.env

### AWS settings
# AWS_ACCESS_KEY_ID=
# AWS_SECRET_ACCESS_KEY=
# AWS_REGION=
# RAW_AUCTIONS_BUCKET=
# RESCRAPED_AUCTIONS_BUCKET=
# PROCESSED_AUCTIONS_BUCKET=

#### DB SETTINGS 
# DB_USER=
# DB_PASSWORD=
# DB_HOST=
# DB_PORT=
# DB_NAME=
```

7. Configure connections

```bash
cd src
python airflow_connections.py
```

8. Copy DAGs

```bash
cp -r airflow/dags/ ~/airflow/
```

9. Start Airflow

You can either start services manually:

```bash
airflow api-server --port 8080
airflow scheduler
airflow dag-processor
airflow triggerer
```

Or use the helper script:

```bash
cd src
bash start_airflow.sh
```

## Project Structure & Roadmap

This repository is a continuation of the [carsnbids_scraper](https://github.com/BrianOyollo/carsnbids_scraper) module, which was responsible for collecting raw auction data from [carsandbids.com](https://carsandbids.com). That module ran independently for over a year, building up a sizable archive of raw auction files in S3.

This phase focuses on:

- **Transforming** the raw data

- **Loading** it into a structured data warehouse

- Leveraging **Airflow** for orchestration

- Automating EC2 lifecycle based on file presence and processing events

## What's Next?

The broader goal is to build a **production-style, end-to-end data engineering pipeline** using varied tools and cloud-native concepts. Future versions will explore other data engineering tools and skills, such as Apache Spark, containerization, dbt, ...

## Educational Use

This project is **strictly for educational purposes** — designed to practice and demonstrate data engineering skills.

It is **not affiliated with carsandbids.com.**

## Contact

Have questions or feedback? Reach out at oyollobrian@gmail.com
