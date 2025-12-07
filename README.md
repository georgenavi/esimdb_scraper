# eSIMDB Scraper

A scraper for collecting eSIM plan data from eSIMDB.com. The scraper collects pricing, data allowances, and validity information across all countries and stores the data in efficient Parquet format with date-based partitioning.

## Table of Contents

- [Features](#features)
- [Project Structure](#project-structure)
- [Setup Instructions](#setup-instructions)
- [Usage](#usage)
- [Technology Stack & Design Decisions](#technology-stack--design-decisions)
- [Data Storage Strategy](#data-storage-strategy)
- [Assumptions & Challenges](#assumptions--challenges)
- [Output Format](#output-format)

## Features

- ✅ **Parallel scraping** - Uses multiprocessing for faster data collection
- ✅ **Robust error handling** - Retry logic with exponential backoff
- ✅ **Data quality validation** - Validates prices, data capacities, and validity periods
- ✅ **Graceful shutdown** - Handles interruption signals properly
- ✅ **Docker support** - Easy deployment with Docker and docker-compose
- ✅ **Incremental consolidation** - Optional data consolidation for time-series analysis
- ✅ **Rate limiting** - Respectful scraping with delays between requests

## Project Structure

```
esimdb_scraper/
├── README.md                 # This file
├── requirements.txt          # Python dependencies
├── .gitignore                # Git ignore rules
├── Dockerfile                # Docker image configuration
├── docker-compose.yml        # Docker compose configuration
├── esumdb_scraper.py         # Main scraper script
├── run.py                    # Entry point with graceful shutdown
├── esimdb_data/              # Raw scraped data (date/country structure)
│   └── YYYYMMDD/
│       ├── france.parquet
│       ├── spain.parquet
│       └── ...
```

## Setup Instructions

### Option 1: Local Setup (Python)

#### Prerequisites
- Python 3.10 or higher
- pip package manager

#### Steps

1. **Clone the repository**
```bash
git clone https://github.com/georgenavi/esimdb_scraper
cd esimdb_scraper
```

2. **Create and activate virtual environment**
```bash
# Create virtual environment
python -m venv venv

# Activate on Linux/Mac
source venv/bin/activate

# Activate on Windows
venv\Scripts\activate
```

3. **Install dependencies**
```bash
pip install -r requirements.txt
```
or
```bash
pip3 install -r requirements.txt
```

4. **Run the scraper**
```bash
cd esimdb_scraper/source
python run.py
```

### Option 2: Docker Setup (Recommended)

#### Prerequisites
- Docker
- Docker Compose

#### Steps

1. **Clone the repository**
```bash
git clone https://github.com/georgenavi/esimdb_scraper
cd esimdb_scraper
```

2. **Build the Docker image**
```bash
docker-compose build
```

3. **Run the scraper**
```bash
# Run in background
docker-compose up -d

# View logs
docker-compose logs -f

# Stop
docker-compose stop
```

## Design Decisions

### Why Pandas over Polars/PySpark?

**Pandas was chosen because:**

1. **Data Scale** - We're dealing with thousands of plans, not billions. Pandas handles this easily.
2. **Parquet Support** - Excellent integration with PyArrow for efficient storage
3. **Development Speed** - Faster to write and debug than PySpark
4. **Simplicity** - No need for Spark cluster overhead for this dataset size

### Architecture Decisions

#### 1. **Multiprocessing over Threading**
- Each country is scraped independently
- ProcessPoolExecutor bypasses Python's GIL
- 5 parallel workers balance speed vs API load

#### 2. **Date-based Partitioning**
- Raw data stored by date: `esimdb_data/YYYYMMDD/country.parquet`
- **Pros**: Easy to find specific dates, simple to delete old data, safe parallel writes
- **Cons**: Harder to analyze trends across dates
- **Solution**: Optional consolidation step for time-series analysis

#### 3. **Parquet Format**
- Columnar storage with Snappy compression (~10x smaller than CSV)
- Fast reads with predicate pushdown
- Schema enforcement prevents data corruption
- Industry standard for data pipelines

#### 4. **Validation Pipeline**
Each data field goes through validation:
- **Prices**: Must be numeric and positive, rounded to 2 decimals
- **Data capacity**: Converted to GB, must be positive
- **Validity period**: Must be positive integer
- **Plan names**: Type checked and whitespace trimmed

#### 5. **Error Handling Strategy**
- Request-level: Retry with exponential backoff (3 attempts)
- Plan-level: Skip bad plans, log warnings, continue processing
- Country-level: Catch exceptions, report failure, continue to next country
- Process-level: Graceful shutdown on SIGTERM/SIGINT

## Data Storage Strategy

### Raw Data Structure
```
esimdb_data/
  20241207/
    france.parquet      # 500 KB
    spain.parquet       # 450 KB
    ...
  20241208/
    france.parquet
    ...
```

**Benefits:**
- No risk of data corruption from concurrent writes
- Each scrape is isolated and traceable
- Easy to re-scrape specific dates
- Simple to implement and debug

### Consolidated Data Structure (Optional)
```
esimdb_data_consolidated/
  all_plans.parquet/
    scrape_date=20241207/
      country=france/
        part-0.parquet
      country=spain/
        part-0.parquet
```

**Benefits:**
- ✅ Fast queries with partition pruning
- ✅ Easy time-series analysis
- ✅ Single source of truth for analytics

### Schema

| Column | Type | Description | Example |
|--------|------|-------------|---------|
| `country` | string | Country name | "France" |
| `region` | string | Geographic region | "Europe" |
| `provider` | string | eSIM provider name | "Airalo" |
| `plan_name` | string | Plan description | "Europe 3GB - 30 Days" |
| `price_usd` | float32 | Price in USD | 12.50 |
| `data_gb` | float32 | Data allowance in GB | 3.0 |
| `validity_days` | int32 | Validity period in days | 30 |
| `scrape_date` | string | Date of scrape | "20241207" |
| `schema_version` | string | Data schema version | "1.0" |

## Assumptions & Challenges

### Assumptions Made

1. **API Stability**
   - Assumed the eSIMDB API structure remains consistent
   - Implemented defensive parsing to handle unexpected responses

2. **Data Units**
   - Assumed capacity without unit specification is in MB for values > 25
   - Added explicit warnings when units are missing or unknown

3. **Rate Limiting**
   - Set 0.2 second delay between page requests per country
   - Assumed 5 parallel workers won't trigger rate limiting

4. **Data Validity**
   - Assumed negative prices and data capacities are errors (set to None)
   - Assumed validity periods > 10 years are suspicious but kept

5. **Character Encoding**
   - Assumed UTF-8 encoding for all text fields
   - Country names may contain special characters

### Challenges Encountered

#### 1. **Inconsistent API Response Structure**
**Problem**: The API sometimes returns data in different formats
- `prices` as dict vs direct `usdPrice` field
- Missing `capacityUnit` field for some plans

**Solution**: Implemented defensive parsing with fallbacks and logging

#### 2. **Data Unit Ambiguity**
**Problem**: Some plans don't specify if capacity is in MB or GB

**Original approach**: Heuristic (capacity ≤ 25 → GB, else MB)
**Improved approach**: Log warning and set to None if unit is missing

#### 3. **Duplicate Plans Across Pages**
**Problem**: Featured plans appear on multiple pages

**Solution**: Track seen plan IDs across all pages for each country

#### 4. **Multiprocessing Session Management**
**Problem**: `requests.Session` objects aren't process-safe

**Solution**: Lazy session creation per process using global variable

#### 7. **Memory Management**
**Problem**: Loading all countries' data at once could use excessive memory

**Solution**: Process countries in parallel but write files individually

## Output Format

### File Naming Convention
```
esimdb_data/YYYYMMDD/country_name.parquet
```

Examples:
- `esimdb_data/20241207/france.parquet`
- `esimdb_data/20241207/united_states.parquet`
- `esimdb_data/20241207/united_arab_emirates.parquet`

### Reading the Data

**Single file:**
```python
import pandas as pd

df = pd.read_parquet('esimdb_data/20241207/france.parquet')
print(df.head())
```

**All countries for a date:**
```python
import pandas as pd
from pathlib import Path

date_dir = Path('esimdb_data/20241207')
dfs = []
for file in date_dir.glob('*.parquet'):
    dfs.append(pd.read_parquet(file))

all_data = pd.concat(dfs, ignore_index=True)
```


## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `PYTHONUNBUFFERED` | 1 | Disable output buffering for real-time logs |
