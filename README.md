Strava Elasticsearch Index Analyzer 

A command-line tool that analyzes Elasticsearch index data from
Strava's logging infrastructure and produces actionable reports
on index sizes and shard allocation.

---

## What it does

- **Top 5 largest indexes by size** ( that is human readable GB)
- **Top 5 indexes by shard count**
- **Top 5 shard offenders** ( with recommended shard counts
  based on the 1 shard per 30 GB Elasticsearch best practice )

---

## Requirements

- Python 3.10 or higher
- pip

---

## Installation

### 1. Clone the repository

```bash
Zip file also submitted but can also , clone from  github below.
git clone https://github.com/strava/index-analyzer.git
cd strava-index-analyzer
```

### 2. Create a virtual environment (recommended)

```bash
# Mac/Linux
python -m venv venv
source venv/bin/activate

# Windows : I did in windows, No MacBook :(  
python -m venv venv
venv\Scripts\activate
```

### 3. Install dependencies

```bash
pip install -r requirements.txt
```

---

## Usage

### Debug mode (local file)

Run against the provided `indexes.json` file:

```bash
python main.py --debug
```

Run against a custom file:

```bash
python main.py --debug --file path/to/indexes.json
```

### Live API mode

Run against a live Elasticsearch cluster:

```bash
python main.py --endpoint my-cluster.example.com --days 7
```

### All options
--debug              Run in debug mode using a local JSON file
--file FILE          Path to local JSON file (default: testdata/indexes.json)
--endpoint ENDPOINT  Elasticsearch cluster hostname for live API mode
--days DAYS          Number of past days to fetch (default: 7)
--verbose            Enable verbose logging output
--help               Show help message

---
%%%%%%%%%%%%%%%%%%%%%OUTPUT%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
## Example Output that i got !!!!!!!
======================================================================
STRAVA ELASTICSEARCH INDEX ANALYSIS REPORT
TOP 5 LARGEST INDEXES BY SIZE
RANK      SIZE (GB)  INDEX NAME
1         2521.31 GB  kubernetes.prime-prod.maharaj.2025-04-10
2         1167.76 GB  audit.kubernetes.prime-prod.alumninati.2025-04-07
3          712.53 GB  kubernetes.data-prod.routemaster.2025-04-10
4          277.42 GB  kubernetes.prime-prod.graphql.2025-04-06
5          263.82 GB  kubernetes.prime-staging.horton.2025-04-04
TOP 5 INDEXES BY SHARD COUNT
RANK     SHARDS  INDEX NAME
1           111  kubernetes.prime-staging.moby.2025-04-04
2           100  kubernetes.prime-prod.maharaj.2025-04-10
3            81  kubernetes.prime-prod.meme.2025-04-04
4            71  kubernetes.prime-staging.cortana.2025-04-08
5            67  kubernetes.prime-prod.goldilocks.2025-04-10
TOP 5 SHARD OFFENDERS (too much data per shard)
RANK      SIZE (GB)    CURRENT     RATIO   RECOMMENDED  INDEX NAME
SHARDS  GB/SHARD        SHARDS
1          263.82 GB          1   263.82x             9  kubernetes.prime-staging.horton.2025-04-04
2         1167.76 GB          5   233.55x            39  audit.kubernetes.prime-prod.alumninati.2025-04-07
3          182.01 GB          1   182.01x             7  kubernetes.data-prod.puzzler.2025-04-04
4          148.48 GB          1   148.48x             5  kubernetes.prime-prod.zanclean.2025-04-04
5          712.53 GB          5   142.51x            24  kubernetes.data-prod.routemaster.2025-04-10
======================================================================
END OF REPORT

---

## Running Tests

```bash
pytest tests/ -v
```

Expected output:
29 passed in timelimit of  0.5s

---

## Project Structure
strava-index-analyzer/
├── src/
│   ├── model/
│   │   └── index.py          # Data models
│   ├── fetcher/
│   │   ├── base.py           # Abstract Fetcher interface
│   │   ├── file_fetcher.py   # Reads from local JSON file
│   │   └── api_fetcher.py    # HTTP calls to live Elasticsearch
│   ├── analyzer/
│   │   └── analyzer.py       # Analysis logic
│   └── reporter/
│       └── reporter.py       # Output formatting
├── tests/
│   ├── test_analyzer.py      # Analyzer unit tests 
│   ├── test_fetcher.py       # Fetcher unit tests 
│   └── test_reporter.py      # Reporter unit tests 
├── testdata/
│   └── indexes.json          # Sample data (7 days)
├── main.py                   # CLI entry point
├── requirements.txt          # Dependencies
└── README.md                 # This file

---

## Design Decisions

### SOLID Principles
- **Single Responsibility**: each module has one job
- **Open/Closed**: add new fetchers without changing analyzer
- **Liskov Substitution**: FileFetcher and APIFetcher are interchangeable
- **Interface Segregation**: small focused interfaces
- **Dependency Inversion**: main.py depends on abstractions

### Production Grade API
- Retry with exponential backoff for transient failures
- Separate connect and read timeouts
- Graceful degradation (one bad day skipped, run continues)
- Connection pooling via requests.Session
- Immediate failure for non-retryable errors (401, 403, 404)

### Elasticsearch Shard Strategy
- 1 shard per 30 GB (official Elasticsearch site recommendation)
- Offenders ranked by GB per shard ratio (highest = worst)
- Recommended shards = ceil(size_gb / 30)

---

## Author

Priya 

