# Pharmacy Data Pipeline

## Overview

This project implements a data pipeline in Python to process pharmacy claim and revert events. It ingests raw data from multiple files, performs transformations and aggregations, and calculates key business metrics as specified in the technical assessment.

The solution is designed following best practices, emphasizing modularity, parameterization, and performance on a multi-core machine.

## Architectural Approach

The pipeline is executed in distinct phases, respecting the technical constraints of the challenge:

1.  **Goals 1 & 2 (Pure Python/Pandas):** Data is loaded from source files in parallel using Python's `multiprocessing` to maximize efficiency. Initial transformations and the first set of KPIs are calculated in-memory using the Pandas library.
2.  **Goals 3 & 4 (PySpark):** For more complex analytics, the cleaned data is processed using a local Spark session. This demonstrates the ability to leverage distributed computing frameworks for advanced aggregations and ranking using window functions.

## Project Structure
```
├── data/
│   ├── pharmacies/
│   ├── claims/
│   └── reverts/
├── output/
├── src/
│   ├── config.py
│   ├── data_loader.py
│   ├── metrics_calculator.py
│   └── analytics_engine.py
├── main.py
└── requirements.txt
```



## How to Run

### Prerequisites
- Python 3.9+
- An Apache Spark installation and a compatible Java Development Kit (JDK). The recommended approach is to use **Conda/Miniconda**, which can manage the Python, PySpark, and Java dependencies within an isolated environment.

### 1. Setup Environment
First, create a Python virtual environment and install the required dependencies.
```bash
# Create and activate a virtual environment
python3 -m venv venv
source venv/bin/activate

# Install dependencies, including pyspark
pip install -r requirements.txt
```

### 2. Place Data Files
This project reads data from the `data/` directory. **You must place the provided sample files into the corresponding subdirectories:**
- Place the pharmacy `.csv` file inside `data/pharmacies/`.
- Place all `claims` `.json` files inside `data/claims/`.
- Place all `reverts` `.json` files inside `data/reverts/`.

### 3. Run the Pipeline
Execute the main script from the project's root directory, providing the paths to the data folders as arguments.
```bash
python main.py \
    --pharmacies data/pharmacies/ \
    --claims data/claims/ \
    --reverts data/reverts/
```


### Expected Output
The script will process the data and generate three JSON files inside the output/ directory, corresponding to the results of Goal 2, Goal 3, and Goal 4.


