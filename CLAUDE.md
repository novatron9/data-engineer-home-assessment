# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This repository contains data engineering assessment materials and datasets for analyzing telecommunications data records. The primary focus is on IPDR (IP Detail Record) data processing and analysis for big data integration projects.

## Key Components

### Data Files
- `ipdr.csv` - IP Detail Record dataset containing telecommunications usage data for analysis
- PDF documentation for Data Engineer positions and assessments

### Data Analysis Context
The IPDR data typically contains:
- Network usage records
- IP traffic details
- Billing and metering information
- Session data for telecommunications services

## Development Environment

### Data Processing
- Primary language: Python (recommended for IPDR analysis)
- Key libraries: `pandas`, `numpy`, `matplotlib`, `seaborn` for data analysis
- For big data processing: Consider `pyspark`, `dask`, or similar frameworks
- CSV processing: Use `pandas.read_csv()` with appropriate parsing for large datasets

### Common Commands
```bash
# Analyze IPDR data structure
python -c "import pandas as pd; df = pd.read_csv('ipdr.csv'); print(df.info()); print(df.head())"

# Basic data profiling
python -c "import pandas as pd; df = pd.read_csv('ipdr.csv'); print(df.describe()); print(df.isnull().sum())"
```

## Data Engineering Best Practices

### IPDR Processing
- Handle large CSV files with chunked reading for memory efficiency
- Validate data quality and completeness
- Consider time-series analysis for usage patterns
- Implement data cleansing for telecommunications data formats
- Parse datetime fields appropriately for temporal analysis

### Performance Considerations
- Use efficient data types for large datasets
- Consider parallel processing for complex transformations
- Implement data validation and error handling
- Monitor memory usage when processing large IPDR files