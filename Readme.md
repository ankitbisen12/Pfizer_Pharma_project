# Pfizer Pharma Data Pipeline Project

## 🏢 Project Overview

This project implements a comprehensive data pipeline for Pfizer pharmaceutical operations using **Databricks** and **Delta Lake** with a **Medallion Architecture**. The system processes clinical trials data, manufacturing information, and quality control results through bronze, silver, and gold layers for data refinement and analytics.

## 🏗️ Architecture Overview

![Architecture Diagram](images/architecture.svg)

## 📁 Project Structure

```
Pharma_project/
├── 📁 pipeline/                 # Databricks job definitions
│   ├── Client_trail_pipeline.yml
│   ├── Lookup_Pipeline.yml
│   └── Manufacturing_Pipeline.yml
├── 📁 transform/               # Data transformation notebooks
│   ├── 📁 bronze/              # Raw data ingestion
│   ├── 📁 silver/              # Data cleaning & validation
│   └── 📁 gold/                # Business logic & aggregation
├── 📁 dataset/                 # Reference data
│   └── schedule_holidays.csv
├── 📁 images/                  # Documentation assets
│   ├── architecture.svg
│   ├── pipeline_img/
│   └── tables/
├── 📁 result/                  # Output visualizations
│   ├── df_analyst_vis.png
│   ├── newplot.png
│   └── visualization.png
├── 📁 Setup/                   # Environment setup
│   ├── setup_catalog.sql
│   └── utilities.py
└── 📁 testing/                 # Test scripts
```

## 🔄 Data Pipeline Components

### 1. Clinical Trials Pipeline

The main pipeline processes three types of clinical trial data:

#### **Laboratory Results (LB)**
- **Source**: Clinical trial laboratory measurements
- **Processing**: Bronze → Silver → Gold transformation
- **Output**: Validated lab results ready for analysis

#### **Vital Signs (VS)**
- **Source**: Patient vital signs monitoring
- **Processing**: Bronze → Silver → Gold transformation
- **Output**: Cleaned vital signs data

#### **Demographics (DM)**
- **Source**: Patient demographic information
- **Processing**: Bronze → Silver → Gold transformation
- **Output**: Standardized demographic data

### 2. Manufacturing Pipeline

Processes manufacturing and quality control data:

- **Equipment Lot Tracking**
- **Manufacturing Deviations**
- **Manufacturing Orders**
- **Quality Control Lab Results**

### 3. Holiday Lookup Pipeline

Conditional pipeline execution based on holidays:

```python
# Holiday Schedule Management
Date,Holiday Name
26-01-2024,Republic Day
08-03-2024,Maha Shivaratri
25-03-2024,Holi
... [Additional holidays]
```

## 🛠️ Tech Stack

- **Platform**: Databricks
- **Storage**: Delta Lake on S3
- **Processing**: Apache Spark
- **Architecture**: Medallion (Bronze-Silver-Gold)
- **Ingestion**: Auto Loader with CloudFiles
- **Scheduling**: Databricks Jobs
- **Testing**: Pytest

## 📊 Data Flow Architecture

### Bronze Layer (Raw Data Ingestion)
```python
# Example: Clinical Trials LB Processing
df_bronze = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("pathGlobFilter", "*.csv")
    .option("header","true")
    .option("cloudFiles.inferColumnTypes", "true")
    .load(base_path)
)
```

### Silver Layer (Data Cleaning & Validation)
- Schema validation
- Data quality checks
- Standardization
- Deduplication

### Gold Layer (Business Analytics)
- Aggregated metrics
- KPI calculations
- Reporting tables
- ML-ready datasets

## Tables

![clinical_trials](images/tables/clinical_trail_1.png)

![clinical_trials](images/tables/clinical_trail_2.png)

![schedule_holiday](images/tables/holiday_png.png)

## 📈 Visualizations & Results

### Analytics Dashboard Outputs

#### Clinical Trial Analysis
![Clinical Trial Visualization](result/visualization.png)

#### Data Analyst View
![Analyst Dashboard](result/df_analyst_vis.png)

#### Statistical Analysis
![Statistical Plot](result/newplot%20(1).png)

## 🚀 Setup & Installation

### Prerequisites
- Databricks workspace
- S3 bucket for data storage
- Appropriate IAM permissions
- Databricks secrets configured

## 📋 Pipeline Execution

### Holiday-Based Scheduling
![Holiday-Based Scheduling](images/pipeline_img/pfizer_lookup_pipeline.png)

### Clinical Trials Pipeline Flow
![Clinical Trials Pipeline](images/pipeline_img/client_trail_pipeline.png)

### Manufacturing Pipeline Flow
![Manufacturing Pipeline](images/pipeline_img/manufacturing_pipeline.png)

## 🔧 Configuration

### Pipeline Settings
- **Performance Target**: PERFORMANCE_OPTIMIZED
- **Notification**: Enabled
- **Trigger**: AvailableNow for streaming
- **Checkpoint Location**: Configured for each stream

### Data Sources
- **Clinical Trials**: Laboratory, Vital Signs, Demographics
- **Manufacturing**: Equipment, Lots, Orders, QC Results
- **Reference**: Holiday schedules

## 📊 Key Features

### ✅ Data Quality
- Schema evolution support
- Automatic type inference
- Data validation rules
- Error handling and logging

### ✅ Scalability
- Auto-scaling Spark clusters
- Incremental processing
- Parallel task execution
- Optimized performance targets

### ✅ Monitoring
- Pipeline health checks
- Data quality metrics
- Execution logs
- Performance monitoring

### Manufacturing Data Types

- Equipment tracking
- Lot manufacturing
- Quality control results
- Deviation tracking

## 📞 Support & Maintenance

### Monitoring Dashboard
Regular monitoring of:
- Pipeline execution status
- Data quality metrics
- Processing times
- Error rates

### Maintenance Tasks
- Schema updates
- Performance optimization
- Security audits
- Backup verification

## 👨‍💻 Author
**Ankit Bisen**