# Pfizer Pharma Project Architecture

This diagram summarizes the architecture implemented in this repository: Databricks Jobs run Spark notebooks, ingest CSV data from S3, and persist Delta Lake outputs through bronze, silver, and gold medallion layers.

![Pfizer Pharma Data Pipeline Architecture](images/architecture.svg)

```mermaid
flowchart LR
    subgraph Sources["Source and configuration"]
        S3Clinical["Clinical trial CSV<br/>LB, VS, DM"]
        S3Mfg["Manufacturing CSV<br/>equipment, deviations, lots, orders"]
        S3QC["QC lab result CSV"]
        HolidayCSV["schedule_holidays.csv"]
        Secrets["Databricks secret scope<br/>bucket paths"]
        Catalog["pfizer catalog<br/>bronze, silver, gold schemas"]
    end

    subgraph Jobs["Databricks orchestration"]
        Lookup["Pfizer_Lookup_Pipeline<br/>get_run_day -> check_holiday"]
        ClinicalJob["Clinical Trail Pipeline<br/>LB/VS/DM bronze and silver -> gold"]
        NotebookJobs["Manufacturing and QC notebooks"]
    end

    subgraph Bronze["Bronze Delta on S3"]
        BLB["clinical_trials_LB"]
        BVS["clinical_trials_VS"]
        BDM["clinical_trials_DM"]
        BMFG["mfg and QC bronze tables"]
    end

    subgraph Silver["Silver Delta on S3"]
        SLB["LB validation<br/>range checks"]
        SVS["VS cleanup<br/>vital categories"]
        SDM["DM cleanup<br/>demographic standardization"]
        SMFG["MFG and QC cleanup<br/>null flags, status/result normalization"]
    end

    subgraph Gold["Gold Delta on S3"]
        GClinical["Clinical safety marts<br/>gold_subject<br/>gold_demo_safety"]
        GQC["QC analytics marts<br/>qc_summary<br/>qc_trend<br/>qc_batch_quality<br/>qc_analyst"]
    end

    subgraph Consume["Consumption and verification"]
        Results["result/*.png visualizations"]
        Displays["Databricks display() outputs"]
        Tests["pytest holiday tests"]
    end

    Secrets --> S3Clinical
    Secrets --> S3Mfg
    Secrets --> S3QC
    HolidayCSV --> Lookup
    Lookup -. RUN_MAIN / SKIP .-> ClinicalJob
    ClinicalJob --> BLB
    ClinicalJob --> BVS
    ClinicalJob --> BDM
    NotebookJobs --> BMFG
    S3Clinical --> BLB
    S3Clinical --> BVS
    S3Clinical --> BDM
    S3Mfg --> BMFG
    S3QC --> BMFG
    BLB --> SLB
    BVS --> SVS
    BDM --> SDM
    BMFG --> SMFG
    SLB --> GClinical
    SVS --> GClinical
    SDM --> GClinical
    SMFG --> GQC
    GClinical --> Results
    GQC --> Results
    GClinical --> Displays
    GQC --> Displays
    Lookup --> Tests
    Catalog -. organizes .-> Bronze
    Catalog -. organizes .-> Silver
    Catalog -. organizes .-> Gold
```

## Implementation Mapping

- `pipeline/Client_trail_pipeline.yml` defines the clinical LB, VS, and DM notebook dependency chain.
- `pipeline/Lookup_Pipeline.yml` runs the date lookup flow before downstream scheduling decisions.
- `transform/bronze/` ingests raw CSV files using Auto Loader and writes Delta tables to S3.
- `transform/silver/` performs cleaning, standardization, null checks, validation flags, and Delta upserts or overwrites.
- `transform/gold/clinical_trails_gold.py` builds subject-level and demographic safety summaries.
- `transform/gold/QC_Lab_result_gold.py` builds QC summary, trend, batch-quality, and analyst-performance marts.
- `Setup/setup_catalog.sql` creates the `pfizer` catalog and medallion schemas.
- `result/` stores visualization outputs generated from the gold-layer analytics.
