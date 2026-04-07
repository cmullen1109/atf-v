# atf-v
Repo for Palantir's American Tech Fellowship - Veterans


# Environmental Data Acquisition Pipeline

## Overview

This project is a side project done during Palantir's ATF-V. This project implements a lightweight data acquisition pipeline that ingests environmental signals from structured and unstructured sources and stages minimally processed datasets in Amazon S3 for downstream transformation and analysis in Palantir's Foundry.

The system integrates:

* Satellite-based wildfire detections (NASA FIRMS)
* Social media signals (Reddit)

The pipeline is intentionally designed to perform minimal preprocessing, preserving raw data fidelity while standardizing key fields required for downstream ingestion.

---

## Problem

Environmental event data exists across fragmented systems:

* Structured high-confidence sensor data (e.g., satellite detections)
* Unstructured low-to-moderate confidence human observations (e.g., social media)

These datasets vary in format and structure, making it difficult to ingest and analyze them within a unified platform without a consistent upstream data layer.

---

## Solution

This pipeline provides:

* Modular ingestion clients for external APIs
* A consistent, minimally standardized schema across sources
* CSV-formatted outputs optimized 
* Partitioned storage in Amazon S3 for ingestion, transformation, and fusion into Palantir Foundry

The result is a clean(ish) raw data layer that enables scalable ETL, enrichment, and data fusion within Foundry.

---

## Architecture

External APIs (FIRMS, Reddit)
↓
Ingestion Layer (Python clients)
↓
Minimal standardization (timestamps, source tagging)
↓
CSV generation and download
↓
Downstream ETL and analytics (Palantir Foundry)

---

## Data Sources

### NASA FIRMS

* Satellite-based fire detection data
* Provides latitude, longitude, timestamps, and confidence indicators

### Reddit

* Subreddit-based ingestion (environmental and regional communities)
* Provides real-time, unstructured observations and context

---

## Data Model (Raw CSV Schema)

All ingested records conform to a unified, ingestion-friendly schema:

* source: data origin (e.g., "firms", "reddit")
* ingestion_timestamp: time of ingestion
* event_timestamp: time of observed event
* lat: latitude (nullable)
* lon: longitude (nullable)
* raw_text: unstructured content (nullable)
* raw_payload: serialized original API response (stringified JSON)
* metadata: serialized source-specific attributes (stringified JSON)

The schema is intentionally minimal to defer transformation, enrichment, and data fusion to downstream systems.

---

## Storage Strategy

Data is written locally using a partitioned layout:

s3://env-firedata-raw/
├── firms/
│     └── date=YYYY-MM-DD/
│           └── data.csv
└── reddit/
└── date=YYYY-MM-DD/
└── data.csv

### Format

* CSV (UTF-8 encoded)
* Nested structures (payload, metadata) stored as stringified JSON
* Compatible with Foundry's Pipeline Builder

---

## Design Principles

* **Minimal preprocessing**
  Preserve raw data; defer transformations to Foundry

* **Schema consistency**
  Standardize only essential fields across sources

* **Separation of concerns**
  Ingestion pipeline focuses solely on data acquisition and staging

* **Extensibility**
  Additional data sources can be added with minimal changes

---

## Example Use Cases

* Environmental monitoring and event detection
* Multi-source data fusion pipelines
* Geospatial analytics workflows
* Data platform prototyping (e.g., Foundry ingestion pipelines)

---

## Future Improvements

* Add additional environmental data sources (weather alerts, air quality APIs)
* Implement ingestion monitoring and logging
* Introduce incremental ingestion and deduplication strategies
* Support near real-time ingestion
