
# Dagster UI Navigation Guide

This document explains how to navigate the Dagster web interface when running the pipeline locally, typically accessible at:

**http://0.0.0.0:3000**

---

## Key URLs and Sections

### 1. **Runs**
- **URL:** `http://0.0.0.0:3000/runs`
- **Purpose:**  
  View all pipeline runs (executions) here. You can monitor status (success, failure, in progress), see logs, and check execution details.
- **Use case:**  
  Useful for tracking pipeline health and debugging failed runs.

### 2. **Jobs**
- **URL:** `http://0.0.0.0:3000/jobs`
- **Purpose:**  
  List all scheduled and manual jobs configured in Dagster. From here you can trigger runs manually or view job schedules.
- **Use case:**  
  Manage recurring pipelines and ad-hoc executions.

### 3. **Assets**
- **URL:** `http://0.0.0.0:3000/assets`
- **Purpose:**  
  Explore the data assets managed by Dagster. Assets represent datasets or tables in the pipeline, showing lineage, freshness, and dependencies.
- **Use case:**  
  Understand how data flows through the pipeline and check data freshness and quality status.

### 4. **Schedules and Sensors**
- **URL:** `http://0.0.0.0:3000/schedules` and `http://0.0.0.0:3000/sensors`
- **Purpose:**  
  View and manage automated triggers for pipeline runs, either time-based (schedules) or event-based (sensors).
- **Use case:**  
  Ensure pipelines run automatically according to business needs.

---

## Access Notes

- The Dagster UI is exposed on port 3000 by default in Docker Compose.
- Use this interface to monitor, debug, and manage your ETL pipelines efficiently.
- Logs and detailed run metadata help with production observability.

