# Production Retail Data Platform — Development Roadmap

## Current milestone: Gold recovery and reliability

### Immediate tasks
- [ ] Verify Gold-only recovery is business-idempotent across repeated runs.
- [ ] Compare recovered Gold against active Gold using `exceptAll()`.
- [ ] Verify historical recovery does not modify active Gold.
- [ ] Confirm missing input batch fails before publishing.
- [ ] Verify normal Gold processing still works through the orchestrator.
- [ ] Review and commit the completed Gold recovery changes.
- [ ] Update the learning log with final test results.

## Milestone 2 — Orchestration and recovery

- [ ] Record Silver input Delta version in audit metadata.
- [ ] Populate `source_batch_id` consistently in orchestrator audit records.
- [ ] Standardize audit statuses (`SUCCESS`, `FAILED`).
- [ ] Add `recovery_of_run_id` for recovery lineage.
- [ ] Separate current processing run ID from original ingestion run ID.
- [ ] Implement Gold-only recovery through the orchestrator.
- [ ] Track stage attempt numbers and outcomes.
- [ ] Handle partial Silver/Quarantine publishing failures.
- [ ] Implement retry-safe audit persistence with Delta MERGE.
- [ ] Design durable audit recovery after driver or process failure.

## Milestone 3 — Incremental processing and idempotency

- [ ] Implement input file fingerprinting.
- [ ] Create an ingestion ledger to prevent duplicate source ingestion.
- [ ] Define stable record identities and deterministic order sequencing.
- [ ] Implement incremental Bronze ingestion.
- [ ] Implement incremental Silver using Delta MERGE.
- [ ] Handle inserted, updated and deleted source records.
- [ ] Design correct cumulative Gold aggregation.
- [ ] Handle order amount, status and region changes.
- [ ] Study Delta Change Data Feed and change-based processing.
- [ ] Implement safe retries without duplicate business contributions.

## Milestone 4 — Production quality

- [ ] Create pytest unit tests for validation and transformation logic.
- [ ] Add integration tests using temporary Delta tables.
- [ ] Test malformed data, missing customers and duplicate orders.
- [ ] Test Silver, Gold and audit failures.
- [ ] Add record-level reconciliation and data-quality assertions.
- [ ] Introduce structured logging and meaningful error messages.
- [ ] Complete README, architecture diagram and dependency manifest.
- [ ] Set up automated testing through GitHub Actions or Azure DevOps.

## Milestone 5 — Spark and Delta performance

- [ ] Analyze logical and physical execution plans.
- [ ] Understand shuffle stages, tasks and partitions.
- [ ] Practice broadcast joins and Adaptive Query Execution.
- [ ] Study caching, persistence and recomputation.
- [ ] Investigate data skew and expensive joins.
- [ ] Study Delta OPTIMIZE, compaction, VACUUM and retention.
- [ ] Benchmark realistic data volumes.

## Milestone 6 — Azure Data Engineering

### Azure foundations and ADLS Gen2
- [ ] Entra ID, tenants, subscriptions and resource groups.
- [ ] Create and navigate Azure Storage resources.
- [ ] Understand containers, directories and hierarchical namespace.
- [ ] Practice RBAC, ACLs and managed identity authentication.
- [ ] Design Bronze/Silver/Gold storage in ADLS Gen2.
- [ ] Connect Spark and Delta Lake to cloud storage.

### Azure Data Factory
- [ ] Explore ADF Studio: Author, Monitor and Manage.
- [ ] Configure Integration Runtime, linked services and datasets.
- [ ] Build Copy activity and parameterized pipelines.
- [ ] Learn ForEach, Lookup, Get Metadata and conditional activities.
- [ ] Configure schedule, event and tumbling-window triggers.
- [ ] Implement retries, failure dependencies, monitoring and alerts.
- [ ] Integrate Azure DevOps Git and CI/CD.
- [ ] Orchestrate the retail data pipeline.

### Azure Databricks
- [ ] Navigate Databricks workspace and compute configuration.
- [ ] Run PySpark notebooks against Delta tables.
- [ ] Learn Unity Catalog and access control.
- [ ] Configure jobs and workflows.
- [ ] Practice performance optimization and troubleshooting.
- [ ] Deploy the retail transformations.

### Azure Synapse Analytics
- [ ] Explore Synapse Studio.
- [ ] Compare serverless SQL, dedicated SQL pools and Spark pools.
- [ ] Query lake data through SQL.
- [ ] Understand external tables and analytical serving.
- [ ] Compare Synapse, Databricks and Fabric architectures.

### Microsoft Purview
- [ ] Understand governance, cataloging and metadata discovery.
- [ ] Explore Data Map and Unified Catalog.
- [ ] Configure data sources and scanning where available.
- [ ] Learn sensitivity classification and lineage.
- [ ] Trace retail source-to-Gold data lineage.

### Microsoft Fabric
- [ ] Explore OneLake, Lakehouse and Warehouse.
- [ ] Learn Fabric Data Factory pipelines.
- [ ] Compare Fabric with ADF, Synapse and Databricks.

## Milestone 7 — Production deployment and operations

- [ ] Airflow DAGs, scheduling, retries and monitoring.
- [ ] CI/CD, environment configuration and secrets management.
- [ ] Azure Key Vault and Azure Monitor.
- [ ] Storage security and network controls.
- [ ] Cost estimation and optimization.
- [ ] Disaster recovery and operational runbooks.
- [ ] Kafka and Spark Structured Streaming fundamentals.

## Interview preparation — Throughout all milestones

- [ ] Advanced SQL exercises.
- [ ] Python coding and debugging exercises.
- [ ] Spark transformations and execution-plan questions.
- [ ] Delta MERGE and idempotency scenarios.
- [ ] ADF/ADLS/Databricks/Synapse/Purview interview questions.
- [ ] System design and production troubleshooting.
- [ ] Timed mock interviews every two weeks.
- [ ] Explain the complete retail capstone without referring to notes.

## Final capstone deliverable

An end-to-end, documented and tested retail data platform demonstrating ingestion, validation, incremental processing, Delta Lake, orchestration, audit, recovery, Azure integration, monitoring, security and analytical reporting.
