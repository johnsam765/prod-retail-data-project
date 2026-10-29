
# Production Retail Data Platform — TODO

Last updated: October 29, 2026

## CURRENT MILESTONE
Gold Recovery VERIFIED
Audit MERGE Initial Implementation TESTED
Audit Conflict Detection PENDING VERIFICATION

---

## 1. Immediate Next Session

- [ ] Confirm corrected audit conflict-detection code.
- [ ] Run the complete four-case audit integration test.
- [ ] Verify new audit record insertion.
- [ ] Verify identical retry does not duplicate data.
- [ ] Verify conflicting retry raises AUDIT_CONFLICT.
- [ ] Verify a different pipeline_run_id inserts correctly.
- [ ] Inspect Delta history after testing.
- [ ] Review git diff and git status.
- [ ] Commit tested audit changes.
- [ ] Update PROJECT_STATE.md with test evidence and commit SHA.

## 2. Gold Recovery

- [x] Implement Gold-only recovery using versionAsOf.
- [x] Support separate recovery output path.
- [x] Verify historical Silver version 8.
- [x] Verify four regional Gold rows.
- [x] Verify total Completed revenue = $2,776.50.
- [x] Re-execute Gold recovery.
- [x] Confirm Recovery Gold version 1 -> 2.
- [x] Confirm Active Gold remains version 5.
- [x] Verify bidirectional exceptAll differences = 0.
- [ ] Record input Silver version in audit metadata.
- [ ] Record recovery execution identity.
- [ ] Test recovery when selected Silver batch is missing.
- [ ] Verify normal Gold processing through the orchestrator.
- [ ] Review and commit recovery code changes.

## 3. Audit Reliability

- [x] Understand Delta append retry duplication risk.
- [x] Implement initial insert-only audit MERGE.
- [x] Match using pipeline_run_id.
- [x] Support optional audit_path for isolated testing.
- [x] Test initial audit insertion.
- [x] Test identical retry.
- [x] Verify conflicting status is silently skipped by initial MERGE.
- [ ] Implement explicit AUDIT_CONFLICT detection.
- [ ] Verify conflict-detection logic using integration tests.
- [ ] Test existing duplicate pipeline_run_id records.
- [ ] Define final audit status transition rules.
- [ ] Define concurrency and retry policy.
- [ ] Handle commit succeeded / acknowledgement lost scenario.
- [ ] Create automated audit pytest tests.
- [ ] Verify audit writer with normal orchestration.

## 4. Orchestration and Recovery

- [ ] Fix orchestrator audit source_batch_id assignment.
- [ ] Standardize SUCCESS and FAILED status values.
- [ ] Track source_silver_version.
- [ ] Track recovery_of_run_id.
- [ ] Distinguish original ingestion run from recovery execution.
- [ ] Implement Gold-only recovery through orchestrator.
- [ ] Handle partial stage failures.
- [ ] Design safe Silver/Quarantine publication.
- [ ] Record input and output Delta versions.
- [ ] Add meaningful structured logs.
- [ ] Introduce stage-level attempt tracking.
- [ ] Test recovery without reingesting Bronze.

## 5. Incremental Data Engineering

### Bronze
- [ ] Implement source file fingerprinting.
- [ ] Create ingestion ledger.
- [ ] Prevent repeated ingestion of identical files.
- [ ] Handle new and changed source files.
- [ ] Track source version and source record identity.

### Silver
- [ ] Implement incremental Silver MERGE.
- [ ] Define deterministic deduplication.
- [ ] Handle late-arriving records.
- [ ] Handle source corrections.
- [ ] Handle updated and deleted orders.
- [ ] Prevent duplicate contributions.
- [ ] Improve customer lookup deduplication.
- [ ] Add record-level reconciliation.

### Gold
- [ ] Define cumulative Gold table grain.
- [ ] Implement incremental Gold updates.
- [ ] Handle order amount changes.
- [ ] Handle region changes.
- [ ] Handle Completed -> Cancelled changes.
- [ ] Prevent double counting during retries.
- [ ] Study Delta Change Data Feed.

## 6. Automated Testing

- [ ] Add pytest tests for Bronze transformations.
- [ ] Add pytest tests for Silver validations.
- [ ] Add tests for rejection reasons.
- [ ] Add customer join and fan-out tests.
- [ ] Add deterministic deduplication tests.
- [ ] Add Gold aggregation tests.
- [ ] Add audit MERGE regression tests.
- [ ] Add recovery and failure-injection tests.
- [ ] Add record-level reconciliation tests.
- [ ] Configure GitHub Actions testing.
- [ ] Add dependency and environment documentation.

## 7. Spark Performance

- [ ] Study logical and physical execution plans.
- [ ] Understand stages, tasks, executors and partitions.
- [ ] Practice Spark shuffle troubleshooting.
- [ ] Study broadcast joins.
- [ ] Study Adaptive Query Execution.
- [ ] Practice caching and persistence.
- [ ] Understand data skew.
- [ ] Study partitioning strategies.
- [ ] Learn Delta OPTIMIZE and compaction.
- [ ] Understand VACUUM and retention.
- [ ] Benchmark larger retail datasets.

## 8. Azure Data Engineering

### ADLS Gen2
- [ ] Understand Azure storage accounts.
- [ ] Understand hierarchical namespace.
- [ ] Create containers and directories.
- [ ] Configure Bronze/Silver/Gold storage.
- [ ] Study Entra ID and managed identities.
- [ ] Understand RBAC and ACLs.
- [ ] Connect PySpark to ADLS Gen2.

### Azure Data Factory
- [ ] Explore ADF Studio.
- [ ] Study Author, Monitor and Manage.
- [ ] Configure linked services.
- [ ] Configure datasets.
- [ ] Understand Integration Runtime.
- [ ] Build Copy activity.
- [ ] Implement parameterized pipelines.
- [ ] Learn Lookup, ForEach and Get Metadata.
- [ ] Configure triggers.
- [ ] Implement retry and failure handling.
- [ ] Monitor pipeline executions.
- [ ] Integrate ADF with the retail project.

### Azure Databricks
- [ ] Explore Databricks workspace.
- [ ] Understand compute configuration.
- [ ] Run PySpark notebooks.
- [ ] Implement Delta tables.
- [ ] Study Unity Catalog.
- [ ] Configure Databricks Jobs.
- [ ] Integrate Databricks with ADLS.
- [ ] Connect Databricks with ADF.
- [ ] Practice troubleshooting and optimization.

### Azure Synapse Analytics
- [ ] Explore Synapse Studio.
- [ ] Understand serverless SQL.
- [ ] Understand dedicated SQL pools.
- [ ] Query Delta/Parquet lake data.
- [ ] Explore external tables.
- [ ] Compare Synapse and Databricks.
- [ ] Understand analytical serving.

### Microsoft Purview
- [ ] Understand data governance.
- [ ] Explore Data Map and Unified Catalog.
- [ ] Learn metadata scanning.
- [ ] Study classification.
- [ ] Understand data lineage.
- [ ] Trace source-to-Gold lineage.

### Microsoft Fabric
- [ ] Explore OneLake.
- [ ] Explore Fabric Lakehouse.
- [ ] Explore Fabric Warehouse.
- [ ] Understand Fabric pipelines.
- [ ] Compare Fabric with Synapse and Databricks.

## 9. Production Operations

- [ ] Study Airflow orchestration.
- [ ] Build a basic Airflow DAG.
- [ ] Configure retries and monitoring.
- [ ] Implement CI/CD.
- [ ] Study Azure Key Vault.
- [ ] Study Azure Monitor.
- [ ] Understand cloud security.
- [ ] Understand environment management.
- [ ] Add operational runbooks.
- [ ] Learn disaster recovery.
- [ ] Understand cloud cost optimization.
- [ ] Study Kafka and Spark Structured Streaming.

## 10. Interview Preparation — Ongoing

- [ ] Practice advanced SQL questions.
- [ ] Practice Python coding questions.
- [ ] Explain Spark transformations and shuffles.
- [ ] Explain Delta MERGE and idempotency.
- [ ] Explain Bronze/Silver/Gold architecture.
- [ ] Practice ADF and Databricks scenarios.
- [ ] Practice ADLS security questions.
- [ ] Practice pipeline failure investigations.
- [ ] Practice system design.
- [ ] Complete timed mock interviews.
- [ ] Explain the retail architecture independently.

---

## Final Project Goal

Build an end-to-end Azure Retail Data Engineering Platform
demonstrating:

- Source ingestion
- Bronze, Silver and Gold architecture
- Data validation and quarantine
- Incremental processing
- Deterministic deduplication
- Delta Lake transactions
- Idempotent operations
- Pipeline orchestration
- Audit and lineage
- Disaster recovery
- Automated testing
- CI/CD
- Azure Data Factory
- ADLS Gen2
- Azure Databricks
- Analytical querying
- Governance and monitoring

The final deliverable should be a documented, tested,
demonstrable project suitable for Data Engineering interviews.
