
# Production Retail Data Platform — TODO

Last updated: November 2, 2026

## CURRENT MILESTONE

- Gold Recovery: VERIFIED
- Audit MERGE: IMPLEMENTED AND TESTED
- Audit Conflict Detection: VERIFIED
- Orchestrator Audit Metadata: MOCK-TESTED
- Concurrent Audit Safety: NOT IMPLEMENTED
- Git Commit: NOT YET CONFIRMED

---

## 1. Immediate Next Session

### Completed — November 2, 2026

- [x] Confirm corrected audit conflict-detection code.
- [x] Run the complete four-case audit integration test.
- [x] Verify new audit record insertion.
- [x] Verify identical retry does not duplicate data.
- [x] Verify conflicting retry raises AUDIT_CONFLICT.
- [x] Verify a different pipeline_run_id inserts correctly.
- [x] Inspect Delta history after testing.
- [x] Fix orchestrator source_batch_id audit assignment.
- [x] Standardize FAILED audit status.
- [x] Simulate Silver failure using MagicMock and patch.
- [x] Verify failed audit contains source_batch_id.
- [x] Verify Bronze count is retained during Silver failure.
- [x] Verify failed stage and exception information.
- [x] Verify Spark cleanup in simulated failure.
- [x] Understand mock testing versus Delta integration testing.

### Next Priorities

- [ ] Create permanent pytest tests for orchestrator failures.
- [ ] Test Bronze failure using mocked pipeline stages.
- [ ] Test successful orchestrator execution using mocks.
- [ ] Review git diff and git status.
- [ ] Commit verified audit and orchestrator changes.
- [ ] Record Git commit SHA.
- [ ] Update PROJECT_STATE.md with commit details.
- [ ] Verify real orchestrator-to-audit Delta persistence.
- [ ] Define safe audit concurrency handling.

### Current Learning Focus

- Pipeline execution identity.
- Source batch identity.
- Audit idempotency and conflict detection.
- Recovery from partial pipeline failures.
- Delta transaction reliability.
- Reusable automated tests.

---

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

### Learning Completed

- Delta tables have independent version histories.
- Historical Silver snapshots can be read using versionAsOf.
- Recovery should use an isolated output destination.
- Overwrite can create a new Delta version with identical values.
- Bidirectional exceptAll verifies record-level equality.
- Equal data does not prove identical source-version lineage.

---

## 3. Audit Reliability

### Implemented and Verified

- [x] Understand Delta append retry duplication risk.
- [x] Implement insert-only audit MERGE.
- [x] Match using pipeline_run_id.
- [x] Support optional audit_path for isolated testing.
- [x] Test initial audit insertion.
- [x] Test identical retry.
- [x] Verify conflicting status was silently skipped by initial MERGE.
- [x] Implement explicit AUDIT_CONFLICT detection.
- [x] Verify conflict-detection logic using integration tests.
- [x] Confirm original SUCCESS remains unchanged after conflicting retry.
- [x] Test insertion of a second pipeline execution.
- [x] Verify Delta history after integration testing.

### Latest Verified Test Results

Test 1:
- Initial audit insertion: PASSED.

Test 2:
- Identical retry: PASSED.
- No duplicate audit record.

Test 3:
- Conflicting retry: PASSED.
- Raised AUDIT_CONFLICT.
- Differences: status, failed_stage, error_message.

Test 4:
- New pipeline execution: PASSED.
- New run inserted.

Final test table:
- TEST-RUN-001: SUCCESS
- TEST-RUN-002: SUCCESS

Delta history:
- Version 0: WRITE
- Version 1: MERGE

### Remaining Audit Work

- [ ] Test existing duplicate pipeline_run_id records.
- [ ] Define final audit status transition rules.
- [ ] Define concurrency and retry policy.
- [ ] Prevent silent conflicts during concurrent writes.
- [ ] Handle commit succeeded / acknowledgement lost scenario.
- [ ] Create automated audit pytest tests.
- [ ] Verify audit writer with normal orchestration.
- [ ] Add audit transaction lineage metadata.
- [ ] Introduce stage-level execution and attempt tracking.

### Known Limitation

The conflict check reads the existing audit table
before executing MERGE.

These are separate operations.

Concurrent writers may read the same initial snapshot,
creating a time-of-check/time-of-use race condition.

Sequential retry correctness has been verified.
Concurrent correctness has NOT been established.

---

## 4. Orchestration and Recovery

### Implemented and Mock-Tested

- [x] Fix orchestrator audit source_batch_id assignment.
- [x] Standardize SUCCESS and FAILED status values.
- [x] Test Silver failure after simulated successful Bronze.
- [x] Verify source_batch_id retained after Silver failure.
- [x] Verify Bronze count retained after Silver failure.
- [x] Verify failed_stage = DATA QUALITY(SILVER).
- [x] Verify exception message recorded.
- [x] Verify Spark cleanup using MagicMock.

### Mock Test Results

Simulated execution:
- pipeline_run_id: TEST-RUN-100
- source_batch_id: TEST-BATCH-050
- bronze_count: 19
- status: FAILED
- failed_stage: DATA QUALITY(SILVER)
- error_message: Simulated Silver failure

Test passed.

IMPORTANT:
The mock test verified orchestration control flow.
It did not execute actual Spark transformations
or persist records to Delta tables.

### Remaining Orchestration Work

- [ ] Convert mock script into permanent pytest tests.
- [ ] Test successful execution control flow.
- [ ] Test Bronze failure control flow.
- [ ] Test Gold failure control flow.
- [ ] Verify real audit persistence after failure.
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

---

## 5. Incremental Data Engineering

### Bronze

- [ ] Implement source file fingerprinting.
- [ ] Create ingestion ledger.
- [ ] Prevent repeated ingestion of identical files.
- [ ] Handle new and changed source files.
- [ ] Track source version and source record identity.
- [ ] Implement persisted Bronze verification.
- [ ] Compare source and persisted record counts.
- [ ] Compare source and persisted record contents.
- [ ] Test Sales commit followed by Customers failure.
- [ ] Design safe partial-batch recovery.

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

---

## 6. Automated Testing

- [ ] Add permanent pytest tests for orchestrator failure paths.
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

### Testing Principles

- Mock tests verify control flow and interactions.
- Integration tests verify real component behavior.
- Delta write tests verify persistence and transactions.
- Count reconciliation is not sufficient for content equality.
- All critical failure scenarios should become repeatable tests.

---

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

---

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

- [ ] Explore Fabric Lakehouse.
- [ ] Explore Fabric Warehouse.
- [ ] Understand OneLake.
- [ ] Understand Fabric pipelines.
- [ ] Compare Fabric with Synapse and Databricks.

---

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

---

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

### Interview Topics Covered So Far

- Delta overwrite versus append.
- Delta version history.
- Batch identity and execution identity.
- Gold recovery.
- exceptAll and data validation.
- Audit MERGE.
- Idempotent retries.
- Conflicting audit records.
- Concurrent write risks.
- Silver failure simulation.
- Bronze partial-write recovery reasoning.
- Mock testing versus integration testing.

---

## 11. Planned Learning Sequence

### November 2–6, 2026

Monday:
- [x] Audit MERGE verification.
- [x] Audit conflict detection.
- [x] Orchestrator metadata improvements.
- [x] Mocked Silver failure verification.
- [ ] Complete Git checkpoint.
- [ ] Convert mock testing into permanent pytest.

Tuesday:
- [ ] Recovery execution identities.
- [ ] Bronze/Silver/Gold failure-path design.
- [ ] Recovery-focused interview questions.

Wednesday:
- [ ] Bronze persisted-data verification.
- [ ] File fingerprinting.
- [ ] Ingestion ledger design.

Thursday:
- [ ] Deterministic Silver deduplication.
- [ ] Incremental MERGE fundamentals.
- [ ] Source version handling.

Friday:
- [ ] Integration testing and review.
- [ ] Git and documentation checkpoint.
- [ ] Senior-level interview scenarios.
- [ ] Review weekly learning outcomes.

Schedule may change based on verified implementation progress.

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
