# Production Retail Data Platform — TODO

Last updated: November 5, 2026

## CURRENT MILESTONE

- Gold Recovery: VERIFIED
- Audit MERGE and Conflict Detection: VERIFIED
- Orchestrator Failure Testing: VERIFIED
- Bronze Source/Persisted Verification: VERIFIED
- SHA-256 File Fingerprinting: IMPLEMENTED AND TESTED
- Bronze Ingestion Ledger: SCHEMA, LOOKUP AND WRITER IMPLEMENTED; NOT YET INTEGRATED
- ADLS Gen2: PORTAL EXPLORATION COMPLETED
- Azure Deployment: NOT STARTED
- Git Commit/Push for Nov 4–5 changes: NOT CONFIRMED
- Writer Integration Tests: USER REPORTED DONE; OUTPUT NOT REVIEWED

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



## NEXT SESSION — Finish Bronze ledger integration (bounded milestone)

**Existing components, already done:** source fingerprint, ledger schema, successful-ingestion lookup, ledger writer. Do **not** redesign them.

**Only active tasks:**
- [ ] Review the final writer test result or rerun the existing test suite; don't count an unreviewed result as verified.
- [ ] Connect existing fingerprint lookup to the actual Bronze processing path.
- [ ] Demonstrate an unchanged previously completed source does not append a second Bronze copy in a controlled Delta test.
- [ ] Demonstrate a changed source is treated as a distinct source version.
- [ ] Review `git status`, update these two documents briefly, and commit after tests pass.

**Definition of done:** A repeatable integration test demonstrates duplicate avoidance for successful sequential replays and correct behavior for changed input. This is **not** a claim of concurrency-safe exactly-once processing or crash recovery.

**Immediately after this milestone:** Incremental Silver MERGE and deterministic deduplication. No additional Bronze hardening tasks unless a test reveals a blocking correctness defect.

**Deferred backlog (not session work):** partial-commit recovery, transaction coordination, and concurrent-writer protections.

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

- [x] Add a permanent mocked Bronze-failure pytest test (reported passed Nov 2).
- [ ] Add permanent pytest coverage for remaining orchestrator paths.
- [ ] Test successful execution control flow.
- [x] Test mocked Bronze failure control flow using pytest (reported passed Nov 2).
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

- [x] Implement source file fingerprinting using SHA-256.
- [x] Test identical file fingerprints.
- [x] Verify reordered CSV produces a different fingerprint.
- [x] Inspect Sales and Customers Bronze schemas.
- [x] Verify selected Sales Bronze batch: 19/19 records.
- [x] Verify selected Customers Bronze batch: 10/10 records.
- [x] Verify bidirectional exceptAll differences = 0.
- [x] Create standalone ingestion ledger schema, successful lookup and writer (Nov 5).
- [ ] Verify latest writer integration test result; no output reviewed.
- [ ] Integrate SHA-256 fingerprints into Bronze ingestion.
- [ ] Prevent repeated ingestion of identical completed files.
- [ ] Handle new and changed source files.
- [ ] Track source version and source record identity.
- [ ] Automate persisted Bronze verification with pytest.
- [ ] Handle partial Sales/Customers ingestion failures.
- [ ] Implement safe partial-batch recovery.

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

- [x] Add a permanent pytest test for mocked Bronze failure (reported passed Nov 2).
- [ ] Extend pytest to remaining orchestrator failure paths.
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

- [x] Explore Azure Portal Storage overview.
- [x] Inspect existing storage account configurations.
- [x] Understand hierarchical namespace at a basic level.
- [x] Review existing Azure resource group costs.
- [ ] Obtain authorization for a development sandbox.
- [ ] Understand Azure storage accounts in depth.
- [ ] Create an approved ADLS Gen2 storage account.
- [ ] Understand hierarchical namespace operations.
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

## 11. Original November 2–6 Plan — historical reference only

**This is the original schedule, not the current task list.** Actual progress diverged: November 4 completed Bronze source verification and SHA-256; November 5 completed standalone ledger functions instead of starting Silver MERGE. **Use Section 1 for the next session; do not revive unchecked items below as immediate tasks.**

### November 2–6, 2026 (original plan)

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

Historical schedule retained to show the original intention. Future milestones do not automatically expand when new risks are identified.

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
