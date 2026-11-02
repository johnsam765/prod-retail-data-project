
# Production Retail Data Platform — Project State

Last Updated: November 2, 2026

## 1. Project Overview

Project: Production Retail Data Platform

Objective:
Build a production-style Data Engineering platform using
Python, PySpark, Delta Lake, SQL and Azure.

Target Architecture:
Source Data
    |
    v
Azure Data Factory (planned)
    |
    v
ADLS Gen2 (planned)
    |
    v
Bronze Delta Tables
    |
    v
Silver + Quarantine
    |
    v
Gold Delta Tables
    |
    v
Analytics / Reporting

Current Environment:
- Ubuntu / WSL
- Python
- PySpark
- Delta Lake
- Java 17
- pytest
- Git / GitHub

Cloud deployment has not yet been implemented.

---

## 2. Current Source Code

### src/bronze_pipeline.py

Implemented:
- Read Sales CSV.
- Read Customers CSV.
- Apply explicit schemas.
- Generate source_batch_id.
- Add ingestion metadata.
- Append records to Bronze Delta tables.

Metadata:
- pipeline_run_id
- source_batch_id
- ingestedTime
- source_path

Known limitations:
- Repeated ingestion can duplicate source data.
- No source fingerprint ledger.
- No durable per-dataset completion tracking.
- Sales and Customers are separate Delta writes.
- Returned DataFrame count does not independently
  verify persisted records.

### src/silver_pipeline.py

Implemented:
- Filter Bronze using pipeline_run_id and source_batch_id.
- Join Sales and Customers.
- Deduplicate using Window functions.
- Validate business rules.
- Send invalid records to Quarantine.
- Perform count reconciliation.
- Overwrite current Silver and Quarantine outputs.

Known limitations:
- Not yet incremental.
- Deterministic deduplication needs improvement.
- Customer duplicate selection may be nondeterministic.
- Silver and Quarantine writes are separate transactions.
- Count reconciliation alone is insufficient.

### src/gold_pipeline.py

Implemented:
- Read Silver Delta.
- Filter by source_batch_id.
- Include Completed orders only.
- Aggregate sales by region.
- Calculate count, total and average revenue.
- Support historical Silver versionAsOf.
- Support isolated recovery output_path.
- Guard historical recovery from writing to active Gold.

Known limitations:
- Current-batch aggregate, not cumulative Gold.
- Incremental updates not implemented.
- Recovery not yet orchestrated automatically.
- Input Silver version is not recorded in audit metadata.

### src/pipeline_audit.py

Implemented locally:
- Explicit audit schema.
- Delta MERGE using pipeline_run_id.
- Insert new execution records.
- Skip identical retries.
- Detect conflicting audit payloads.
- Raise AUDIT_CONFLICT.
- Detect multiple existing records for a run ID.
- Support optional audit_path for isolated tests.

Verified:
- Four isolated integration tests passed on Nov 2.

Known limitations:
- Conflict pre-check and MERGE are separate operations.
- Concurrent writes may encounter race conditions.
- No atomic uniqueness guarantee established.
- Existing legacy duplicates require separate handling.
- Status transitions and stage attempts are not modeled.

### src/pipeline_orchestrator.py

Implemented:
- Generate execution run ID.
- Run Bronze -> Silver -> Gold.
- Track current stage.
- Capture stage counts.
- Handle success and failure.
- Persist final audit through audit writer.
- Stop Spark in cleanup.

Changes implemented November 2:
- Populate audit_record["source_batch_id"] after Bronze.
- Standardize failure status to FAILED.

Verified:
- Mocked Silver failure captures correct audit metadata.
- Spark cleanup invoked.

Known limitations:
- Full rerun can reingest Bronze.
- Partial Bronze recovery not implemented.
- Actual audit persistence during real failure
  has not been integration-tested.
- Recovery mode and recovery_of_run_id not implemented.

---

## 3. Verified Data Results

Historical Bronze:
- Sales: 228 rows across 12 ingestions.
- Each Sales CSV ingestion: 19 rows.

Corrected Silver batch:
- Source Sales: 19
- Silver: 10
- Quarantine: 9
- Reconciliation: PASSED

Source batch:
20261024_042204_ab58a267

Gold:
- East: 4 orders, $1500.75
- North: 2 orders, $950.00
- South: 1 order, $175.25
- West: 1 order, $150.50

Completed orders: 8
Total completed revenue: $2776.50

These are verified historical outputs,
not a claim about the latest current table counts.

---

## 4. Gold Recovery — VERIFIED

Test Date: October 29, 2026

Source:
- Historical Silver version: 8
- source_batch_id: 20261024_042204_ab58a267

Before repeated recovery:
- Active Gold version: 5
- Recovery Gold version: 1

After repeated recovery:
- Active Gold version: 5
- Recovery Gold version: 2

Validation:
- Active Gold rows: 4
- Recovery Gold rows: 4
- Bidirectional exceptAll differences: 0

Result:
PASSED

Verified:
- Historical recovery produced matching business results.
- Active Gold was not modified.
- Recovery overwrite created a new Delta version.

Not proven:
- Active Gold originally read Silver version 8.
- Recovery has transaction-level no-op behavior.
- Full orchestrated recovery works.

---

## 5. Audit MERGE — VERIFIED

Test Date: November 2, 2026

Test 1:
New execution insertion.
Result: PASSED

Test 2:
Retry identical audit record.
Result: PASSED
No duplicate record created.

Test 3:
Same pipeline_run_id with conflicting status.
Result: PASSED

Actual exception:
AUDIT_CONFLICT:
['status', 'failed_stage', 'error_message']

Existing SUCCESS remained unchanged.

Test 4:
Insert different pipeline_run_id.
Result: PASSED

Final isolated audit records:
- TEST-RUN-001 | TEST-BATCH-001 | SUCCESS
- TEST-RUN-002 | TEST-BATCH-001 | SUCCESS

Delta History:
- Version 0: WRITE / ErrorIfExists
- Version 1: MERGE

Conclusion:
Sequential audit retry behavior verified.

Concurrency safety is NOT verified.

---

## 6. Orchestrator Failure Test — VERIFIED

Test Date: November 2, 2026

Testing approach:
unittest.mock / MagicMock / patch

Scenario:
- Bronze succeeds.
- Bronze Sales count = 19.
- Bronze Customers count = 10.
- source_batch_id = TEST-BATCH-050.
- Silver raises RuntimeError.

Captured audit:
- pipeline_run_id: TEST-RUN-100
- source_batch_id: TEST-BATCH-050
- status: FAILED
- failed_stage: DATA QUALITY(SILVER)
- bronze_count: 19
- silver_count: None
- quarantine_count: None
- gold_count: None
- error_message: Simulated Silver failure

Result:
PASSED

Additional:
- Spark cleanup called once.
- No real Bronze/Silver/Gold execution.
- No actual Delta audit persistence.

Important:
This is a CONTROL-FLOW test, not a Delta
persistence integration test.

---

## 7. Production Reliability Principles

### Execution vs Batch Identity

pipeline_run_id:
Identifies one pipeline execution.

source_batch_id:
Identifies the source data batch.

Different executions may process the same source batch.

Example:

RUN100:
- Batch: BATCH050
- Status: FAILED
- Failed stage: GOLD

RUN101:
- Batch: BATCH050
- Status: SUCCESS
- Execution: Recovery

Keep both execution records.

### Retry Idempotency

Identical retry:
Should not duplicate business data or audit entries.

Conflicting retry:
Must be identified and investigated.

New execution:
Should have a distinct pipeline_run_id.

### Delta Failure Investigation

When a write reports failure:
1. Inspect Delta history.
2. Check commit timestamp.
3. Check operation and write mode.
4. Inspect operation metrics.
5. Check commit metadata where available.
6. Verify persisted records.
7. Compare with the expected input.
8. Inspect the exception and driver logs.

A scheduler failure does not necessarily mean
the Delta transaction failed.

### Partial Bronze Failure

Scenario:
Sales commits successfully.
Customers fails.

Current risk:
A full rerun may append Sales again.

Required future design:
- Stable batch identity.
- Source file fingerprint.
- Ingestion ledger.
- Per-dataset completion status.
- Persisted-content verification.
- Safe retry of incomplete stages.

---

## 8. Current Known Issues

Priority: HIGH

1. Audit concurrency protection is incomplete.
2. Partial Bronze recovery is not implemented.
3. Source fingerprinting is missing.
4. Recovery execution identity is not tracked.
5. Incremental Silver MERGE is not implemented.
6. Gold aggregation is not cumulative.
7. Real orchestrator audit persistence requires testing.
8. Critical manual tests need permanent pytest coverage.

Priority: MEDIUM

9. Improve deterministic deduplication.
10. Improve customer duplicate handling.
11. Add record-level reconciliation.
12. Add source and target Delta version lineage.
13. Add structured execution logs.
14. Improve standalone pipeline entry points.
15. Add GitHub Actions testing.

---

## 9. Current Development Milestone

Milestone:
Production Reliability and Recovery

Status:
IN PROGRESS

Completed:
- Gold recovery validation.
- Sequential audit MERGE.
- Audit conflict detection.
- Orchestrator source batch tracking.
- Mocked Silver failure test.

Next tasks:
1. Create permanent pytest failure tests.
2. Review and commit current code.
3. Verify successful orchestrator control flow.
4. Test Bronze failure control flow.
5. Design stage-specific recovery.
6. Begin Bronze ingestion ledger implementation.

Do not start incremental Silver MERGE until
the necessary ingestion and recovery behavior
has been defined.

---

## 10. Current Git Status

Latest confirmed commit:
NOT PROVIDED

Current local source changes:
- src/pipeline_audit.py
- src/pipeline_orchestrator.py

These changes were reported and tested by the user.

Git commit confirmation:
PENDING

Do not mark these changes committed until
a commit SHA is confirmed.

---

## 11. Change History

### October 29, 2026
- Verified repeat Gold recovery.
- Verified Active Gold remained version 5.
- Verified Recovery Gold advanced to version 2.
- Verified identical regional output using exceptAll.
- Introduced insert-only audit MERGE.
- Tested initial audit insert and identical retry.
- Identified silent conflict behavior.

### November 2, 2026
- Verified four audit MERGE scenarios.
- Confirmed explicit AUDIT_CONFLICT detection.
- Verified separate pipeline executions are recorded.
- Implemented orchestrator source_batch_id tracking.
- Standardized FAILED status.
- Verified mocked Silver failure audit.
- Discussed partial Bronze write recovery.
- Identified need for persisted Bronze content checks.

---

## 12. Change Tracking Rules

After each implementation:

Record:
- Date
- File changed
- Behavior changed
- Test performed
- Actual observed result
- Known limitations
- Git commit SHA

Status definitions:
- PLANNED: Not implemented.
- IMPLEMENTED: Code changed but not verified.
- TESTED: Test evidence confirmed.
- COMMITTED: Git commit SHA confirmed.
- BLOCKED: Cannot proceed until issue resolved.

Never mark a feature tested merely because
the code compiles or appears correct.

---

## 13. Next Session Starting Point

Start with:
Permanent pytest tests for orchestrator failure paths.

Then:
Review the Git diff and commit verified changes.

Afterward:
Recovery execution identities and partial-failure
handling.

Do not reingest Bronze merely to test orchestration.
Prefer isolated Delta test tables and mocked stages.

---

## 14. Long-Term Direction

Local PySpark/Delta reliability
    |
    v
Incremental Silver and Gold
    |
    v
Automated testing and CI/CD
    |
    v
ADLS Gen2 + ADF + Databricks
    |
    v
Analytical serving and governance
    |
    v
End-to-end Azure Data Engineering Capstone

Goal:
A documented, testable, demonstrable data
engineering project suitable for technical interviews.
