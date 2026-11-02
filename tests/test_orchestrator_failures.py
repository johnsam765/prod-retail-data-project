
from unittest.mock import MagicMock, patch
import pytest

import pipeline_orchestrator as p


def test_bronze_failure_records_audit_and_stops_pipeline():
    fake_spark = MagicMock()
    saved_audits = []

    def capture_audit(spark, record):
        saved_audits.append(record.copy())

    with (
        patch.object(
            p, "create_spark_session",
            return_value=fake_spark
        ),
        patch.object(
            p, "generate_pipeline_run_id",
            return_value="TEST-RUN-100"
        ),
        patch.object(
            p, "run_bronze",
            side_effect=RuntimeError(
                "Simulated Customers write failure"
            )
        ),
        patch.object(
            p, "get_source_batch_id"
        ) as get_batch,
        patch.object(
            p, "run_silver"
        ) as run_silver,
        patch.object(
            p, "run_gold"
        ) as run_gold,
        patch.object(
            p, "write_pipeline_audit",
            side_effect=capture_audit
        )
    ):
        with pytest.raises(
            RuntimeError,
            match="Simulated Customers write failure"
        ):
            p.run_pipeline()

    assert len(saved_audits) == 1

    audit = saved_audits[0]

    assert audit["pipeline_run_id"] == "TEST-RUN-100"
    assert audit["source_batch_id"] is None
    assert audit["status"] == "FAILED"
    assert audit["failed_stage"] == "BATCH_VALIDATION(BRONZE)"
    assert audit["bronze_count"] is None
    assert audit["error_message"] == (
        "Simulated Customers write failure"
    )

    get_batch.assert_not_called()
    run_silver.assert_not_called()
    run_gold.assert_not_called()

    fake_spark.stop.assert_called_once()
