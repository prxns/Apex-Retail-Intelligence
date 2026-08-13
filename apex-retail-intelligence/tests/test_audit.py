from src.audit.core import reconcile_counts


def test_audit_passes_exact_count():
    assert reconcile_counts(1052, 1052) == (0, "PASS")


def test_audit_fails_mismatch():
    assert reconcile_counts(1052, 1050) == (-2, "FAIL")
