"""Pure audit helpers with no Spark dependency."""
def reconcile_counts(expected: int, actual: int) -> tuple[int, str]:
    difference = actual - expected
    return difference, ("PASS" if difference == 0 else "FAIL")
