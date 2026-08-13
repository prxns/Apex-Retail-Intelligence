from pathlib import Path


def test_required_notebooks_exist():
    root = Path(__file__).resolve().parents[1] / "notebooks"
    expected = [f"0{i}_{name}.py" for i, name in enumerate([
        "Raw_Ingestion", "Landing_Conversion", "Bronze_Layer", "Silver_Layer", "Gold_Layer", "KPI_Reporting"
    ], start=1)]
    assert all((root / name).exists() for name in expected)


def test_no_watermarking_in_pipeline_code():
    root = Path(__file__).resolve().parents[1]
    source = "\n".join(p.read_text(encoding="utf-8") for p in root.rglob("*.py") if "tests" not in p.parts)
    assert "withWatermark" not in source
