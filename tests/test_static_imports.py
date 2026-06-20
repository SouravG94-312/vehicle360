def test_static_structure():
    from pathlib import Path
    root = Path(__file__).resolve().parents[1]
    assert (root / "src" / "vehicle360" / "loaders" / "silver_to_gold_loader.py").exists()
    assert (root / "src" / "vehicle360" / "transformations" / "silver_to_gold.py").exists()
