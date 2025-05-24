import pytest


def test_valid_acled_files_are_sorted(tmp_path):
    from acled_concat.cli import _get_lexically_sorted_csv_paths

    # Setup
    f1 = tmp_path / "01-acled_2021.csv"
    f2 = tmp_path / "02-acled_2022.csv"
    f1.write_text("dummy")
    f2.write_text("dummy")

    result = _get_lexically_sorted_csv_paths(tmp_path)
    assert result == [f1, f2]


def test_consolidated_file_is_ignored(tmp_path):
    from acled_concat.cli import _get_lexically_sorted_csv_paths

    # Setup
    (tmp_path / "01-acled.csv").write_text("dummy")
    (tmp_path / "02-acled.csv").write_text("dummy")
    (tmp_path / "consolidated_acled.csv").write_text("should be ignored")

    result = _get_lexically_sorted_csv_paths(tmp_path)
    assert len(result) == 2
    assert all("consolidated" not in p.name for p in result)


def test_invalid_naming_raises(tmp_path):
    from acled_concat.cli import _get_lexically_sorted_csv_paths

    # Setup
    (tmp_path / "01-acled.csv").write_text("dummy")
    (tmp_path / "acled_2022.csv").write_text("invalid")
    (tmp_path / "not-even-csv.txt").write_text("irrelevant")

    with pytest.raises(RuntimeError) as exc_info:
        _get_lexically_sorted_csv_paths(tmp_path)

    assert "Invalid files found" in str(exc_info.value)
    assert "acled_2022.csv" in str(exc_info.value)
    assert "not-even-csv.txt" not in str(exc_info.value)


def test_too_few_valid_files_raises(tmp_path):
    from acled_concat.cli import _get_lexically_sorted_csv_paths

    (tmp_path / "01-acled.csv").write_text("dummy")

    with pytest.raises(RuntimeError) as exc_info:
        _get_lexically_sorted_csv_paths(tmp_path)

    assert "At least two valid ACLED source files" in str(exc_info.value)
