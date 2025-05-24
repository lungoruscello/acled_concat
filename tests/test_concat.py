import numpy as np
import pandas as pd
import pytest

# our tests use a simplified ACLED dataframe schema
TEST_RETAINED_COLS = [
    "event_id_cnty", "timestamp", "event_date", "iso", "iso3", "_orig_fname"
]


TEST_ISO_MAP = {123: 'ABC', 456: 'XYZ'}


# Note: The three data fixtures below create temporary CSV files in a
# shared temp directory (`tmp_path`), which pytest provides for isolated
# test file handling. When storing mocked ACLED CSV files in that directory,
# `cli.concat(tmp_path)` will see them as inputs.

@pytest.fixture
def mock_shard1(tmp_path):
    shard1 = pd.DataFrame({
        "event_id_cnty": ['ABC01', 'ABC02', 'XYZ01'],
        "event_date": [
            pd.Timestamp('1 Jan 2020'),
            pd.Timestamp('31 Dec 2020'),
            pd.Timestamp('31 Dec 2020')
        ],
        "timestamp": [1, 1, 1],
        "iso": [123, 123, 456],
    })
    fname = tmp_path / f"01-acled_mock.csv"
    shard1.to_csv(fname, index=False)
    return fname


@pytest.fixture
def mock_shard2(tmp_path):
    shard2 = pd.DataFrame({
        "event_id_cnty": ['ABC02', 'ABC03', 'XYZ01', 'XYZ02'],
        "event_date": [
            pd.Timestamp('31 Dec 2020'),
            pd.Timestamp('1 Feb 2021'),
            pd.Timestamp('31 Dec 2020'),
            pd.Timestamp('1 Feb 2021')
        ],
        "timestamp": [9, 9, 9, 9],
        "iso": [123, 123, 456, 456],
    }
    )
    fname = tmp_path / f"02-acled_mock.csv"
    shard2.to_csv(fname, index=False)
    return fname


@pytest.fixture
def mock_shard3(tmp_path):
    shard3 = pd.DataFrame({
        "event_id_cnty": ['ABC99'],
        "event_date": [pd.Timestamp('31 Dec 2099')],
        "timestamp": [pd.Timestamp('31 Dec 2099')],
        "iso": [123],
    }
    )
    fname = tmp_path / f"03-acled_mock.csv"
    shard3.to_csv(fname, index=False)
    return fname


def test_load_and_format_df(tmp_path, mock_shard1):
    from acled_concat import cli

    # patch RETAINED_COLS and ISO_MAP to match test schema and data
    cli.RETAINED_COLS = TEST_RETAINED_COLS
    cli.ISO_MAP = TEST_ISO_MAP

    df = cli._load_and_format_df(mock_shard1)

    # basic structure checks
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 3
    assert set(df.columns) == {"event_id_cnty", "event_date", "timestamp", "iso", "iso3", '_orig_fname'}

    # ISO3 mapping check
    assert np.all(df["iso3"] == ["ABC", "ABC", "XYZ"])

    # datetime parsing check
    assert pd.api.types.is_datetime64_any_dtype(df["event_date"])


def test_concat_merges_and_deduplicates(tmp_path, mock_shard1, mock_shard2):
    from acled_concat import cli

    cli.RETAINED_COLS = TEST_RETAINED_COLS
    cli.ISO_MAP = TEST_ISO_MAP

    result_df = cli.concat(tmp_path)

    # validate de-duplication
    assert len(result_df) == 5
    assert np.all(result_df.event_id_cnty == ['ABC01', 'ABC02', 'XYZ01', 'ABC03', 'XYZ02'])
    assert np.all(result_df.iso3 == ['ABC', 'ABC', 'XYZ', 'ABC', 'XYZ'])

    # validate newer duplicates were retained
    expected_fnames = ['01-acled_mock.csv'] + 4 * ['02-acled_mock.csv']
    assert np.all(result_df['_orig_fname'] == expected_fnames)
    assert len(result_df[result_df.timestamp == 1]) == 1
    assert len(result_df[result_df.timestamp == 9]) == 4


def test_concat_raises_without_temporal_overlap(tmp_path, mock_shard1, mock_shard3):
    from acled_concat import cli

    cli.RETAINED_COLS = TEST_RETAINED_COLS
    cli.ISO_MAP = TEST_ISO_MAP

    with pytest.raises(RuntimeError) as exc_info:
        cli.concat(tmp_path)

    assert "shards must have overlapping dates" in str(exc_info.value).lower()
