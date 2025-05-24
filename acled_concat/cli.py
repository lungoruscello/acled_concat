import argparse
import logging
import re
from pathlib import Path

import pandas as pd
from tqdm.auto import tqdm
from iso_map import ISO_MAP

ROOT_DIR = Path(__file__).parent

RETAINED_COLS = [
    "event_id_cnty", "iso", "iso3", "event_date", "year",
    "time_precision", "event_type", "sub_event_type",
    "actor1", "assoc_actor_1", "inter1", "actor2", "assoc_actor_2",
    "inter2", "interaction", "region", "country",
    "admin1", "admin2", "admin3", "location", "latitude", "longitude",
    "geo_precision", "source", "source_scale", "notes", "fatalities",
    "timestamp", "_orig_fname"
]

LOG = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


def concat(source_dir):
    """
    Consolidate and deduplicate ACLED CSV files in a directory.

    Loads multiple ACLED CSV files from `source_dir`, sorts them lexically
    by their numerical filename prefix (e.g., '01-acled_*.csv'), and writs
    the result to `consolidated_acled.csv`.

    Consecutive input files are expected to have temporal overlap in their
    `event_date` values, to avoid accidental data gaps in the consolidated
    output file. Duplicated event records are resolving by always keeping
    the record that has the most recent `timestamp` field.

    Parameters
    ----------
    source_dir : str or Path
        Directory containing input ACLED CSV files.

    Returns
    -------
    pd.DataFrame
        The consolidated ACLED dataset.
    """
    source_dir = Path(source_dir)
    out_path = source_dir / "consolidated_acled.csv"

    shard_paths = _get_lexically_sorted_csv_paths(source_dir)

    LOG.info("Loading CSVs...")
    load_pbar = tqdm(shard_paths, leave=False)
    dataframes = [_load_and_format_df(path) for path in load_pbar]

    LOG.info("Concatenating...")
    consolidated = dataframes[0]
    concat_pbar = tqdm(dataframes[1:], leave=False)
    for next_df in concat_pbar:
        consolidated = _concat_two_dfs(consolidated, next_df)
        assert consolidated.duplicated('event_id_cnty').sum() == 0

    LOG.info(f"Writing result to {out_path}...")
    consolidated.to_csv(out_path, index=False)

    LOG.info(f"Done. {len(shard_paths)} ACLED files consolidated.")
    return consolidated


def _get_lexically_sorted_csv_paths(source_dir):
    """
    Return a list of valid ACLED CSV file paths in `source_dir`, sorted by their
    numerical prefix.

    Files must be named using the pattern `NN-acled_<description>.csv`,
    where `NN` is a two-digit numerical prefix, with higher prefix numbers
    indicating ACLED files covering later date ranges.

    Parameters
    ----------
    source_dir : str or Path
        Path to the directory containing the input CSV files.

    Returns
    -------
    List[Path]
        Sorted list of validated CSV file paths.

    Raises
    ------
    RuntimeError
        - If fewer than two valid CSV files are found.
        - If `source_dir` contains one or more CSV files with invalid names
          (except for an old output file)
    """
    all_csvs = list(Path(source_dir).glob("*.csv"))
    pattern = re.compile(r"^(\d{2})-acled.*\.csv$")

    valid_files = []
    invalid_files = []

    for path in all_csvs:
        if path.name.startswith('consolidated_acled'):
            continue  # skip known output files

        match = pattern.match(path.name)
        if match:
            prefix = int(match.group(1))
            valid_files.append((prefix, path))
        else:
            invalid_files.append(path.name)

    if invalid_files:
        msg = (
                f"Found {len(invalid_files)} invalid ACLED source file(s).\n"
                "Each single CSV file in 'source_dir' (other than known output files "
                "starting with 'consolidated_acled') must follow the naming pattern:\n"
                "    NN-acled_<description>.csv\n"
                "Examples:\n"
                "  - 01-acled_2021_download.csv\n"
                "  - 02-acled_2022_update.csv\n"
                "  - 03-acled_2023_another_update.csv\n"
                "Invalid files found:\n"
                + "\n".join(f"  - {name}" for name in invalid_files)
        )
        raise RuntimeError(msg)

    if len(valid_files) < 2:
        raise RuntimeError("At least two valid ACLED source files are required.")

    sorted_paths = [p for _, p in sorted(valid_files, key=lambda x: x[0])]
    return sorted_paths


def _concat_two_dfs(df1, df2):
    """
    Concatenate two ACLED DataFrames with overlapping date ranges and remove
    duplicated event records.

    Assumes each event is uniquely identified by `event_id_cnty`, and that
    `timestamp` reflects the latest edit time. Events with the same ID are
    deduplicated by retaining the version with the most recent timestamp.

    Parameters
    ----------
    df1 : pd.DataFrame
        First ACLED DataFrame to concatenate.
    df2 : pd.DataFrame
        Second ACLED DataFrame to concatenate.

    Returns
    -------
    pd.DataFrame
        The merged, deduplicated DataFrame.

    Raises
    ------
    RuntimeError
        If either input is empty or if the shards are not temporally contiguous or overlapping.
    """
    if df1.empty or df2.empty:
        raise RuntimeError("Cannot merge empty ACLED shards.")

    max1 = df1.event_date.max()
    min2 = df2.event_date.min()

    if max1 < min2:
        raise RuntimeError(
            "ACLED shards must have overlapping dates to avoid data gaps."
        )

    df = pd.concat([df1, df2])
    df.sort_values(["event_id_cnty", "timestamp"], inplace=True)
    df.drop_duplicates(["event_id_cnty"], keep="last", inplace=True)
    df.sort_values(["event_date", "event_id_cnty"], inplace=True)
    df.reset_index(drop=True, inplace=True)
    return df


def _load_and_format_df(path):
    """
    Load and format a single ACLED CSV file.

    Parses dates and appends a provenance column (`_orig_fname`) for traceability.
    Delegates standardisation and schema validation to `_format_df`.

    Parameters
    ----------
    path : Path
        Path to a single ACLED CSV file.

    Returns
    -------
    pd.DataFrame
        A cleaned and formatted DataFrame.
    """
    df = pd.read_csv(path, low_memory=False, parse_dates=["event_date"])
    df['_orig_fname'] = path.name  # track provenance
    return _format_df(df)


def _format_df(df):
    """
    Standardise an ACLED dataframe by adding a missing ISO3 column if needed.

    Ensures all required columns exist, adds missing `iso3` codes (if needed),
    and enforces a fixed schema by subsetting to a known column set.

    Parameters
    ----------
    df : pd.DataFrame
        Input ACLED DataFrame.

    Returns
    -------
    pd.DataFrame
        Standardised ACLED DataFrame with `RETAINED_COLS`.

    Raises
    ------
    ValueError
        If expected columns are missing or ISO3 codes cannot be mapped.
    """

    # very old ACLED files only provide numerical country codes, which we
    # convert to three-letter country codes for compatibility
    if "iso3" not in df.columns:
        if unknown := set(df.iso) - set(ISO_MAP.keys()):
            raise ValueError(
                "The following numerical ISO codes could not be mapped "
                f"to a three-letter equivalent: {unknown}."
            )

        df["iso3"] = df["iso"].map(ISO_MAP)

    # ensure required columns exist
    if missing_cols := set(RETAINED_COLS) - set(df.columns):
        raise ValueError(f"Missing expected columns: {missing_cols}")

    return df[RETAINED_COLS].copy()


def main():
    """
    Command-line interface for ACLED CSV consolidation.

    Parses command-line arguments and triggers the consolidation process.

    Raises
    ------
    SystemExit
        If an error occurs during processing.
    """
    parser = argparse.ArgumentParser(
        description="Consolidate multiple ACLED CSV files into one unified dataset."
    )

    parser.add_argument(
        "source_dir",
        type=str,
        help="Directory containing ACLED source files"
    )

    args = parser.parse_args()

    try:
        concat(args.source_dir)
    except Exception as e:
        import traceback
        LOG.error(f"Failed to consolidate ACLED data: {e}")
        LOG.info("Full traceback:", exc_info=True)
        exit(1)


if __name__ == "__main__":
    main()
