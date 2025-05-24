# ACLED Concat

A simple Python tool to consolidate multiple data files from the [ACLED project](https://acleddata.com/) into a single, 
deduplicated dataset.

## Overview

The ACLED project provides widely-used data on reported conflict events worldwide. With a standard account, 
however, you can only download data for the most recent three years. Users who are interested in long-term trends 
hence often download a fresh batch of ACLED data every few months – and gradually build up their own historical backlog of 
ACLED files going back much further in time.

This script automates the ensuing data consolidation process: it loads multiple ACLED CSV files from disk and merges 
them while removing duplicate records. Specifically, the most recent data update for a given event is kept (as identified 
using ACLED’s official `timestamp` field). To prevent accidental data gaps, the script checks that the range of event 
dates in each successive input file partially overlaps with the range of event dates in the previous file.


The final output is a single deduplicated CSV file containing all events, ready for analysis.

## Installation

Installation is best done using [`pipx`](https://pipxproject.github.io/pipx/). 
This allows you to run the script as a command-line tool without polluting your global Python environment or 
having to manage virtual environments manually.


To install **ACLED Concat** using pipx, run:

```bash
pipx install git+https://github.com/lungoruscello/acled_concat.git
```

This will make the `acled-concat` command globally available, while isolating its dependencies.

## Usage

```bash
acled-concat /directory/with/acled/csv/files
```

The script will scan the directory, consolidate the CSVs, and write the output to `consolidated_acled.csv` (within the 
same directory).

## File Naming Convention

Input files must be named as:

> NN-acled_\<description>.csv
 
where `NN` is a two-digit numeric prefix indicating the file order (older to newer). For example:

* `01-acled_2021_download.csv`

* `02-acled_2022_update.csv`

* `03-acled_2023_another_update.csv`

## Requirements

* Python 3.9 or higher
* pandas
* tqdm

## Licence

MIT Licence. See [LICENSE.txt](https://github.com/lungoruscello/acled_concat/blob/master/LICENSE.txt) for details.