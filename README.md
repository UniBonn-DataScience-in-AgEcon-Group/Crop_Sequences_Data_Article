# Creating crop sequences from largest overlaps

This repository presents a Python script allowing to generate crop sequences based on largest area overlaps. The script is used for the data article "A crop sequence dataset of the German federal state of North Rhine-Westphalia from 2019-2024".

## Prerequisites

- Python 3.6 or higher
- [uv](https://docs.astral.sh/uv/#installation)
- [git](https://git-scm.com/)

## Installation

1. Clone the repository
2. Install the required Python packages by running `uv sync`

## Usage

The easiest way to get started is to run the `download.sh` script to get the required input data. On macOS or Linux, you can run the script by executing the following command in the terminal:

```bash
make download
```

On Windows, open a Bash shell (e.g. Git Bash) and run the following command:

```bash
sh ./download.sh
```

After downloading the input data, you can run the script to generate the crop sequences:

```bash
make join 
```

or manually using uv:

```bash
uv run run join-plots.py --cur "./2024.parquet" --hist "./hist" --out "./Crop_Sequences_NRW_2019_2024.parquet"
```
