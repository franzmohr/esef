# esef

esef is a package that can be used to download and -- in the future -- analyse ESEF data.

## Installation

The package is stil under development, but you can install the development version from **source**.

### Install from Source (GitHub)

```bash
git clone https://github.com/franzmohr/esef.git
cd esef
pip install .
```

## Usage

After installation, you can use `esef` to download data from https://filings.xbrl.org/.

### Application 1: Downloading Report Packages

```python
from esef import esef

# Set root direcetory, where all the data should be stored
dl_folder = "/path/to/data/lake"

# Make sure all the neccessary (sub-)directories exist
esef.create_directory_tree(dl_folder)

# Get metadata on all the filings that are available for a country
filings = esef.available_filings("AT")

# Filter for a selected language
filings = filings.filter(lang = "de")

# Download the Report Packages
esef.download_report_package(dl_folder, filings)
```

### Application 2:

**Under development**
