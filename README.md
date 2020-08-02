# Using splitters:

## 1. Simple Day Splitter (simple-day-splitter.py)

### Installation:
1. Install dask

*python simple-day-splitter.py --start_date=yyyy-mm-dd --end_date=yyyy-mm-dd --prefix=out --out_dir=outputDirectoryPath inputFilePath*

**Example:**

*python simple-day-splitter.py --start_date=2019-12-18 --end_date=2019-12-30 --prefix=out --out_dir=./data/splitted ./data/Santa_Clara_County_Pin_Report_1e6.tsv*