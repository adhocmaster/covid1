# Using splitters:

## Installation:
1. Install dask
2. conda install -c conda-forge peewee
3. conda install -c anaconda psycopg2

### 1. Simple Day Splitter (simple-day-splitter.py)


*python simple-day-splitter.py --start_date=yyyy-mm-dd --end_date=yyyy-mm-dd --prefix=out --out_dir=outputDirectoryPath inputFilePath*

**Example:**

*python simple-day-splitter.py --start_date=2019-12-18 --end_date=2019-12-30 --prefix=out --out_dir=./data/splitted ./data/Santa_Clara_County_Pin_Report_1e6.tsv*

### 2. Extracting unique device ids ( extract-unique-device-ids.py )
*python simple-day-splitter.py --prefix=meta_ --out_dir=outputDirectoryPath inputFilePath*

*python extract-unique-device-ids.py --prefix=meta_ --out_dir=./data/splitted ./data/Santa_Clara_County_Pin_Report_1e6.tsv*