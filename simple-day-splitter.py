import sys
import dask.dataframe as dd
from datetime import datetime, timedelta
exec(open("./lib/splitter.py").read())

# python simple-day-splitter.py --start_date=2019-12-18 --end_date=2019-12-30 --prefix=out --out_dir=./data/splitted ./data/Santa_Clara_County_Pin_Report_1e6.tsv

if __name__ == "__main__":


    #1.********************Configuration****************
    args = getArgs()
    
    if 'inputFile' not in args:
        raise Exception("You must set an input file path at the end")
    
    if '--start_date' not in args:
        raise Exception("--start_date not set")
    if '--end_date' not in args:
        raise Exception("--end_date not set")

    if '--prefix' in args:
        outputPrefix = args['--prefix']
    else:
        outputPrefix = 'out_'

    if '--out_dir' in args:
        outputDir = args['--out_dir']
        if outputDir.endswith("/") is False:
            outputDir += "/"
    else:
        outputDir = './data/splitted' + '/'

    print(f"output directory: {outputDir}")

    startDate = args['--start_date']
    endDate = args['--end_date']
    inputFile = args['inputFile']
    flushThresholdPerDate = 10000 # We will cache rows for a date in memory before writing to the files.

    #2. ********************Read data****************
    df = dd.read_csv(inputFile, sep='\t')
    df = df.rename(columns=columnNamesMap)
    
    
    #3. ********************Start****************
    dates = getDateStrings(startDate, endDate)
    initFiles(dates)

    # we will caches rows for each date until flushed to files. keys: dates, values: list of rows
    cachedRows = getInitializedCacheRows(dates)
    flushSize = 100000 # try to write after parsing flushSize number of rows.
    progress = 0

    count = 0 # to track number of rows in cache   
    for index, row in df.iterrows():
        
        if row['date'] in dates:
            progress = progress + 1
            cachedRows[row['date']].append(row)
            
        count += 1
        if count >= flushSize:
            flushDataToFiles(cachedRows)
            print(f"wrote {progress} lines.")
            count = 0

    flushDataToFiles(cachedRows, force=True)
    print(f"wrote {progress} lines.")
            




         


