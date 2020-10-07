import sys
import dask.dataframe as dd
from datetime import datetime, timedelta
exec(open("./lib/splitter.py").read())
exec(open("./lib/tzMap.py").read())
exec(open("./lib/distance.py").read())

# python simple-day-splitter.py --start_date=2019-12-18 --end_date=2019-12-30 --prefix=out --out_dir=./data/splitted ./data/Santa_Clara_County_Pin_Report_1e6.tsv

if __name__ == "__main__":


    #1.********************Configuration****************
    args = getArgs()
    
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
    df = prepareTSVDF(inputFile)
    
    
    #3. ********************Start****************
    dates = getDateStrings(startDate, endDate)
    stopDateStr = getNDaysLaterString(endDate)

    print(f"Search will stop at the first instance on {stopDateStr}")
    initFilesWithGridCols(dates)

    # we will caches rows for each date until flushed to files. keys: dates, values: list of rows
    cachedRows = getInitializedCacheRows(dates)
    flushSize = 100000 # try to write after parsing flushSize number of rows.
    progress = 0

    count = 0 # to track number of rows in cache   
    for index, row in df.iterrows():

        if row['date'] == stopDateStr:
            break
        
        if row['date'] in dates:
            progress = progress + 1
            row['tz'] = getTimeZoneId(row['tz'])
            cachedRows[row['date']].append(row)
            
        count += 1
        if count >= flushSize:
            flushDataToFilesWithGrid(cachedRows)
            print(f"wrote {progress} lines.")
            count = 0

    flushDataToFilesWithGrid(cachedRows, force=True)
    print(f"wrote {progress} lines.")
            




         


