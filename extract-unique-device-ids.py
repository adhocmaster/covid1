import sys
import dask.dataframe as dd
from datetime import datetime, timedelta
exec(open("./lib/splitter.py").read())

# python extract-unique-device-ids.py --prefix=meta_ --out_dir=./data/splitted ./data/Santa_Clara_County_Pin_Report_1e6.tsv

if __name__ == "__main__":


    #1.********************Configuration****************
    args = getArgs()
    
    if 'inputFile' not in args:
        raise Exception("You must set an input file path at the end")

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

    outputPath = outputDir + outputPrefix + 'unique_devices.csv'

    print(f"output path: {outputPath}")

    inputFile = args['inputFile']

    #2. ********************Read data****************
    df = dd.read_csv(inputFile, sep='\t')
    df = df.rename(columns=columnNamesMap)

    # *******************start*********************
    cachedRows = {}
    for index, row in df.iterrows():
        if row['hid'] in cachedRows:
            cachedRows[row['hid']][1] += 1
        else:
            cachedRows[row['hid']] = [row['date'], 1]
            if len(cachedRows) % 10000 == 0:
                print(f"Ongoing: found {len(cachedRows)} unique device ids.") 
        

    print(f"Done: found {len(cachedRows)} unique device ids.") 
    
    with open(outputPath, 'w+') as file:
        file.write("hid,first_date,count")
        for hid in cachedRows:
            file.write(f"{hid},{cachedRows[hid][0]},{cachedRows[hid][1]}")