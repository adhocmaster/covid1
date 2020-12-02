import sys
import dask.dataframe as dd
from datetime import datetime, timedelta
exec(open("./lib/splitter.py").read())
exec(open("./lib/tzMap.py").read())
exec(open("./lib/distance.py").read())


# save the last imported indice in a file so that we can restart.
