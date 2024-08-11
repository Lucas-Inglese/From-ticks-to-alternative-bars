import MetaTrader5 as mt5
from datetime import datetime, timedelta
import numpy as np
import pandas as pd
import os
from multiprocessing import Pool, cpu_count
mt5.initialize()

# PARAMETERS INITIALIZATION
broker = "Admiral-Markets"
symbol = "AUDCHF-Z"
start_year = 2014

def import_ticks(symbol,date_import):
    """
    Import all the ticks of a specific day for a specific symbol
    """
    
    # You have several columns in ticks format of MT5, we keep only the ones we need
    keeped_columns = ["time", "bid", "ask"]
    
    # import ticks
    ticks_chunk = mt5.copy_ticks_range(symbol, date_import - timedelta(days=1), date_import + timedelta(days=2), mt5.COPY_TICKS_ALL)
    
    # array to dataframe
    df_ticks = pd.DataFrame(ticks_chunk, columns=keeped_columns)
        
    # Convert number format of the date into date format
    df_ticks["time"] = pd.to_datetime(df_ticks["time"], unit="s")
    df_ticks = df_ticks.set_index("time")

    # BE CAREFUL, HERE ADMIRAL MARKETS USE EET TIME, your broker may use another one
    # We convert the EET time into GMT+0 time
    df_ticks.index = df_ticks.index.tz_localize('EET').tz_convert('GMT')
        
    # 2 CASES: there is data into our tick chunck or there is not
    try:
        return df_ticks.loc[date_import.strftime("%Y-%m-%d")]
    except:
        df_ticks = pd.DataFrame(columns=keeped_columns)
        df_ticks = df_ticks.set_index("time")
        return df_ticks


def maybe_make_dir(directory_path):
    """
    Create the folder path to store our data if it doesn't exist
    """
    if not os.path.exists(directory_path):
        os.makedirs(directory_path)
        print(f"The Folder '{directory_path}' has been created.")

        
def make_dates(start_year):
    dates = []
    now = datetime.now()
    for year in range(start_year,now.year+1):
        for month in range(1,13):
            for day in range(1,32):

                try:
                    if datetime(year,month,day) < now:
                        dates.append(datetime(year,month,day))
                except:
                    pass
    return dates


# Create a list of the dates for each day
dates = make_dates(start_year)

# Extract the current time before our extraction in order to analyze the time computation of our code
start = datetime.now()
with Pool(processes=int(cpu_count())) as pool:
    for date in dates:
        year, month, day = date.strftime("%Y"), date.strftime("%m"), date.strftime("%d")
        maybe_make_dir(f"DATA/{symbol}-{broker}/{year}/{month}")
        df = import_ticks(symbol,date)
        df.to_parquet(f"DATA/{symbol}-{broker}/{year}/{month}/{day}.parquet",compression='gzip') 

    
# Extract the current time after our extraction in order to analyze the time computation of our code
end = datetime.now()

# Compute the difference between start and end, it is the time computation
diff = (end-start).total_seconds()/60
print(f"{diff} minutes")
