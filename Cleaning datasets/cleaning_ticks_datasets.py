import numpy as np
import pandas as pd
from functions_ticks_cleaning import *
from datetime import datetime

# PARAMETERS
k = 5  # Window size --> 2*k + 1
gamma = 0.000035  # Granularity (security threshold to avoid finding outliers which are not)
folder_path = "../../../EURUSD-Z-Admiral-Markets"
folder_end_path = "../../../EURUSD-Z-Admiral-Markets-clean"

pct_errors_negprice = []
pct_errors_bidsup = []
pct_outliers = []

# RUN
paths = traverse_save_paths(folder_path)
start = datetime.now()

for path in paths:
    print(path)

    # Initialize the run
    df = pd.read_parquet(path)
    size = len(df)

    # Clean the dataset
    df_verified, nb_errors_negprice, nb_errors_bidsup = basic_verifications(df)
    df_clean, nb_outliers = remove_outliers(df_verified, k, gamma)

    # Store the data with a try excpet because sometimes size=0
    try:
        pct_errors_negprice.append(nb_errors_negprice/size*100)
        pct_errors_bidsup.append(nb_errors_bidsup/size*100)
        pct_outliers.append(nb_outliers/size*100)

    except:
        pass

    # Save cleaned data
    save_path = path.replace(folder_path, folder_end_path)
    save_path_folder = save_path[:save_path.rfind('/')]
    maybe_make_dir(save_path_folder)
    df_clean.to_parquet(save_path, compression='gzip')

print(f"Negative or null prices - Average {np.mean(pct_errors_negprice)}% \t Max {np.max(pct_errors_negprice)}%")
print(f"Bid prices higher than Ask prices - Average {np.mean(pct_errors_bidsup)}% \t Max {np.max(pct_errors_bidsup)}%")
print(f"Outliers: - Average {np.mean(pct_outliers)}% \t Max {np.max(pct_outliers)}%")

end = datetime.now()
diff = (end-start).total_seconds()

# return the number of seconds taken
print(diff)