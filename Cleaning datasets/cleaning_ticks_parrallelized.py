import numpy as np
import pandas as pd
from multiprocessing import Pool, cpu_count
from datetime import datetime
from functions_ticks_cleaning import basic_verifications, remove_outliers, traverse_save_paths
from tqdm import tqdm
import os
# PARAMETERS
k = 5 # Window size --> 2*k + 1
gamma = 0.000005  # Granularity (security threshold to avoid finding outliers which are not)
folder_path = "../../../EURUSD-Z-Admiral-Markets"
folder_end_path = "../../../EURUSD-Z-Admiral-Markets-clean-test"

def maybe_make_dir(directory_path):
    """
    Create the folder path to store our data if it doesn't exist
    """
    if not os.path.exists(directory_path):
        os.makedirs(directory_path)
        print(f"The Folder '{directory_path}' has been created.")

def process_file(path):
    df = pd.read_parquet(path)
    df = df.drop_duplicates()
    size = len(df)

    df_verified, nb_errors_negprice, nb_errors_bidsup = basic_verifications(df)
    df_clean, nb_outliers = remove_outliers(df_verified, k, gamma)

    save_path = path.replace(folder_path, folder_end_path)
    save_path_folder = save_path[:save_path.rfind('/')]
    maybe_make_dir(save_path_folder)
    df_clean.to_parquet(save_path, compression='gzip')
    return nb_errors_negprice / size * 100, nb_errors_bidsup / size * 100, nb_outliers / size * 100


if __name__ == '__main__':
    paths = traverse_save_paths(folder_path)
    start = datetime.now()

    # Initialize the list to save the data
    pct_errors_negprice = []
    pct_errors_bidsup = []
    pct_outliers = []

    # Prepare the multi-processing
    with Pool(processes=int(cpu_count()/2)) as pool:
        results = [pool.apply_async(process_file, (path,)) for path in paths]

        for result in tqdm(results):
            try:
                nb_errors_negprice, nb_errors_bidsup, nb_outliers = result.get()
                pct_errors_negprice.append(nb_errors_negprice)
                pct_errors_bidsup.append(nb_errors_bidsup)
                pct_outliers.append(nb_outliers)

            except:
                pass

    print(f"Negative or null prices: {np.mean(pct_errors_negprice)}%")
    print(f"Bid prices higher than Ask prices: {np.mean(pct_errors_bidsup)}%")
    print(f"Outliers: {np.mean(pct_outliers)}%")

    end = datetime.now()
    diff = (end - start).total_seconds()
    print(diff)
