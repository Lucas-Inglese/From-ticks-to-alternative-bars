import os
import numpy as np
import pandas

def basic_verifications(df):
    # 1. Sort index by chronological order
    df = df.sort_index(ascending=True)

    # 2. We reset thte index because the dates are not unique (essential to drop only the wrong ticks not some others on the same date)
    df = df.reset_index(drop=False)

    # 3. We remove the rows when we have unsual cotation like price below 0 or egual 0
    index_errors_negprice = list(df.loc[(df["bid"] <= 0) | (df["ask"] <= 0)].index)
    df = df.drop(index_errors_negprice, axis=0, inplace=False)
    nb_errors_negprice = len(index_errors_negprice)

    # 4. We remove the rows when we have bid price above ask price
    index_errors_bidsup = list(df.loc[df["ask"] < df["bid"]].index)
    df = df.drop(index_errors_bidsup, axis=0, inplace=False)
    nb_errors_bidsup = len(index_errors_bidsup)

    # 5. Replace the date as index
    df = df.set_index("time")

    return df, nb_errors_negprice, nb_errors_bidsup


def is_outlier(s, k, gamma):
    s = s.reset_index(drop=True)
    center_value = s.iloc[k]
    trimmed_s = s[~s.index.isin([k])]  # We remove the current value
    trimmed_mean = trimmed_s.mean()
    std_dev = trimmed_s.std()
    return abs(center_value - trimmed_mean) > 3 * std_dev + gamma


def remove_outliers(df, k, gamma):
    # 0. MANDATORY to remove only the wrong cotations
    df = df.reset_index(drop=False)

    # 1. We extract the index of the bid outliers
    outliers_mask_bid = df['bid'].rolling(window=2 * k + 1, center=True).apply(
        lambda s: is_outlier(s, k, gamma), raw=False)
    outliers_index_bid = outliers_mask_bid[outliers_mask_bid == 1].index

    # 2. We extract the index of the ask outliers
    outliers_mask_ask = df['ask'].rolling(window=2 * k + 1, center=True).apply(
        lambda s: is_outlier(s, k, gamma), raw=False)
    outliers_index_ask = outliers_mask_ask[outliers_mask_ask == 1].index

    # 3. Drop the outliers
    outliers_index = list(outliers_index_bid)
    outliers_index.extend(list(outliers_index_ask))
    outliers_index = list(set(outliers_index))  # we keep only the unique index

    df_clean = df.drop(outliers_index, axis=0, inplace=False)
    nb_outliers = len(outliers_index)

    df_clean = df_clean.set_index("time")

    return df_clean, nb_outliers


def traverse_save_paths(folder):
    paths = []
    for root, dirs, files in os.walk(folder):
        for file_name in files:
            if file_name.endswith('.parquet'):
                full_path = os.path.join(root, file_name)
                paths.append(full_path)
    return paths


def maybe_make_dir(directory_path):
    """
    Create the folder path to store our data if it doesn't exist
    """
    if not os.path.exists(directory_path):
        os.makedirs(directory_path)
        print(f"The Folder '{directory_path}' has been created.")

