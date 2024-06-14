"""This set of helper functions is used to do things like point to the most recent
FF archive, etc."""

import pandas as pd
import os
import shutil

# arc_dir = r"C:\MyDocs\integrated\archive"
arc_dir = r"D:\openFF_archive"
bulk_dir = os.path.join(arc_dir,'FF_bulk_data')
meta_dir = os.path.join(arc_dir,'pub_dates')

def get_most_recent_archive(final_dir):
    # copies the file into final_dir and returns the file's name
    lst = os.listdir(bulk_dir)
    # keep only "ff_archive_" files
    clst = []
    for fn in lst:
        if fn[:11]=='ff_archive_':
            clst.append(fn)
    clst.sort()
    fn = os.path.join(bulk_dir,clst[-1])
    shutil.copy2(fn,final_dir)
    fn = os.path.join(meta_dir,'pub_delay_df.parquet')
    shutil.copy2(fn,final_dir)
    return clst[-1]
    