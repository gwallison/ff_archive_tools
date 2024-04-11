"""This set of helper functions is used to do things like point to the most recent
FF archive, etc."""

import pandas as pd
import os
import shutil

arc_dir = r"C:\MyDocs\integrated\archive"
daily_dir = os.path.join(arc_dir,'daily')
meta_dir = os.path.join(arc_dir,'meta')

def get_most_recent_archive(final_dir):
    # copies the file into final_dir and returns the file's name
    lst = os.listdir(daily_dir)
    lst.sort()
    fn = os.path.join(daily_dir,lst[-1])
    shutil.copy2(fn,final_dir)
    fn = os.path.join(meta_dir,'pub_delay_df.parquet')
    shutil.copy2(fn,final_dir)
    return lst[-1]
    