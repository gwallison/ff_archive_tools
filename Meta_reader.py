"""


@author: GAllison

This module is used to read the meta files in from a FracFocus zip 
of CSV files to create a set of archives for analysis.  One output from
this module is a data set with earliest pub date, by APINumber and end_date.

"""
import zipfile
import re
import pandas as pd
import os


class Make_Meta_From_Archive():
    def __init__(self,in_name='testData.zip',
                 zipdir='./working/',
                 out_dir=r"C:\MyDocs\integrated\archive\meta"):
        self.in_name = in_name
        self.zipdir = zipdir
        self.zname = os.path.join(zipdir,in_name)
        self.out_dir = out_dir
        self.delay_fn = os.path.join(self.out_dir,'pub_delay_df.parquet')
        self.pub_delay_info_fn = os.path.join(self.out_dir,'pub_delay_info.txt')

    def import_meta_df(self, in_name='',verbose=True):
        """Finds meta files in the archive, checks for which version (V3 orV4) and produces the df.  All
        columns are kept in "str" dtype.
        """
        if in_name:
            self.zname = os.path.join(self.zipdir,in_name) # otherwise, keep class level version
            
        dflist = []
        version = 4
        with zipfile.ZipFile(self.zname) as z:
            inf = []
            for fn in z.namelist():
                #  always start with this prefix...
                if fn[:14]=='DisclosureList':
                    # need to extract number of file to correctly order them
                    num = int(re.search(r'\d+',fn).group())
                    inf.append((num,fn))
            if len(inf)==0: # must be the other Version
                version = 3
                for fn in z.namelist():
                    #  always start with this prefix...
                    if fn[:14]=='registryupload':
                        # need to extract number of file to correctly order them
                        num = int(re.search(r'\d+',fn).group())
                        inf.append((num,fn))
            inf.sort()
            infiles = [x for _,x in inf]  # now we have a well-sorted list
            
            for fn in infiles[0:]:
                with z.open(fn) as f:
                    if verbose:
                        print(f' -- processing {fn}')
                    t = pd.read_csv(f,low_memory=False, dtype='str') # read everything as string
                    # print(t.columns)
                    dflist.append(t)
        final = pd.concat(dflist,sort=True)
        final['bulk_version'] = version  # keep track of which kind of bulk download it is, 3 or 4
        final['date'] = pd.to_datetime(final.JobEndDate,errors='coerce',format= "%m/%d/%Y %H:%M:%S %p")
        final.date = final['date'].dt.floor('D')  # keep just date
        
        # convert DisclosureId or pKey to a common name
        final = final.rename({'DisclosureId':'disc_ID',
                             'pKey':'disc_ID'},axis=1)
        
        final.reset_index(drop=True,inplace=True) #  single integer as index
        return final
        
    def create_pub_delay_df(self,verbose=False):
        # Creates the df from scratch (!!erases any previous ones!!).  Uses just the 
        # single zipdir, presumably the main archive source.
        
        ldir = os.listdir(self.zipdir)
        ldir.sort() # make sure they are in chronological order
        print(ldir[-10:])
        last_date = 'unknown'
        pubdf = pd.DataFrame()
        # erase old archive date list
        try:
            os.remove(self.pub_delay_info_fn)
        except:
            print(f'Old archive list ({self.pub_delay_info_fn}) not available to delete.')
            
        for fn in ldir:
            if fn[:11] == 'ff_archive_':
                fdate = fn[11:-4]
                df = self.import_meta_df(in_name=fn,verbose=verbose)
                df = df[['APINumber','date','disc_ID','bulk_version']]
                df['df'] = 'curr'
                print(len(df))
                ccat = pd.concat([pubdf,df],sort=True)
                ccat = ccat[~ccat.duplicated(keep=False,subset=['APINumber','date','disc_ID'])]
                ccat = ccat[ccat.df=='curr'] # ignore removals
                ccat['earliest_poss_date'] = last_date
                ccat['first_detected'] = fdate
                if len(pubdf)>0:
                    pubdf = pd.concat([pubdf,ccat],sort=True)
                else:
                    pubdf = ccat.copy() # for first iteration
                pubdf.df = 'done'
                print(fdate,len(ccat),len(pubdf))
                last_date = fdate
                with open(self.pub_delay_info_fn,'a') as f:
                    f.write(last_date+'\n')
        pubdf.earliest_poss_date = pd.to_datetime(pubdf.earliest_poss_date,errors='coerce',format= "%Y-%m-%d")
        pubdf.first_detected = pd.to_datetime(pubdf.first_detected,errors='coerce',format= "%Y-%m-%d")
        pubdf['pub_delay_days'] = (pubdf.earliest_poss_date - pubdf.date).dt.days
        pubdf.to_parquet(self.delay_fn)
        return pubdf
                

    def update_pub_delay_df(self,archive_dirs=[],verbose=False):
        # Looks across dirs for archive files not yet processed.  Ignores files already done.
        pubdf = pd.read_parquet(self.delay_fn)
        with open(self.pub_delay_info_fn,'r') as f:
            done_lst = f.read().split('\n')
        done_lst[:] = [x for x in done_lst if x != ""] # remove all empty lines
        for arc_dir in archive_dirs:
            self.zipdir = arc_dir
            print(f'Searching in {arc_dir} for new archives')
            ldir = os.listdir(arc_dir)            
            ldir.sort()
            # print(ldir[-10:])
            for fn in ldir:
                if fn[:11] == 'ff_archive_':
                    fdate = fn[11:-4]
                    if not fdate in done_lst:
                        df = self.import_meta_df(in_name=fn,verbose=verbose)
                        df = df[['APINumber','date','disc_ID','bulk_version']]
                        df['df'] = 'curr'
                        print(len(df))
                        ccat = pd.concat([pubdf,df],sort=True)
                        ccat = ccat[~ccat.duplicated(keep=False,subset=['APINumber','date','disc_ID'])]
                        ccat = ccat[ccat.df=='curr'] # ignore removals
                        ccat['earliest_poss_date'] = done_lst[-1]
                        ccat['first_detected'] = fdate
                        
                        if (len(pubdf)>0)&(len(ccat)>0):
                            pubdf = pd.concat([pubdf,ccat],sort=True)
                        else:
                            pubdf = ccat.copy() # for first iteration
                        pubdf.df = 'done'
                        print(fdate,len(ccat),len(pubdf))
                        with open(self.pub_delay_info_fn,'a') as f:
                            f.write(fdate+'\n')
                        done_lst.append(fdate)
        pubdf.earliest_poss_date = pd.to_datetime(pubdf.earliest_poss_date,errors='coerce',format= "%Y-%m-%d")
        pubdf.first_detected = pd.to_datetime(pubdf.first_detected,errors='coerce',format= "%Y-%m-%d")
        pubdf['pub_delay_days'] = (pubdf.earliest_poss_date - pubdf.date).dt.days
        pubdf.to_parquet(self.delay_fn)
        return pubdf
                     

if __name__ == '__main__':
    obj = Make_Meta_From_Archive(zipdir = r"D:\archives\FF bulk data",in_name=r"ff_archive_2018-08-28.zip")
