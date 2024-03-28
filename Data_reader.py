
# -*- coding: utf-8 -*-
"""
Created on Wed Apr 17 10:15:03 2019

@author: GAllison

This module is used to read the data files in from a FracFocus zip 
of CSV files to create a set of archives for analysis.  No cleanup is
done; this is about examining the raw data.

"""
import zipfile
import re
import pandas as pd
import os


class Make_DF_From_Archive():
    def __init__(self,in_name='testData.zip',
                 zipdir='./working/',
                 outset_dir='./working/'):
        self.zipdir = zipdir
        self.zname = os.path.join(zipdir,in_name)
        self.outset_dir = outset_dir
    
    def import_raw_FFV3(self, verbose=True):
        """
        """
        dflist = []
        with zipfile.ZipFile(self.zname) as z:
            inf = []
            for fn in z.namelist():
                # the files in the FF archive with the Ingredient records
                #  always start with this prefix...
                if fn[:17]=='FracFocusRegistry':
                    # need to extract number of file to correctly order them
                    num = int(re.search(r'\d+',fn).group())
                    inf.append((num,fn))
                    
            inf.sort()
            infiles = [x for _,x in inf]  # now we have a well-sorted list
            #print(self.startfile,self.endfile)
            for fn in infiles[0:]:
                with z.open(fn) as f:
                    if verbose:
                        print(f' -- processing {fn}')
                    t = pd.read_csv(f,low_memory=False,
                                    dtype= {'APINumber':'str', 
                                            'CASNumber':'str', 
                                            'ClaimantCompany':'str', 
                                            'CountyName':'str',
                                            'CountyNumber':'str', 
                                            'DTMOD':'str', 
                                            'DisclosureKey':'str', 
                                            'FFVersion':'str', 
                                            'FederalWell':'str',
                                            'IndianWell':'str', 
                                            'IngredientComment':'str', 
                                            'IngredientKey':'str', 
                                            'IngredientMSDS':'str',       
                                            'IngredientName':'str', 
                                            'IsWater':'str', 
                                            'JobEndDate':'str', 
                                            'JobStartDate':'str', 
                                            'Latitude':'str',
                                            'Longitude':'str', 
                                            'MassIngredient':'str', 
                                            'OperatorName':'str', 
                                            'PercentHFJob':'str',
                                            'PercentHighAdditive':'str', 
                                            'Projection':'str', 
                                            'Purpose':'str', 
                                            'PurposeIngredientMSDS':'str',
                                            'PurposeKey':'str', 
                                            'PurposePercentHFJob':'str', 
                                            'Source':'str', 
                                            'StateName':'str',       
                                            'StateNumber':'str', 
                                            'Supplier':'str', 
                                            'SystemApproach':'str', 
                                            'TVD':'str',
                                            'TotalBaseNonWaterVolume':'str', 
                                            'TotalBaseWaterVolume':'str', 
                                            'TradeName':'str',
                                            'UploadKey':'str', 
                                            'WellName':'str', 
                                            'ingKeyPresent':'str', 
                                            'raw_filename':'str', 
                                            'reckey':'str'}
                                    )
                    
                    # we need an indicator of the presence of IngredientKey
                    # whitout keeping the whole honking thing around
                    t['ingKeyPresent'] = t.IngredientKey.notna()
                    
                    t['raw_filename'] = fn # helpful for manual searches of raw files
                    dflist.append(t)
        final = pd.concat(dflist,sort=True)
        final.reset_index(drop=True,inplace=True) #  single integer as index
        final['reckey'] = final.index.astype(int)
        assert(len(final)==len(final.reckey.unique()))
        return final
        
    def import_raw_FFV4(self, verbose=True):
        """
        """
        dflist = []
        with zipfile.ZipFile(self.zname) as z:
            inf = []
            for fn in z.namelist():
                # the files in the FF archive with the Ingredient records
                #  always start with this prefix...
                if fn[:17]=='FracFocusRegistry':
                    # need to extract number of file to correctly order them
                    num = int(re.search(r'\d+',fn).group())
                    inf.append((num,fn))
                    
            inf.sort()
            infiles = [x for _,x in inf]  # now we have a well-sorted list
            
            for fn in infiles[0:]:
                with z.open(fn) as f:
                    if verbose:
                        print(f' -- processing {fn}')
                    t = pd.read_csv(f,low_memory=False, dtype='str')
                                    # dtype={'APINumber':'str', 
                                    #        'CASNumber':'str', 
                                    #        'ClaimantCompany':'str', 
                                    #        'CountyName':'str', 
                                    #        'DisclosureId':'str', 
                                    #        'FFVersion':'str', 
                                    #        'FederalWell':'str', 
                                    #        'IndianWell':'str', 
                                    #        'IngredientComment':'str', 
                                    #        'IngredientCommonName':'str', 
                                    #        'IngredientMSDS':'str', 
                                    #        'IngredientName':'str', 
                                    #        'IngredientsId':'str', 
                                    #        'JobEndDate':'str', 
                                    #        'JobStartDate':'str', 
                                    #        'Latitude':'str', 
                                    #        'Longitude':'str', 
                                    #        'MassIngredient':'str', 
                                    #        'OperatorName':'str', 
                                    #        'PercentHFJob':'str', 
                                    #        'PercentHighAdditive':'str', 
                                    #        'Projection':'str', 
                                    #        'Purpose':'str', 
                                    #        'PurposeId':'str', 
                                    #        'StateName':'str', 
                                    #        'Supplier':'str', 
                                    #        'TVD':'str', 
                                    #        'TotalBaseNonWaterVolume':'str', 
                                    #        'TotalBaseWaterVolume':'str', 
                                    #        'TradeName':'str', 
                                    #        'WellName':'str', 
                                    #        'ingKeyPresent':'str', 
                                    #        'raw_filename':'str', 
                                    #        'reckey':'str'}
                                    # )
                    
                    # we need an indicator of the presence of IngredientKey
                    # whitout keeping the whole honking thing around
                    t['ingKeyPresent'] = t.IngredientsId.notna()
                    
                    t['raw_filename'] = fn # helpful for manual searches of raw files
                    dflist.append(t)
        final = pd.concat(dflist,sort=True)
        
        final.reset_index(drop=True,inplace=True) #  single integer as index
        final['reckey'] = final.index.astype(int)
        assert(len(final)==len(final.reckey.unique()))
        
        return final
        
    def compile_master_from_set(self, verbose=False):
        ldir = os.listdir(self.zipdir)
        for fn in ldir:
            if fn[:11] == 'ff_archive_':
                fdate = fn[11:-4]
                nfn = os.path.join(self.outset_dir,
                                   'ff_archive_meta_'+fn[11:-4]+'.parquet')
                # don't redo if it already exists
                if not os.path.exists(nfn):                
                    print(fdate)
                    self.zname = os.path.join(self.zipdir,fn)
                    try: # old version first
                        ndf = self.import_raw_FFV3()
                    except:
                        print('old import raw didnt work; trying new version')
                        ndf = self.import_raw_FFV4()
                    # nfn = os.path.join(self.outset_dir,
                    #                    'ff_archive_meta_'+fn[11:-4]+'.parquet')
                    ndf.to_parquet(nfn)
                else:
                    if verbose:
                        print(f'{fdate} already done...')
        print('Done')

if __name__ == '__main__':
    mdfa = Make_DF_From_Archive(zipdir=r"D:\archives\FF bulk data",
                                outset_dir=r"D:\archives\raw_df_archive")
    # mdfa.compile_master_from_set()
    mdfa.get_difference_set_FFV4(r"D:\archives\raw_df_archive\ff_archive_meta_2024-03-13.parquet",
                                 r"D:\archives\raw_df_archive\ff_archive_meta_2024-03-18.parquet")