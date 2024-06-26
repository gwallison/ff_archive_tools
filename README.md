<center> <img src="header_logo.png" width="100"/></center>
<!-- this is a test of a comment 
To do:
--->

# ff_archive_tools
The files in this repository are use to create working collections of archived FracFocus data.  The [Open-FF project](https://github.com/gwallison/openFF) regularly downloads a full dataset from the [FracFocus download page](https://fracfocus.org/data-download) and compiles a fresh data set from that download.  However re-examining older downloads and comparing them with new ones can be instructive: finding out when disclosures are published, identifying changes to already published disclosures, and detecting when disclosures are removed.

To make these archived data sets useful, we create a few different forms:

- **bulk download**: the CSV file as downloaded from FracFocus, named with a date tag of the download date.

- **raw dataframe**: a single pandas dataframe of a processed bulk download. All fields are taken as is and kept as strings (i.e. numbers and dates are represented as strings).  This dataframe is saved in parquet format with a date tag in the name.  Because these dataframes are exclusively made of strings, they differ from the raw dataframes used to actually build Open-FF data sets in the `build_nb` process.  

- **difference dictionary**: a python dictionary is created for each new raw dataframe that summarizes the differences from the last download.  This file is saved as a python pickle with a date tag in the name. One use of these dictionaries is to document the daily changes in the FracFocus output, to find new disclosures and to detect as early as possible changes that require adaptation of Open-FF code.  A difference dictionary is also created during the data set build process (`build_nb`) to help document what has changed since the last big update.

- **Date of publication dataframe**: a single file that summarizes all when new disclosures are detected and the number of days between the end of the fracking job and that publication date.  This pandas dataframe is saved as a parquet file.

--- 

With every download, these different forms are updated.  Occasionally, we add features so we need to recreate all files in some of the different forms. Here's how to do that:

- Bulk download files cannot be recreated. The earliest we have is from August 2018. 
- Raw dataframe files are recreated using the `Data_reader.compile_master_from_set` routine. If the directories in the "if __name__==..." code are correct and you start with an enpty output directory, running Data_reader directly will recreate the entire set based on the bulk files.  Otherwise, it will create raw dataframes that don't yet exist from the list of bulk files.  With 350 files, processing times was about 5 hours.
- Difference dictionaries are recreated using the `openFF.build.core.fetch_archive_difference_set.py` and the `make_multiple_sets` function.  Because the FracFocus data format has changed on occasion, these difference dictionaries are only created between like-formatted files.  For example, In Dec. 2023, FracFocus started its version 4, and so difference files are generated starting with reference to the first version 4 download but not before. Because download files that have different column structures cannot be easily compared, we can use the `scan_for_column_incompatibility` function in `openff.build.core.fetch_archive_difference_set` to find where those discontinuities happen.
- To recreate the Data of publication dataframe, use the `ff_archive_tools.Meta_reader` function called `create_pub_delay_df.py`.