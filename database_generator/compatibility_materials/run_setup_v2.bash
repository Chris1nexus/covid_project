#!/bin/bash
cd ./eurostat_lib/
python3 eurostat_downloader.py tsv_filemapper.tsv ../eurostat_datasets
cd ../policies_lib/
python3 policy_downloader.py ../coronanet_datasets ../policies.csv
cd ../db_gen_lib/
python3 db_gen.py --covid-data ../covid.csv  --euro-data ../eurostat_datasets --pol-data ../policies.csv  --db-path ../DB_finale_v3.xlsx
cd ../