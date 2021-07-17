#!/bin/bash
python3 db_gen_lib/db_gen.py --covid-data ../covid.csv  --euro-data ../eurostat_datasets --pol-data ../policies.csv  --db-path ../DB_finale_v3.xlsx

