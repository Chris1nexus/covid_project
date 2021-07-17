#!/bin/bash
cd ./eurostat_lib/
bash ./run_eurostat_downloader.sh
cd ../policies_lib/
bash ./run_policy_downloader.sh
cd ../db_gen_lib/
bash ./run_db_generator.sh
cd ../