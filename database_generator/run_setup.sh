#!/bin/bash
cd ./eurostat_lib/
bash ./run_eurostat_downloader.sh
cd ../ml_dataset_lib/
bash ./generate_dataset.sh
cd ../policies_lib/
bash ./run_policy_downloader.sh
cd ../db_gen_lib/
bash ./run_db_generator.sh
cd ../