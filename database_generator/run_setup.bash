#!/bin/bash
cd ./eurostat_lib/
bash ./run_eurostat_downloader.bash
cd ../policies_lib/
bash ./run_policies_downloader.bash
cd ../db_gen_lib/
bash ./run_db_generator.bash
cd ../