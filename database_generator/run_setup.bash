#!/bin/bash
cd ./eurostat_lib/
bash ./eurostat_lib/run_eurostat_downloader.bash
cd ../policies_lib/
bash ./policies_lib/run_policies_downloader.bash
cd ../db_gen_lib/
bash ./db_gen_lib/run_db_generator.bash
cd ../