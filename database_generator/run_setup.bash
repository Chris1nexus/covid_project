#!/bin/bash
bash ./eurostat_lib/run_eurostat_downloader.bash
bash ./policies_lib/run_policies_downloader.bash
bash ./db_gen_lib/run_db_generator.bash