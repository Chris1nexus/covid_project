import os 
import pandas as pd
from tqdm import tqdm
import pandas as pd
import numpy as np
import sys
from policy_utils import CoronanetDownloader, load_dataframes, setup_dataframes, process_dataframes



coronanet_datasets_folder = sys.argv[1]
policy_csv_filepath = sys.argv[2]


coronanet = CoronanetDownloader(coronanet_datasets_folder)
coronanet_dataset_paths = coronanet.download()   
dataframes = load_dataframes(coronanet_dataset_paths)
national_dataframes = setup_dataframes(dataframes, coronanet_dataset_paths)
padded_national_dataframes = process_dataframes(national_dataframes)

complete = padded_national_dataframes[0][2]
for _, _, final in padded_national_dataframes[1:]:
  complete = pd.concat([complete, final])
complete.to_csv(policy_csv_filepath)