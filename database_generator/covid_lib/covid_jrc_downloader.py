import urllib.request
import argparse
import pandas as pd
from io import StringIO

def main(args):

    
    

    covid_data_url = "https://raw.githubusercontent.com/ec-jrc/COVID-19/master/data-by-region/jrc-covid-19-all-days-by-regions.csv"

    with urllib.request.urlopen(covid_data_url) as f:
        string = f.read().decode('utf-8')
    
    str_stream = StringIO(string)
    df = pd.read_csv(str_stream)
    
    # obtain comparable date format (from 12/31/2020 -> 2020-12-31)
    query_date = pd.to_datetime(args.until_date)
    parsed_dates = pd.to_datetime(df["Date"], infer_datetime_format=True)
    query_result = parsed_dates <=  query_date
    df = df[query_result]
    df.to_csv(args.out_filepath,index=False)
    #with open(args.out_filepath, "w+",  encoding="utf-8") as outfile:
    #    outfile.write(string)
    
  



if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="download jrc covid cases"
    )


    parser.add_argument(
        "--out-filepath",
        type=str,
        default="../covid_cases_jrc.csv",
        help="path in which the output dataset is saved",
    )
    parser.add_argument(
        "--until-date",
        type=str,
        default="12/31/2020",
        help="remove all dates greater than the given one (format is month/day/year\n\t default=12/31/2020 )",
    )
 


    args = parser.parse_args()
    main(args)