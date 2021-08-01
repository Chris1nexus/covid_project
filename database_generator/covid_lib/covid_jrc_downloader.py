import urllib.request
import argparse


def main(args):

    
    

    covid_data_url = "https://raw.githubusercontent.com/ec-jrc/COVID-19/master/data-by-region/jrc-covid-19-all-days-by-regions.csv"

    with urllib.request.urlopen(covid_data_url) as f:
        string = f.read().decode('utf-8')

    
    with open(args.out_filepath, "w+",  encoding="utf-8") as outfile:
        outfile.write(string)
    
  



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
 


    args = parser.parse_args()
    main(args)