import os 
import pandas as pd
from tqdm import tqdm
import pandas as pd
import numpy as np

def load_dataframes(coronanet_dataset_paths):
    coronanet_dataframes = []
    for dataset_path in tqdm(coronanet_dataset_paths, leave=True, position=0, total=len(coronanet_dataset_paths)) :
      coronanet_df = pd.read_csv(dataset_path, encoding='latin-1').iloc[:,1:]
      coronanet_dataframes.append(coronanet_df)
    return coronanet_dataframes 

def setup_dataframes(coronanet_dataframes, coronanet_dataset_paths):
    cleaned_dataframes = []

    for df, dataset_path in tqdm(zip(coronanet_dataframes, coronanet_dataset_paths)):
        coronanet_release = df
        coronanet_release = coronanet_release[coronanet_release.province != 'Pennsylvania']


        coronanet_release = coronanet_release.sort_values(by=['date_start', 'date_announced', 'date_end'])[['date_announced', 'date_start', 'date_end', 'province', 'type', 'type_sub_cat']].drop_duplicates()
        coronanet_release['date_announced'] = pd.to_datetime(coronanet_release['date_announced'], format='%Y-%m-%d')
        coronanet_release['date_start'] = pd.to_datetime(coronanet_release['date_start'], format='%Y-%m-%d')
        coronanet_release['date_end'] = pd.to_datetime(coronanet_release['date_end'], format='%Y-%m-%d')
      

        coronanet_release = coronanet_release[coronanet_release['type'].isin(['Closure and Regulation of Schools', 'Curfew',
              'External Border Restrictions',
              'Internal Border Restrictions', 'Lockdown',
              'Quarantine', 'Restriction and Regulation of Businesses',
              'Restriction and Regulation of Government Services',
              'Restrictions of Mass Gatherings'])]

        filename = os.path.basename(dataset_path)
        idx = filename.rindex("_")
        nation = filename[(idx+1):].replace(".csv","")
        cleaned_dataframes.append((nation,coronanet_release) )
    return cleaned_dataframes
def process_dataframes(coronanet_national_dataframes):
    from collections import OrderedDict
    
    padded_national_dataframes = []
    post_national_dataframes = []
    for nation, clean_df in tqdm(coronanet_national_dataframes):

            col_data = OrderedDict()
            for column in clean_df.columns:
              col_data[column] = clean_df[column].values
        

        
            date_key = 'date'
            date_column = []

            formatted_data = OrderedDict()
            for column in col_data.keys():
              formatted_data[column] = []


            items = list(zip( *tuple(col_data.values()) ))
            N = len(items)
            for idx, item in enumerate(items):
                date_announced, date_start, date_end, province, type, type_sub_cat =  item 

                if pd.isnull(date_end):
                    date_end = pd.to_datetime('2020-12-31', format='%Y-%m-%d').to_numpy()  # date.today()
                    # date_end = (date_start +  pd.to_timedelta(1, unit='D')).to_numpy()
                
                date_start = date_start.astype('datetime64[D]')
                date_end = date_end.astype('datetime64[D]') 
                
                days_np_delta = (date_end - date_start)
                days = days_np_delta / np.timedelta64(1, 'D')
                #print(days)
                if days >= 0:    #(date_end - date_start).days >= 0:
                    for a in range(int(days) + 1):

                        date = date_start +  pd.to_timedelta(a, unit='D') #pd.DateOffset(days=a)
                      
                        data_tuple = (date_announced, date_start, date_end, province, type, type_sub_cat)
                        for i, column in enumerate(formatted_data.keys()):
                          formatted_data[column].append(data_tuple[i] )
                        date_column.append(date)
                        #clean = clean.append(row, ignore_index=True)
                
                prev_date_end = date_end
                """
                # lookahead from the current date_end to the next date. If the delta is > 1 day 
                # fill missing positions with the previous value
                for new_idx in range(idx+1, N):
                    item = items[new_idx]
                    _, new_date_start, date_end, curr_province, _, _ =  item 
                    if curr_province == province: # find next row related to the previously found province(rows are ordered by date, so for sure, this way will determine the next available rowfor this province)
                        new_date_start = new_date_start.astype('datetime64[D]')
                        if pd.isnull(date_end): 
                          date_end = (new_date_start +  pd.to_timedelta(1, unit='D')).to_numpy()
                        date_end = date_end.astype('datetime64[D]') 
                        

                        prev_date_to_curr_delta_days = (new_date_start - prev_date_end)
                        days = prev_date_to_curr_delta_days / np.timedelta64(1, 'D') # divide by the time unit to have pure units

                        if days > 1:  # date_start will be counted in the following cycle so we add rows for all days following the current prev_date_end until the day previous to the next row's date_start 
                          for a in range(1,int(days) ):
                
                              date = prev_date_end +  pd.to_timedelta(a, unit='D') 

                                                                                  # end of this extension is the day previous to the new policy start date
                              data_tuple = (date_announced, date_start, (new_date_start -  pd.to_timedelta(1, unit='D')).to_numpy().astype('datetime64[D]'), 
                                                    province, type, type_sub_cat)
                              for i, column in enumerate(formatted_data.keys()):
                                formatted_data[column].append(data_tuple[i] )
                              date_column.append(date)
                        # after having found the successor, the next iterations are not needed   
                        break
                """
            formatted_data[date_key] = date_column


            def len_0(x):
                if len(x)>0:
                    return 1
                else:
                    return 0

            final = pd.DataFrame(formatted_data)
            final['province'] = final['province'].fillna(nation)
            final = final[['date', 'province', 'type']]
            final = final.pivot_table(index=['date', 'province'], columns=['type'], aggfunc=[len_0], fill_value=0)
            if not final.empty:
              final = final['len_0'].sort_values(by=['date', 'province'])


              padded_national_dataframes.append(  (nation, pd.DataFrame(formatted_data), final) )


    return padded_national_dataframes


class CoronanetDownloader(object):
    def __init__(self, dst_folder , allvars=False):
      self.dst_folder = dst_folder

      self.raw_content_url = "https://raw.githubusercontent.com"
      self.github_allvars_dataset_link = "https://github.com/saudiwin/corona_tscs/tree/master/data/CoronaNet/data_country/coronanet_release_allvars" 
      self.github_dataset_link = "https://github.com/saudiwin/corona_tscs/tree/master/data/CoronaNet/data_country/coronanet_release"

      if allvars:
        self.github_link = self.github_allvars_dataset_link
      else:
        self.github_link = self.github_dataset_link

      self.table_class = "Details-content--hidden-not-important js-navigation-container js-active-navigation-container d-block"
    def download(self):
      import requests
      import os
      from bs4 import BeautifulSoup

      github_dataset_response = requests.get(self.github_link)

      
      self.dataset_link_list = []
      self.dataset_paths = []

      soup = BeautifulSoup(github_dataset_response.content, "html.parser")
      table_divs = soup.find_all("div", {"class": self.table_class})
      for item in table_divs:
        for link in item.find_all("a"):
          dataset_link = link.get('href')
          if dataset_link.endswith(".csv"):
            dataset_link = dataset_link.replace("blob/","")
            self.dataset_link_list.append(self.raw_content_url + dataset_link)
      
      root_path = self.dst_folder
      if not os.path.exists(root_path):
        os.makedirs(root_path)

      for link in self.dataset_link_list:
        response = requests.get(link)
        filename = os.path.basename(link)
        filepath = os.path.join(root_path, filename) 

        self.dataset_paths.append(filepath)
        with open(filepath , "w") as f:
          f.write(response.content.decode("latin-1") )

      return self.dataset_paths