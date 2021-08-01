def to_float(item):
    if len(item) > 0:
        item = float(item)
        return item
    return 0
def map0(row):
    row_data = row.split(",")
    date = row_data[0]
    nuts = row_data[-1]
    # avoid mapping the header
    if "CumulativePositive" not in row:
        return nuts, (date, CovidData.parse(row))
    return None
def reduce0(item1, item2):
    date1, covid1 = item1
    date2, covid2 = item2
    
    if date1 >= date2:
        return date1, CovidData.time_aggregate(covid1, covid2)
    else:
        return date2, CovidData.time_aggregate(covid1, covid2)

    
def map1(item):
    nuts, (date, covid_data) = item
    #row_data = row.split(",")
    #date = row_data[0]
    #nuts = row_data[-1]
    
    if len(nuts)> 4:
        nuts = nuts[:4]
    return nuts, covid_data
    
def reduce1(covid1, covid2):
    return CovidData.aggregate(covid1, covid2)
    


class CovidData(object):
    def __init__(self, cum_positive, cum_deceased, cum_recovered, curr_positive, hospitalized, intensive_care,
                 iso3, country_name, EUCountry, EUCPMCountry, date):
        self.cum_positive, self.cum_deceased, self.cum_recovered = cum_positive, cum_deceased, cum_recovered
        self.curr_positive, self.hospitalized, self.intensive_care = curr_positive, hospitalized, intensive_care
        self.iso3, self.country_name, self.EUCountry, self.EUCPMCountry = iso3, country_name, EUCountry, EUCPMCountry
        self.date = date
        
    def aggregate(covid1, covid2):
        
        # there should be assert statemetns to verify that time-independent data are equal for both items (e.g. iso3 of the first == iso3 of the second)
        
        if covid1.date >= covid2.date:
            date = covid1.date
        else:
            date = covid2.date
            
        iso3 = covid1.iso3
        country_name = covid1.country_name
        
        cum_positive = covid1.cum_positive + covid2.cum_positive
        cum_deceased = covid1.cum_deceased + covid2.cum_deceased
        cum_recovered = covid1.cum_recovered + covid2.cum_recovered
        curr_positive = covid1.curr_positive + covid2.curr_positive
        hospitalized = covid1.hospitalized + covid2.hospitalized
        intensive_care  = covid1.intensive_care + covid2.intensive_care

        EUCountry = covid1.EUCountry
        EUCPMCountry = covid1.EUCPMCountry
    
        covid_res = CovidData(cum_positive, cum_deceased, cum_recovered, curr_positive, hospitalized, intensive_care,
                 iso3, country_name, EUCountry, EUCPMCountry,
                 date)
        return covid_res
    
    
    def time_aggregate(covid1, covid2):
        
        iso3 = covid1.iso3
        country_name = covid1.country_name
        EUCountry = covid1.EUCountry
        EUCPMCountry = covid1.EUCPMCountry
    
        # for cumulated values, take the highest value of the two, for others, the most recent is instead ok
        cum_positive = max(covid1.cum_positive, covid2.cum_positive)
        cum_deceased = max(covid1.cum_deceased, covid2.cum_deceased)
        cum_recovered = max(covid1.cum_recovered, covid2.cum_recovered)
        if covid1.date >= covid2.date:
            date = covid1.date
            curr_positive = covid1.curr_positive 
            hospitalized = covid1.hospitalized 
            intensive_care  = covid1.intensive_care 
        else:
            date = covid2.date 
            curr_positive = covid2.curr_positive 
            hospitalized = covid2.hospitalized 
            intensive_care  = covid2.intensive_care 
            
            
        covid_res = CovidData(cum_positive, cum_deceased, cum_recovered, curr_positive, hospitalized, intensive_care,
                 iso3, country_name, EUCountry, EUCPMCountry,
                 date)
        return covid_res
    
    def parse(row):
        row_data = row.split(",")
        date = row_data[0]
        iso3 = row_data[1]
        country_name = row_data[2]
        
        region = row_data[3]
        
        
        cum_positive, cum_deceased, cum_recovered, curr_positive, hospitalized, intensive_care = [ to_float(item) for item in row_data[6:12] ]

        EUCountry = row_data[12]
        EUCPMCountry = row_data[13]
        
        covid = CovidData(cum_positive, cum_deceased, cum_recovered, curr_positive, hospitalized, intensive_care,
                 iso3, country_name, EUCountry, EUCPMCountry,
                 date)
        return covid
    
    def row_header():
        return ",".join(["nuts", "cum_positive", "cum_deceased", "cum_recovered",
                        "curr_positive", "hospitalized", "intensive_care",
                        "iso3", "country_name", "EUCountry", "EUCPMCountry", "date"])
    
    # date is not needed in the row representation (for the purpose of this work,
    # a coviddata item can be the aggregate of multiple dates, rendering this attribute useless, in a generic context)
    def as_row(self, nuts):
        return ",".join([nuts, str(self.cum_positive), str(self.cum_deceased), str(self.cum_recovered),
                        str(self.curr_positive), str(self.hospitalized), str(self.intensive_care),
                        self.iso3, self.country_name, self.EUCountry, self.EUCPMCountry, self.date])
        
        
        
        
        
class CovidInfo(object):
    def __init__(self, cum_positive, cum_deceased, cum_recovered, curr_positive, hospitalized, intensive_care,
                 iso3, country_name, EUCountry, EUCPMCountry):
        self.cum_positive, self.cum_deceased, self.cum_recovered = cum_positive, cum_deceased, cum_recovered
        self.curr_positive, self.hospitalized, self.intensive_care = curr_positive, hospitalized, intensive_care
        self.iso3, self.country_name, self.EUCountry, self.EUCPMCountry = iso3, country_name, EUCountry, EUCPMCountry
   
    

class DateContainer(object):
    def __init__(self):
        self.dates = []
       
        
    def add(self, date, row):
        row_data = row.split(",")
        date = row_data[0]
        iso3 = row_data[1]
        country_name = row_data[2]
        
        region = row_data[3]
        #lat = float(row_data[4])
        #long = float(row_data[5])
        
        
        cum_positive, cum_deceased, cum_recovered, curr_positive, hospitalized, intensive_care = [ to_float(item) for item in row_data[6:12] ]
        """
        if len(row_data[6]) > 0:
            cum_positive = float(row_data[6])
        else:
            cum_positive = 0
        if len(row_data[7]) > 0:
            cum_deceased = float(row_data[7])
        else:
            
        if len(row_data[6]) > 0:
            cum_recovered = float(row_data[8])
        else:
            cum_recovered = 0
        if len(row_data[6]) > 0:
            curr_positive = float(row_data[9])
        else:
            curr_positive =0
        if len(row_data[6]) > 0:
            hospitalized = float(row_data[10])
        else:
            hospitalized = 0
        if len(row_data[6]) > 0:
            intensive_care = float(row_data[11])
        else:
            intensive_care = 0
        """
        EUCountry = row_data[12]
        EUCPMCountry = row_data[13]
        
        NUTS = row_data[14]
        
        self.dates.append((date, CovidInfo(cum_positive, cum_deceased, cum_recovered, curr_positive, hospitalized, intensive_care,
                                           iso3, country_name, EUCountry, EUCPMCountry) )
                             )
        