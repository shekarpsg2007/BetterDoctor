import json
import csv
import re
import pandas as pd
from pandas.io.json import json_normalize


#########Read CSV, rename_columns + strip empty spaces########
csv_dataFrame=pd.read_csv('/Users/cdivaka/Downloads/data_files_2/match_file.csv')
csv_dataFrame.rename(columns={'street':'street_csv', 'street_2':'street_2_csv', 'city': 'city_csv', 'state': 'state_csv', 'zip': 'zip_csv', 'npi': 'npi_csv'}, inplace=True)
csv_dataFrame['state_csv']=csv_dataFrame['state_csv'].str.upper().str.strip()
csv_dataFrame['city_csv']=csv_dataFrame['city_csv'].str.lower().str.strip()
csv_dataFrame['zip_csv']=csv_dataFrame['zip_csv'].str[:5]
csv_dataFrame['street_csv']=csv_dataFrame['street_csv'].str.lower().str.strip()
csv_dataFrame['street_2_csv']=csv_dataFrame['street_2_csv'].str.lower().str.strip()
csv_dataFrame['first_name']=csv_dataFrame['first_name'].str.lower().str.strip()
csv_dataFrame['last_name']=csv_dataFrame['last_name'].str.lower().str.strip()


####Load Json#####
def load_json_multiple(segments):
    chunk = ""
    for segment in segments:
        chunk += segment
        try:
            yield json.loads(chunk)
            chunk = ""
        except ValueError:
            pass

count_json = 0
all_normalized=pd.DataFrame()
with open('/Users/cdivaka/Downloads/data_files_2/source_data.json') as f:
    for parsed_json in load_json_multiple(f):
        ###loop thru json and count the number of documents###
        count_json +=1
        ###normalize json###
        normalized=json_normalize(parsed_json, 'practices', [['doctor', 'first_name'],['doctor', 'last_name'],['doctor', 'npi']],errors='ignore')
        all_normalized=all_normalized.append(normalized)
print('Count of number of read documents = ', count_json)
#####First pass of join - ON NPI - #######
#Rename clean the data to match CSV#
all_normalized['doctor.first_name']=all_normalized['doctor.first_name'].str.lower().str.strip()
all_normalized['doctor.last_name']=all_normalized['doctor.last_name'].str.lower().str.strip()
all_normalized['city']=all_normalized['city'].str.lower().str.strip()
all_normalized['state']=all_normalized['state'].str.strip()
all_normalized['street']=all_normalized['street'].str.lower().str.strip()
all_normalized['street_2']=all_normalized['street_2'].str.lower().str.strip()
#Join on NPI#
join_first_pass=pd.merge(pd.DataFrame(all_normalized), csv_dataFrame[['npi_csv']], how='left', left_on='doctor.npi', right_on='npi_csv')
#Filter for non matching records
join_npi=join_first_pass[join_first_pass.npi_csv.notnull()]
print('Count of number of documents joined on npi = ', join_npi.shape[0])
#####Second pass of join - ON name, street, city, state and zip - #####
#Remove matching NPI#
filter=join_first_pass[join_first_pass.npi_csv.isnull()]
#Join on Rest#
join_second_pass=pd.merge(pd.DataFrame(filter), csv_dataFrame[['first_name', 'last_name','street_csv', 'street_2_csv', 'state_csv', 'city_csv', 'zip_csv']], how='inner', left_on=['doctor.first_name', 'doctor.last_name', 'street', 'street_2', 'state', 'city', 'zip'], right_on=['first_name', 'last_name','street_csv','street_2_csv','state_csv', 'city_csv', 'zip_csv'])
print('Count of number of documents joined on everything else = ', join_second_pass.shape[0])
