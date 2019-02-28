import os
import urllib.request
import zipfile
import pandas as pd



## Donwload file and unzip in cwd
# url = 'https://gist.github.com/nziehn/b2cff6d3a22a2dc906b6736356a5a1f5/archive/fd1dacca7bc662cb0fb482570cfcc82d33af3226.zip'
# urllib.request.urlretrieve(url, os.path.join(os.getcwd(), 'homelike.zip'))
# with zipfile.ZipFile("homelike.zip","r") as zip_ref:
    # zip_ref.extractall(os.getcwd())



## Import conversion data file to dataframe

conversion_df = pd.read_csv('conversion_data.csv', parse_dates= ['timestamp'])


class getConversion():
    def __init__(self, conversion_df):
        self.conversion_df = conversion_df
    
    ## Get Values to calculate conversion rate
    def getConversion(self, page_num):
        conversion_df_mod = self.conversion_df.copy()
        if page_num:
            ## Get series of sessions that started on page_num
            conversion_df_mod['session_time_rank'] = conversion_df_mod.groupby(['session_id'])['timestamp'].rank(method='first',ascending=True)
            session_series = conversion_df_mod['session_id'].loc[(conversion_df_mod['session_time_rank'] == 1) & (conversion_df_mod['page_id'] == page_num)]
            conversion_df_mod = self.conversion_df.loc[self.conversion_df['session_id'].isin(session_series)]
            
        extract_values = conversion_df_mod['event_type'].value_counts()
        conversions = extract_values.get('conversion')
        unique_users = conversion_df_mod['session_id'].nunique()
        return (conversions/unique_users)
        
    def sortData(self):
        self.conversion_df.sort_values(by=['page_id', 'session_id'])
        
    def mostEffective(self, param):
        ## Group data by param and filter out only conversions
        param_df = self.conversion_df[[param, 'event_type']].groupby([param, 'event_type'])['event_type'].agg(['count']).reset_index()
        param_conversions_df = param_df.loc[param_df['event_type'] == 'conversion']
        
        ## Find unique sessions within each campaign 
        unique_sessions_df = self.conversion_df[[param, 'session_id']].groupby([param])['session_id'].nunique()
        
        ## Merge Dataframes into one
        param_conversion = param_conversions_df.merge(unique_sessions_df, left_on=param, right_on=param)

        ##Calculate Conversion rates for each campaign 
        param_conversion['conversion_rate'] = param_conversion['count']/param_conversion['session_id']
        param_conversion_filtered = param_conversion[param_conversion['session_id'] >= 50]

        return param_conversion_filtered
        

def main():
    conversion_class = getConversion(conversion_df)
    campaign_conversions = conversion_class.mostEffective('campaign_id')
    device_conversions = conversion_class.mostEffective('device_type')

    campaign_conversions.sort_values(by=['conversion_rate'], ascending=False).to_csv('campaign_conversions.csv')
    device_conversions.sort_values(by=['conversion_rate'], ascending=False).to_csv('device_conversions.csv')
    print(conversion_class.getConversion(None))
    print(conversion_class.getConversion(4903628644844587131))
    conversion_class.sortData()



if __name__== "__main__":
    main()

