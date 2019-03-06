#!/usr/bin/env python
# coding: utf-8

# In[4]:


import os
import urllib.request
import zipfile
import pandas as pd
import dask.dataframe as dd


# In[ ]:


## Donwload file and unzip in cwd
url = 'https://gist.github.com/nziehn/b2cff6d3a22a2dc906b6736356a5a1f5/archive/fd1dacca7bc662cb0fb482570cfcc82d33af3226.zip'
urllib.request.urlretrieve(url, os.path.join(os.getcwd(), 'homelike.zip'))
with zipfile.ZipFile("homelike.zip","r") as zip_ref:
    zip_ref.extractall(os.getcwd())


# In[5]:


## Import conversion data file to dataframe and export back to parquet to be
conversion_df = dd.read_csv('conversion_data.csv', parse_dates= ['timestamp'])
conversion_df.set_index('session_id')


# In[57]:


class getConversion():
    def __init__(self, conversion_df):
        self.conversion_df = conversion_df
    
    ## Get Values to calculate conversion rate
    def getConversion(self, page_num):
        if not page_num:
            conversion_df_mod = self.conversion_df.copy()
        else:
            ## Get sessions min timestamps
            session_min = self.conversion_df.groupby('session_id')['timestamp'].aggregate(['min']).reset_index()
            session_min = session_min.astype({'min': 'datetime64[ns, UTC]'})
            session_page = self.conversion_df[['page_id', 'session_id', 'timestamp']]

            ## Merge with pages dataframe
            session_page_merge = dd.merge(session_min, session_page, left_on=['session_id', 'min'], right_on=['session_id', 'timestamp'])
            
            ## Find sessions where pages are equal to page_num
            session_series = session_page_merge['session_id'].loc[session_page_merge['page_id'] == page_num]
            session_series_pandas = session_series.compute()
            session_series_list = session_series_pandas.to_list()
            
            ## Dataframe with only sessions that began on page_num
            conversion_df_mod = self.conversion_df[self.conversion_df['session_id'].isin(session_series_list)]   

        extract_values = conversion_df_mod.event_type.value_counts().compute()
        
        conversions = extract_values.get('conversion')
        unique_users = conversion_df_mod['session_id'].nunique()
        return (conversions/unique_users)

        
    def sortData(self):
        conversion_df_sorted = self.conversion_df.copy()
        conversion_df_sorted.reset_index()
        conversion_df_sorted['index_col'] = conversion_df_sorted.session_id.map(str) + "" + conversion_df_sorted.page_id.map(str)
        conversion_df_sorted = conversion_df_sorted.set_index('index_col')
        print(conversion_df_sorted.head(50))
        
    def mostEffective(self, param):
        ## Group data by param and filter out only conversions
        param_df = self.conversion_df[[param, 'event_type']].groupby([param, 'event_type'])['event_type'].agg(['count']).reset_index()
        param_conversions_df = param_df.loc[param_df['event_type'] == 'conversion']
        
        ## Find unique sessions within each campaign
        unique_sessions_df = self.conversion_df[[param, 'session_id']].groupby([param])['session_id'].nunique()
        
        ## Merge Dataframes into one
        param_conversion = param_conversions_df.merge(unique_sessions_df.to_frame(), left_on=param, right_on=param)

        ##Calculate Conversion rates for each campaign 
        param_conversion['conversion_rate'] = param_conversion['count']/param_conversion['session_id']
        param_conversion_filtered = param_conversion[param_conversion['session_id'] >= 50]

        ## Converts Dask DF to Pandas to facilitate easier write to CSV. Shouldn't be an issue for memory constraints
        return param_conversion_filtered.compute()


# In[59]:


def main():
    conversion_class = getConversion(conversion_df)
    ## Overall Conversion Rate
    print('Overall Conversion Rate', conversion_class.getConversion(None))
    
    ## Page Specific Conversion Rate
    print('Page Specific Conversion Rate', conversion_class.getConversion(4903628644844587131))
    
    ## Conversion Rates by Campaign
    campaign_conversions = conversion_class.mostEffective('campaign_id')
    campaign_conversions.sort_values(by=['conversion_rate'], ascending=False).to_csv('campaign_conversions.csv')

    ##Conversion Rates by Device
    device_conversions = conversion_class.mostEffective('device_type')
    device_conversions.sort_values(by=['conversion_rate'], ascending=False).to_csv('device_conversions.csv')
    
    ## Sort Data by session_id and page_id
    conversion_class.sortData()


# In[60]:


if __name__== "__main__":
    main()

