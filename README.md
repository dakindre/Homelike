# Homelike

Submission for Drew Dakin

## Results

### Task A
The overall conversion rate of all users was found to be:
#### 76.2%

### Task B
The conversion for users that started their session on page 4903628644844587131 was calculated by ordering all sessions by asending timestamps. After that all sessions which have in the first position the mentinoned page are collected. This collection of sessions is then used to filter the main dataframe. All conversions that occur within those sessions are then counted in the calculation.
#### 89.67%

The following code was used for Tasks A and B
```python

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
```

### Task C
The dataset is sorted using the following code snippet
```python
self.conversion_df.sort_values(by=['page_id', 'session_id'])
```

### Task D
The two insights I looked for in the data are: 
1. Top 10 campaign conversion rates with over 50 unique sessions 
![alt text](/campaign_conversions.PNG)
2. Device type conversion rates sorted
![alt text](/device_conversions.PNG)

The code used to find these were
```python
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
```
