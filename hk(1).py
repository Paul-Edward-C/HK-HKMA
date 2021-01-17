#!/usr/bin/env python
# coding: utf-8

# In[19]:


import urllib
import ssl
import pandas as pd

context = ssl._create_unverified_context()

url = 'https://api.hkma.gov.hk/public/market-data-and-statistics/daily-monetary-statistics/daily-figures-monetary-base?offset=0'

#this works to produce string
#with urllib.request.urlopen(url, context=context) as req:
#    print (req.read())


# In[20]:


#and this dooes too
#with urllib.request.urlopen(url, context=context) as f:
#    print(f.read())


# In[21]:


#thos doesn't work, because it is a json object, where loads tries to load a string into json
#datasets = json.loads(f)

# but isn't it as json file?


# In[22]:


import urllib, json

url = "https://api.hkma.gov.hk/public/market-data-and-statistics/daily-monetary-statistics/daily-figures-monetary-base?offset=0"

response = urllib.request.urlopen(url, context=context)

data = json.loads(response.read())

print(data)


# In[23]:


dataset = json.dumps(data, indent=2)
print(dataset)


# In[24]:


#json dumps creates a string; the json data is still in the data variable
for item in data['result']['records']:
    print(item)


# In[25]:


for item in data['result']['records']:
    Date = item['end_of_date']
    cert_of_indebt = item['cert_of_indebt']
    gov_notes_coins = item['gov_notes_coins_circulation'] 
    aggr_balance_bf_disc_win = item['aggr_balance_bf_disc_win']
    aggr_balance_af_disc_win = item['aggr_balance_af_disc_win']
    outstanding_efbn = item['outstanding_efbn']
    ow_lb_bf_disc_win = item['ow_lb_bf_disc_win']
    mb_bf_disc_win_total = item['mb_bf_disc_win_total']
    print(Date, cert_of_indebt, gov_notes_coins, aggr_balance_bf_disc_win)    

# ??? how to just get first four lines???


# In[26]:


rec_grid = list()
# try to define your result by your use case
fields = ["date", "cert_of_indebt"]

# ????SINGLE V DOUBLE SPEECH MARKS???

for item in data['result']['records']:
    Date = item['end_of_date']
    cert_of_indebt = item['cert_of_indebt']
    gov_notes_coins = item['gov_notes_coins_circulation'] 
    aggr_balance_bf_disc_win = item['aggr_balance_bf_disc_win']
    aggr_balance_af_disc_win = item['aggr_balance_af_disc_win']
    outstanding_efbn = item['outstanding_efbn']
    ow_lb_bf_disc_win = item['ow_lb_bf_disc_win']
    mb_bf_disc_win_total = item['mb_bf_disc_win_total']
    itemDict = {"date":Date, "cert_of_indebt": cert_of_indebt}
    rec_grid.append(itemDict)
    
resultDF = pd.DataFrame(rec_grid, columns=fields)

resultDF.head()


# In[94]:


rec_grid = list()
# try to define your result by your use case
fields = ["date", "mb_bf_disc_win_total", "outstanding_efbn"]

# ????SINGLE V DOUBLE SPEECH MARKS???

for item in data['result']['records']:
    Date = item['end_of_date']
    cert_of_indebt = item['cert_of_indebt']
    gov_notes_coins = item['gov_notes_coins_circulation'] 
    aggr_balance_bf_disc_win = item['aggr_balance_bf_disc_win']
    aggr_balance_af_disc_win = item['aggr_balance_af_disc_win']
    ExchangeFundBillsandNotes = item['outstanding_efbn']
    ow_lb_bf_disc_win = item['ow_lb_bf_disc_win']
    MonetaryBase = item['mb_bf_disc_win_total']
    itemDict = {"date":Date, "mb_bf_disc_win_total":MonetaryBase, "outstanding_efbn":ExchangeFundBillsandNotes}
    rec_grid.append(itemDict)
    
resultDF = pd.DataFrame(rec_grid, columns=fields)

resultDF.rename(columns={'date': 'Date', 'mb_bf_disc_win_total': 'Monetary Base', 'outstanding_efbn': 'Exchange Fund Bills and Notes'}, inplace = True)

resultDF.head()


# In[27]:


pd_response = urllib.request.urlopen(url, context=context)


df = pd.read_json(pd_response)

df

#doesn't work for multiple level json:


# In[28]:


#so how to use muliple level json
#this doesn't work - get repsonse 'HTTPResponse' object is not subscriptable
#from pandas.io.json import json_normalize

#import urllib, json
#url = "https://api.hkma.gov.hk/public/market-data-and-statistics/daily-monetary-statistics/daily-figures-monetary-base?offset=0"
#response = urllib.request.urlopen(url, context=context)
#nycphil = json_normalize(response['end_of_date'])


# In[29]:


pd_response = urllib.request.urlopen(url, context = context)

responseString = pd_response.read().decode('utf-8')

responseJSON = json.loads(responseString)


# In[30]:


print(responseJSON)


# In[13]:


#same
#print(responseJSON["result"])


# In[120]:


#paul_version = pd.json_normalize(responseJSON, record_path =['result'])
#dumb!


# In[31]:


df = pd.json_normalize(responseJSON, record_path=["result","records"])

df


# In[32]:


df.drop(columns=['cert_of_indebt', 'gov_notes_coins_circulation', 'aggr_balance_af_disc_win', 'ow_lb_bf_disc_win', 'ow_lb_af_disc_win'],inplace=True)
df.rename(columns={'end_of_date': 'Date', 'mb_bf_disc_win_total': 'Monetary Base', 'outstanding_efbn': 'Exchange Fund Bills and Notes', 'aggr_balance_bf_disc_win': 'Aggregate Balance'}, inplace = True)
df


# In[33]:


#turn date data into dates

df['Date'] = pd.to_datetime(df['Date']) 
print (df.dtypes)


# In[34]:


#setting the index as the date, and resaampling on that to get last in the month 
df_mtd = df.set_index('Date').resample('M').last()
df_mtd.head()


# In[36]:


#monthly data

url_m = 'https://api.hkma.gov.hk/public/market-data-and-statistics/monthly-statistical-bulletin/financial/monetary-statistics'

pd_response_m = urllib.request.urlopen(url_m, context = context)

responseString_m = pd_response_m.read().decode('utf-8')

responseJSON_m = json.loads(responseString_m)   
    
    


# In[37]:


# this works produces dict read out
#print(responseJSON_m)


# In[48]:


df_m = pd.json_normalize(responseJSON_m, record_path=["result","records"])
df_m_narrow = df_m[['end_of_month', 'aggr_balance', 'ef_bills_notes', 'monetary_base_total']]

#????how to select columns and use new same df ie use inplace=????

df_m_narrow.head()


# In[53]:


print (df_m_narrow.dtypes)
print ("------")

#df_m_narrow['end_of_month'] = pd.to_datetime(df_m_narrow['end_of_month']) 
#print (df_m_narrow.dtypes)


# In[39]:


# ???? This doesn't work ???

#df_m_narrow.rename(columns={'end_of_month': 'Date', 'aggr_balance': 'Aggregate Balance', 'ef_bills_notes': 'Exchange Fund Bills and Notes', 'monetary_base_total': 'Monetary Base'}, inplace = True)
#df_m_narrow.head()

# but this does

df_m_narrow.columns = ['Date', 'Aggregate Balance', 'Exchange Fund Bills and Notes', 'Monetary Base']
df_m_narrow.head()


# In[54]:


#check data types

print (df_m_narrow.dtypes)


# In[55]:


df_m_narrow['Date'] = pd.to_datetime(df_m_narrow['Date']) 
print (df_m_narrow.dtypes)


# In[201]:


df_m_narrow_dates = df_m_narrow.set_index('Date')
df_m_narrow_dates.head()


# In[193]:


combos = [df_mtd, df_m_narrow_dates] #listing the data sets
combined = pd.concat(combos) #combining the datasets
combined


# In[ ]:




