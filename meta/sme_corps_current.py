import pymongo
import pandas as pd
from pymongo import MongoClient
client=MongoClient('mongodb://localhost:27017')
print(client)
#connect to the database
db=client["databank"]
#read the collection in mongodb
print(db.list_collection_names())
#read the coollections and save them
sme_corps_current_account=db['sme_corps_current_accounts']
sme_corps_current=list(sme_corps_current_account.find())
#turn the collection to a dataframe
df=pd.DataFrame(sme_corps_current)
print(df.columns)
#select the main columns i need
main=['opening_date','cust_id']
#select the remaining columns in the collection apart from the one i need
remain=[col for col in df.columns if col not in main]
#function to turn the columns into a dictionary
def sme_current(df):
    df2=df[main]
    df2['sme_corps_current']=df[remain].to_dict(orient='records')
    return df2
#group by date and customer id and call the function
result=df.groupby(['opening_date','cust_id']).apply(sme_current)
#drops the index from the results
sme_current=result.reset_index(drop=True)
print(sme_current.head(5))