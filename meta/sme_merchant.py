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
sme_merchants_account=db['sme_merchants_accounts']
sme_merchants_account=list(sme_merchants_account.find())
#turn the collection to a dataframe
df=pd.DataFrame(sme_merchants_account)
print(df.columns)
#select the main columns i need
main=['cust_id']
#select the remaining columns in the collection apart from the one i need
remain=[col for col in df.columns if col not in main]
#function to turn the columns into a dictionary
def sme_merchant(df):
    df2=df[main]
    df2['sme_corps_current']=df[remain].to_dict(orient='records')
    return df2
#group by date and customer id and call the function
result=df.groupby('cust_id').apply(sme_merchant)
#drops the index from the results
sme_merchant=result.reset_index(drop=True)
print(sme_merchant.head(5))