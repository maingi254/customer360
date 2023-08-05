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
saving_account=db['savings_accounts']
saving_account=list(saving_account.find())
#turn the collection to a dataframe
df=pd.DataFrame(saving_account)
print(df.columns)
#select the main columns i need
main=['opening_date','cust_id']
#select the remaining columns in the collection apart from the one i need
remain=[col for col in df.columns if col not in main]
#function to turn the columns into a dictionary
def savings(df):
    df2=df[main]
    df2['savings']=df[remain].to_dict(orient='records')
    return df2
#group by date and customer id and call the function
result=df.groupby(['opening_date','cust_id']).apply(savings)
#drops the index from the results
saving_account=result.reset_index(drop=True)
print(saving_account.head(5))