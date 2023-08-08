
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
core_transaction=db["core_transactions"]

#reads all data in the collection
core_transaction=list(core_transaction.find())
#turn the collection to a dataframe
df=pd.DataFrame(core_transaction)
print(df.columns)
#select the main columns i need
main=['trans_date','cust_id']
#select the remaining columns in the collection apart from the one i need
remain=[col for col in df.columns if col not in main]
#function to turn the columns into a dictionary
def core_transact(df):
    df2=df[main]
    df2['core_transaction']=df[remain].to_dict(orient='records')
    return df2
#group by date and customer id and call the function
result=df.groupby(['trans_date','cust_id']).apply(core_transact)
#drops the index from the results
core_trans=result.reset_index(drop=True)
print(core_trans.head(5))
