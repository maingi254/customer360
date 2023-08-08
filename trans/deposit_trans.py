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
deposit_transactions=db["deposit_transactions"]

#reads all data in the collection
deposit_transactions=list(deposit_transactions.find())
#turn the collection to a dataframe
df=pd.DataFrame(deposit_transactions)
print(df.columns)
#select the main columns i need
main=['deposit_date','cust_id']
#select the remaining columns in the collection apart from the one i need
remain=[col for col in df.columns if col not in main]
#function to turn the columns into a dictionary
def deposit_transactions(df):
    df2=df[main]
    df2['deposit_transactions']=df[remain].to_dict(orient='records')
    return df2
#group by date and customer id and call the function
result=df.groupby(['deposit_date','cust_id']).apply(deposit_transactions)
#drops the index from the results
deposit_transactions=result.reset_index(drop=True)
print(deposit_transactions.head(5))
