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
eod_balance=db['eod_balance']
eod_balance=list(eod_balance.find())
#turn the collection to a dataframe
df=pd.DataFrame(eod_balance)
print(df.columns)
#select the main columns i need
main=['trans_date','cust_id']
#select the remaining columns in the collection apart from the one i need
remain=[col for col in df.columns if col not in main]
#function to turn the columns into a dictionary
def eod_balance(df):
    df2=df[main]
    df2['eod_balance']=df[remain].to_dict(orient='records')
    return df2
#group by date and customer id and call the function
result=df.groupby(['trans_date','cust_id']).apply(eod_balance)
#drops the index from the results
eod_balance=result.reset_index(drop=True)
print(eod_balance.head(5))