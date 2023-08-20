import pymongo
import polars as pl
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
df=pl.DataFrame(eod_balance)
print(df.columns)
df=df.drop(columns='_id')
print(df.columns)


def core_tra(core_trans2):
    main=['trans_date','cust_id']
    remaining_columns = [col for col in core_trans2.columns if col not in main]
    remain=core_trans2.select(remaining_columns)
    core_transdata=core_trans2.select(main)
    core_transdata=core_transdata.with_columns(core_transactions=remain.to_dicts())
    return core_transdata
#group by date and customer id and call the function
result9=df.groupby(['trans_date','cust_id']).apply(core_tra)
print(result9.head())
"""
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
"""