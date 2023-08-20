
import pymongo
import pandas as pd
import polars as pl
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
core_transaction=pd.DataFrame(core_transaction)
core_trans=core_transaction.reset_index(drop=True)
core_trans['_id']=core_trans['_id'].astype(str)
core_trans2=pl.from_pandas(core_trans)
print(core_trans2.head(5))
#select the main columns i need
def core_tra(core_trans2):
    main=['trans_date','cust_id']
    remaining_columns = [col for col in core_trans2.columns if col not in main]
    remain=core_trans2.select(remaining_columns)
    core_transdata=core_trans2.select(main)
    core_transdata=core_transdata.with_columns(core_transactions=remain.to_dicts())
    return core_transdata
#group by date and customer id and call the function
result9=core_trans2.groupby(['trans_date','cust_id']).apply(core_tra)


print(result9.head(5))
