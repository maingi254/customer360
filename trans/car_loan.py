import pymongo
import pandas as pd
import databricks.koalas as ks
from pymongo import MongoClient
client=MongoClient('mongodb://localhost:27017')
print(client)
#connect to the database
db=client["databank"]
#read the collection in mongodb
print(db.list_collection_names())
#read the coollections and save them
car_loans_transaction=db["car_loan_transactions"]

#reads all data in the collection
car_loans_transaction=list(car_loans_transaction.find())
#turn the collection to a dataframe
df=pd.DataFrame(car_loans_transaction)

print(df.columns)
#select the main columns i need
main=['payment_date','cust_id']
#select the remaining columns in the collection apart from the one i need
remain=[col for col in df.columns if col not in main]
#function to turn the columns into a dictionary
def car_transact(df):
    df2=df[main]
    df2['car_loan_transaction']=df[remain].to_dict(orient='records')
    return df2
#group by date and customer id and call the function
result=df.groupby(['payment_date','cust_id']).apply(car_transact)
#drops the index from the results
car_trans=result.reset_index(drop=True)
print(car_trans.head(5))
