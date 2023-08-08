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
home_transactions=db["home_loan_transactions"]

#reads all data in the collection
home_transactions=list(home_transactions.find())
#turn the collection to a dataframe
df=pd.DataFrame(home_transactions)
print(df.columns)
#select the main columns i need
main=['payment_date','cust_id']
#select the remaining columns in the collection apart from the one i need
remain=[col for col in df.columns if col not in main]
#function to turn the columns into a dictionary
def home_transactions(df):
    df2=df[main]
    df2['home_loan_transactions']=df[remain].to_dict(orient='records')
    return df2
#group by date and customer id and call the function
result=df.groupby(['payment_date','cust_id']).apply(home_transactions)
#drops the index from the results
home_transactions=result.reset_index(drop=True)
print(home_transactions.head(5))
