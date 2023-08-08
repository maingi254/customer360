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
debit_card_transactions=db["debit_card_transactions"]

#reads all data in the collection
debit_card_transactions=list(debit_card_transactions.find())
#turn the collection to a dataframe
df=pd.DataFrame(debit_card_transactions)
print(df.columns)
#select the main columns i need
main=['transaction_date','cust_id']
#select the remaining columns in the collection apart from the one i need
remain=[col for col in df.columns if col not in main]
#function to turn the columns into a dictionary
def debit_card(df):
    df2=df[main]
    df2['debit_card_transactions']=df[remain].to_dict(orient='records')
    return df2
#group by date and customer id and call the function
result=df.groupby(['transaction_date','cust_id']).apply(debit_card)
#drops the index from the results
debit_card=result.reset_index(drop=True)
print(debit_card.head(5))
