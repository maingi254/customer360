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
credit_card_account=db['credit_card_accounts']
credit_card_account=list(credit_card_account.find())
#turn the collection to a dataframe
df=pd.DataFrame(credit_card_account)
print(df.columns)
#select the main columns i need
main=['subscription_date','cust_id']
#select the remaining columns in the collection apart from the one i need
remain=[col for col in df.columns if col not in main]
#function to turn the columns into a dictionary
def credit_card(df):
    df2=df[main]
    df2['credit_card']=df[remain].to_dict(orient='records')
    return df2
#group by date and customer id and call the function
result=df.groupby(['subscription_date','cust_id']).apply(credit_card)
#drops the index from the results
credit_card=result.reset_index(drop=True)
print(credit_card.head(5))