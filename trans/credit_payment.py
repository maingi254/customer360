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
credit_card_payments=db["credit_card_payments"]

#reads all data in the collection
credit_card_payments=list(credit_card_payments.find())
#turn the collection to a dataframe
df=pd.DataFrame(credit_card_payments)
print(df.columns)
#select the main columns i need
main=['date_of_payment','cust_id']
#select the remaining columns in the collection apart from the one i need
remain=[col for col in df.columns if col not in main]
#function to turn the columns into a dictionary
def credit_payment(df):
    df2=df[main]
    df2['credit_card_payment']=df[remain].to_dict(orient='records')
    return df2
#group by date and customer id and call the function
result=df.groupby(['date_of_payment','cust_id']).apply(credit_payment)
#drops the index from the results
credit_payment=result.reset_index(drop=True)
print(credit_payment.head(5))
