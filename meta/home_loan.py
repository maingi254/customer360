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
home_loan_account=db['home_loan_accounts']
home_loan_account=list(home_loan_account.find())
#turn the collection to a dataframe
df=pd.DataFrame(home_loan_account)
print(df.columns)
#select the main columns i need
main=['loan_date','cust_id']
#select the remaining columns in the collection apart from the one i need
remain=[col for col in df.columns if col not in main]
#function to turn the columns into a dictionary
def home_loan(df):
    df2=df[main]
    df2['home_loan']=df[remain].to_dict(orient='records')
    return df2
#group by date and customer id and call the function
result=df.groupby(['loan_date','cust_id']).apply(home_loan)
#drops the index from the results
home_loan=result.reset_index(drop=True)
print(home_loan.head(5))