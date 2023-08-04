import pymongo
import pandas as pd
from pymongo import MongoClient
client=MongoClient('mongodb://localhost:27017')
print(client)
db=client["databank"]
print(db)
print(db.list_collection_names())
car_loans_accounts=db["car_loans_accounts"]

car_loan_account=list(car_loans_accounts.find())
df=pd.DataFrame(car_loan_account)
print(df.columns)
main=['loan_date','cust_id']


remain=[col for col in df.columns if col not in main]
def car_loan(df):
    df2=df[main]
    df2['car_loan']=df[remain].to_dict(orient='records')
    
    return df2

result=df.groupby(['loan_date','cust_id']).apply(car_loan)
results=result.reset_index(drop=True)
print(results.head(5))
print(results.columns)

#df1=pd.DataFrame(credit_card_account)
#print(df.head())
#print(df1.head())
#print(car_loan_account)