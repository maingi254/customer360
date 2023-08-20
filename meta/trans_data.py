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
car_loans_account=db["car_loans_accounts"]
deposit_account=db['deposit_accounts']
home_loan_account=db['home_loan_accounts']
personal_loan_account=db['personal_loan_accounts']
saving_account=db['savings_accounts']
sme_business_loan_account=db['sme_business_loan_accounts']
sme_corps_current_account=db['sme_corps_current_accounts']
#eod_balance=db['eod_balance']


#READ THE DATA 
credit_card_accounts=list(credit_card_account.find())
car_loan_accounts=list(car_loans_account.find())
deposit_account=list(deposit_account.find())
home_loan_account=list(home_loan_account.find())
personal_loan_account=list(personal_loan_account.find())
saving_account=list(saving_account.find())
sme_business_loan=list(sme_business_loan_account.find())
sme_corps_current=list(sme_corps_current_account.find())
#eod_balance=list(eod_balance.find())
#turn the collection to a dataframe
credit_card_account=pd.DataFrame(credit_card_accounts)
car_loans_account=pd.DataFrame(car_loan_accounts) 
deposit_account=pd.DataFrame(deposit_account)
home_loan_account=pd.DataFrame(home_loan_account)
personal_loan_account=pd.DataFrame(personal_loan_account)
saving_account=pd.DataFrame(saving_account)
sme_business_loan=pd.DataFrame(sme_business_loan)
sme_corps_current=pd.DataFrame(sme_corps_current)
#eod_balance=pd.DataFrame(eod_balance)

#change datatype of date column and rename column to date_of_transaction

# change date type from string to date
credit_card_account['subscription_date']=pd.to_datetime(credit_card_account['subscription_date'],format='YYYY-MM-DD')
credit_card_account=credit_card_account.rename(columns={'subscription_date':"date_of_transaction"})

car_loans_account['loan_date']=pd.to_datetime(car_loans_account['loan_date'],format='YYYY-MM-DD')
car_loans_account=car_loans_account.rename(columns={'loan_date':"date_of_transaction"})

deposit_account['deposit_opening_date']=pd.to_datetime(deposit_account['deposit_opening_date'],format='YYYY-MM-DD')
deposit_account=deposit_account.rename(columns={'deposit_opening_date':"date_of_transaction"})

home_loan_account['loan_date']=pd.to_datetime(home_loan_account['loan_date'],format='YYYY-MM-DD')
home_loan_account=home_loan_account.rename(columns={'loan_date':"date_of_transaction"})

personal_loan_account['loan_date']=pd.to_datetime(personal_loan_account['loan_date'],format='YYYY-MM-DD')
personal_loan_account=personal_loan_account.rename(columns={'loan_date':"date_of_transaction"})

saving_account['opening_date']=pd.to_datetime(saving_account['opening_date'],format='YYYY-MM-DD')
saving_account=saving_account.rename(columns={'opening_date':"date_of_transaction"})

sme_business_loan['loan_date']=pd.to_datetime(sme_business_loan['loan_date'],format='YYYY-MM-DD')
sme_business_loan=sme_business_loan.rename(columns={'loan_date':"date_of_transaction"})

sme_corps_current['opening_date']=pd.to_datetime(sme_corps_current['opening_date'],format='YYYY-MM-DD')
sme_corps_current=sme_corps_current.rename(columns={'opening_date':"date_of_transaction"})

#eod_balance['trans_date']=pd.to_datetime(eod_balance['trans_date'],format='YYYY-MM-DD')
#eod_balance=eod_balance.rename(columns={'trans_date':"date_of_transaction"})

#function to turn the columns into a dictionary
def main(df):
    main=['date_of_transaction','cust_id']
    remain=[col for col in df.columns if col not in main]
    df2=df[main]
    df2['reccords']=df[remain].to_dict(orient='records')
    return df2
#group by date and customer id and call the function
result=credit_card_account.groupby(['date_of_transaction','cust_id']).apply(main)
result2=car_loans_account.groupby(['date_of_transaction','cust_id']).apply(main)
result3=deposit_account.groupby(['date_of_transaction','cust_id']).apply(main)
result4=home_loan_account.groupby(['date_of_transaction','cust_id']).apply(main)
result5=personal_loan_account.groupby(['date_of_transaction','cust_id']).apply(main)
result6=saving_account.groupby(['date_of_transaction','cust_id']).apply(main)
result7=sme_business_loan.groupby(['date_of_transaction','cust_id']).apply(main)
result8=sme_corps_current.groupby(['date_of_transaction','cust_id']).apply(main)
#result9=eod_balance.groupby(['date_of_transaction','cust_id']).apply(main)

print(result.head(9))
#RENAME THE RECORDS COLUMN TO RESPECTIVE COLUMNS
result.rename(columns={'reccords':'credit_card_account'},inplace=True)
result2.rename(columns={'reccords':'car_loans_account'},inplace=True)
result3.rename(columns={'reccords':'deposit_account'},inplace=True)
result4.rename(columns={'reccords':'home_loan_account'},inplace=True)
result5.rename(columns={'reccords':'personal_loan_account'},inplace=True)
result6.rename(columns={'reccords':'savings_account'},inplace=True)
result7.rename(columns={'reccords':'sme_business_loan'},inplace=True)
result8.rename(columns={'reccords':'sme_corps_current'},inplace=True)
#result9.rename(columns={'reccords':'eod_balance'},inplace=True)

#DROP THE INDEX
result=result.reset_index(drop=True)
result2=result2.reset_index(drop=True)
result3=result3.reset_index(drop=True)
result4=result4.reset_index(drop=True)
result5=result5.reset_index(drop=True)
result6=result6.reset_index(drop=True)
result7=result7.reset_index(drop=True)
result8=result8.reset_index(drop=True)
#result9=result8.reset_index(drop=True)
#MERGE THE DATA
merged=result.merge(result2,on=['date_of_transaction','cust_id'],how='outer')
merged2=merged.merge(result3,on=['date_of_transaction','cust_id'],how='outer')
merged3=merged2.merge(result4,on=['date_of_transaction','cust_id'],how='outer')
merged4=merged3.merge(result5,on=['date_of_transaction','cust_id'],how='outer')
merged5=merged4.merge(result6,on=['date_of_transaction','cust_id'],how='outer')
merged6=merged5.merge(result7,on=['date_of_transaction','cust_id'],how='outer')
merged7=merged6.merge(result8,on=['date_of_transaction','cust_id'],how='outer')
#merged8=merged7.merge(result9,on=['date_of_transaction','cust_id'],how='outer')


print(merged7.head(8))

print(merged7.dtypes)

#CREATE CONNECTION TO MONGODB AND PERSIST THE DATA
database=client["transformed"]
collection=database["metadata"]

documents=merged7.to_dict(orient='records')
try:
    mongo=collection.insert_many(documents)
   
except Exception as e:
   print("error duuring excetion", e)
client.close()
