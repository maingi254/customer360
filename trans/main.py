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
credit_card_transaction=db["credit_card_transactions"]
credit_card_payments=db["credit_card_payments"]
debit_card_transactions=db["debit_card_transactions"]
deposit_transactions=db["deposit_transactions"]
home_transactions=db["home_loan_transactions"]
personal_loan_transactions=db["personal_loan_transactions"]
sme_loan_transactions=db["sme_loan_transactions"]
car_loans_transactions=db["car_loan_transactions"]


#READ THE DATA 
credit_card_transaction=list(credit_card_transaction.find())
credit_card_payments=list(credit_card_payments.find())
debit_card_transactions=list(debit_card_transactions.find())
deposit_transactions=list(deposit_transactions.find())
home_transactions=list(home_transactions.find())
personal_loan_transactions=list(personal_loan_transactions.find())
sme_loan_transactions=list(sme_loan_transactions.find())
car_loans_transaction=list(car_loans_transactions.find())
#turn the collection to a dataframe
credit2=pd.DataFrame(credit_card_transaction)
credit_card2=pd.DataFrame(credit_card_payments)
debit_card2=pd.DataFrame(debit_card_transactions)
deposit_trans2=pd.DataFrame(deposit_transactions)
home_trans=pd.DataFrame(home_transactions)
personal_loan=pd.DataFrame(personal_loan_transactions)
sme_loan=pd.DataFrame(sme_loan_transactions)
car_loans=pd.DataFrame(car_loans_transaction)

"""
#reset the index
credit2=credit.reset_index(drop=True)
credit_card2=credit_card.reset_index(drop=True)
debit_card2=debit_card.reset_index(drop=True)
deposit_trans2=deposit_trans.reset_index(drop=True)
home_trans=home_transaction.reset_index(drop=True)
personal_loan=personal_loan_transaction.reset_index(drop=True)
sme_loan=sme_loan_transaction.reset_index(drop=True)
car_loans=car_loans_transaction.reset_index(drop=True)
"""
#change datatype of date column and rename column to date_of_transaction#change datatype of date column and rename column to date_of_transaction
credit2['transaction_date']=pd.to_datetime(credit2['transaction_date'])
credit2['transaction_date']=pd.to_datetime(credit2['transaction_date'],format='YYYY-MM-DD')
credit2=credit2.rename(columns={'transaction_date':"date_of_transaction"})

credit_card2['date_of_payment']=pd.to_datetime(credit_card2['date_of_payment'],format='YYYY-MM-DD')
credit_card2=credit_card2.rename(columns={'date_of_payment':"date_of_transaction"}) 

debit_card2['transaction_date']=pd.to_datetime(debit_card2['transaction_date'])
debit_card2['transaction_date']=pd.to_datetime(debit_card2['transaction_date'],format='YYYY-MM-DD')
debit_card2=debit_card2.rename(columns={'transaction_date':"date_of_transaction"})

deposit_trans2['deposit_date']=pd.to_datetime(deposit_trans2['deposit_date'],format='YYYY-MM-DD')
deposit_trans2=deposit_trans2.rename(columns={'deposit_date':"date_of_transaction"})

home_trans['payment_date']=pd.to_datetime(home_trans['payment_date'],format='YYYY-MM-DD')
home_trans=home_trans.rename(columns={'payment_date':"date_of_transaction"})

personal_loan['payment_date']=pd.to_datetime(personal_loan['payment_date'],format='YYYY-MM-DD')
personal_loan=personal_loan.rename(columns={'payment_date':"date_of_transaction"})

sme_loan['payment_date']=pd.to_datetime(sme_loan['payment_date'],format='YYYY-MM-DD')
sme_loan=sme_loan.rename(columns={'payment_date':"date_of_transaction"})

car_loans['payment_date']=pd.to_datetime(car_loans['payment_date'],format='YYYY-MM-DD')
car_loans=car_loans.rename(columns={'payment_date':"date_of_transaction"})
#function to turn the columns into a dictionary
def main(df):
    main=['date_of_transaction','cust_id']
    remain=[col for col in df.columns if col not in main]
    df2=df[main]
    df2['reccords']=df[remain].to_dict(orient='records')
    return df2
#group by date and customer id and call the function
result = credit2.groupby(['date_of_transaction', 'cust_id']).apply(main)
result2=credit_card2.groupby(['date_of_transaction','cust_id']).apply(main)
result3=debit_card2.groupby(['date_of_transaction','cust_id']).apply(main)
result4=deposit_trans2.groupby(['date_of_transaction','cust_id']).apply(main)
result5=home_trans.groupby(['date_of_transaction','cust_id']).apply(main)
result6=personal_loan.groupby(['date_of_transaction','cust_id']).apply(main)
result7=sme_loan.groupby(['date_of_transaction','cust_id']).apply(main)
result8=car_loans.groupby(['date_of_transaction','cust_id']).apply(main)

print(result.head(9))
#RENAME THE RECORDS COLUMN TO RESPECTIVE COLUMNS
result.rename(columns={'reccords':'credit_card_transaction'},inplace=True)
result2.rename(columns={'reccords':'credit_card_payments'},inplace=True)
result3.rename(columns={'reccords':'debit_card_transactions'},inplace=True)
result4.rename(columns={'reccords':'deposit_transactions'},inplace=True)
result5.rename(columns={'reccords':'home_transactions'},inplace=True)
result6.rename(columns={'reccords':'personal_loan_transactions'},inplace=True)
result7.rename(columns={'reccords':'sme_loan_transactions'},inplace=True)
result8.rename(columns={'reccords':'car_loans_transaction'},inplace=True)

#DROP THE INDEX
result=result.reset_index(drop=True)
result2=result2.reset_index(drop=True)
result3=result3.reset_index(drop=True)
result4=result4.reset_index(drop=True)
result5=result5.reset_index(drop=True)
result6=result6.reset_index(drop=True)
result7=result7.reset_index(drop=True)
result8=result8.reset_index(drop=True)
#MERGE THE DATA
merged=result.merge(result2,on=['date_of_transaction','cust_id'],how='outer')
merged2=merged.merge(result3,on=['date_of_transaction','cust_id'],how='outer')
merged3=merged2.merge(result4,on=['date_of_transaction','cust_id'],how='outer')
merged4=merged3.merge(result5,on=['date_of_transaction','cust_id'],how='outer')
merged5=merged4.merge(result6,on=['date_of_transaction','cust_id'],how='outer')
merged6=merged5.merge(result7,on=['date_of_transaction','cust_id'],how='outer')
merged7=merged6.merge(result8,on=['date_of_transaction','cust_id'],how='outer')



print(merged7.head(8))

print(merged7.dtypes)

#CREATE CONNECTION TO MONGODB AND PERSIST THE DATA
database=client["transformed"]
collection=database["trans_data"]

documents=merged7.to_dict(orient='records')
try:
    mongo=collection.insert_many(documents)
   
except Exception as e:
   print("error duuring excetion", e)
client.close()
