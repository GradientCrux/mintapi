from operator import index
import pandas as pd
from pandas.io.json._normalize import nested_to_record

import json

from datetime import datetime
import pytz
import socket

from sqlalchemy import create_engine
import psycopg2

pd.set_option('display.max_columns', None)

import mintapi

import cred

# Get the IP of the system this script is coming from
host_name = socket.gethostname()
host_ip = socket.gethostbyname(host_name)

mintUser = cred.user
mintUserPassword = cred.mintPassword



# Get start time for the script
Houston_TZ = pytz.timezone('America/Chicago')
StartDateTime = datetime.now(Houston_TZ)




# Instantiate a class for Mint
mint = mintapi.Mint(
    email=mintUser,  # Email used to log in to Mint
    password=mintUserPassword,  # Your password used to log in to mint
    # Optional parameters
    mfa_method='sms',  # Can be 'sms' (default), 'email', or 'soft-token'.
                       # if mintapi detects an MFA request, it will trigger the requested method
                       # and prompt on the command line.
    headless=False,  # Whether the chromedriver should work without opening a
                     # visible window (useful for server-side deployments)
    mfa_input_callback=None,  # A callback accepting a single argument (the prompt)
                              # which returns the user-inputted 2FA code. By default
                              # the default Python `input` function is used.
    # intuit_account=None, # account name when multiple accounts are registered with this email.
                         # None will use the default account.
    session_path=None, # Directory that the Chrome persistent session will be written/read from.
                       # To avoid the 2FA code being asked for multiple times, you can either set
                       # this parameter or log in by hand in Chrome under the same user this runs
                       # as.
    # imap_account=None, # account name used to log in to your IMAP server
    # imap_password=None, # account password used to log in to your IMAP server
    # imap_server=None,  # IMAP server host name
    # imap_folder='INBOX',  # IMAP folder that receives MFA email
    # wait_for_sync=False,  # do not wait for accounts to sync
    # wait_for_sync_timeout=300,  # number of seconds to wait for sync
	# use_chromedriver_on_path=False,  # True will use a system provided chromedriver binary that
	                                 # is on the PATH (instead of downloading the latest version)
)

print("instantiated mint")




def findUnixDateKeys(sample_dict):

    unix_dates = []
    for key, value in sample_dict.items():
        # print(f'{key} is {value} and type {type(value)}')
        if isinstance(value, int) and len(str(value))==13:
            # print(f'{key} is an integer and length is {len(str(value))}')
            unix_dates.append(key)
        # print(value, '->', type(value), len(value))

    return unix_dates




def add_logging_fields_to_df(temp_df):
    # Add time markers to the columns
    temp_df['Execution_Time'] = StartDateTime
    temp_df['Execution_host_name'] = host_name
    temp_df['Execution_host_ip'] = host_ip
    temp_df['Source'] = 'Mint.com'
    temp_df['user'] = mintUser
    
    return temp_df


def convert_response_to_clean_df(r):
    
    temp_df = pd.DataFrame.from_dict(r)

    datefields = findUnixDateKeys(r[0])
    for field in datefields:
        temp_df[field] = pd.to_datetime(temp_df[field], unit='ms')

    temp_df = add_logging_fields_to_df(temp_df)

    return temp_df






def convert_budgets_response_to_df(budgets_r):

    # print(b['spend'])
    budgets_spend_df = pd.DataFrame.from_dict(budgets_r['spend'])

    budgets_income_df = pd.DataFrame.from_dict(budgets_r['income'])

    budgets_df = budgets_income_df.append(budgets_spend_df)

    budgets_df = add_logging_fields_to_df(budgets_df)

    return budgets_df


def convert_networth_to_df(value):
    networth_dict = {"NetWorth": value}
    networth_df = pd.DataFrame(networth_dict, index=[0])

    networth_df = add_logging_fields_to_df(networth_df)

    return networth_df



def convert_creditscore_to_df(value):

    creditscore_dict = {"CreditScore": value}
    creditscore_df = pd.DataFrame(creditscore_dict, index=[0])

    creditscore_df = add_logging_fields_to_df(creditscore_df)

    return creditscore_df


def flatten_bill_details(df):
    
       
    # Create columns to hold data
    df['billDetails_Type'] = None
    
    # Credit Card Info
    df['billDetails_creditCardNameAndEndingNumber'] = None
    df['billDetails_creditCardAvailableBalanceAmount'] = None
    df['billDetails_creditCardAvailableBalanceType'] = None
    df['billDetails_creditCardCreditLimit'] = None
    df['billDetails_creditCardPurchasesApr'] = None
    df['billDetails_creditCardStatus'] = None

    
    # Utility Payments
    df['billDetails_utilityName']  = None
    df['billDetails_utilityValue'] = None

    # Loan Details
    df['billDetails_loanNumber'] = None
    df['billDetails_loanCurrentBalance'] = None
    df['billDetails_loanPayOffAmount'] = None
    df['billDetails_loanEndDate'] = None
    df['billDetails_loanRemaningPayments'] = None
    df['billDetails_loanStatus'] = None

    
    NF = "Not Found"
    
    
    for index , _ in df.iterrows():
#         print(row.billDetailsList)
        try:
                        df.loc[index, 'billDetails_Type'] = df.loc[index, 'billDetailsList'][0]['billDetailsType']
        except:
            df.loc[index, 'billDetails_Type'] = "Bill Type Not Found"
        
        if  df.loc[index, 'billDetails_Type'] == 'CREDIT_CARD':
            try:
                
                df.loc[index, 'billDetails_creditCardNameAndEndingNumber'] =  df.loc[index, 'billDetailsList'][0]['name'] + " - " + df.loc[index, 'billDetailsList'][0]['number']
            except:
                df.loc[index, 'billDetails_creditCardNameAndEndingNumber'] = NF
            
            try:
                df.loc[index, 'billDetails_creditCardAvailableBalanceAmount'] =  df.loc[index, 'billDetailsList'][0]['availableBalanceAmount']
            except:
                df.loc[index, 'billDetails_creditCardAvailableBalanceAmount'] = NF
            try:
                df.loc[index, 'billDetails_creditCardAvailableBalanceType'] =  df.loc[index, 'billDetailsList'][0]['availableBalanceType']
            except:
                df.loc[index, 'billDetails_creditCardAvailableBalanceType'] = NF
            try:
                df.loc[index, 'billDetails_creditCardCreditLimit'] =  df.loc[index, 'billDetailsList'][0]['creditLimit']
            except:
                df.loc[index, 'billDetails_creditCardCreditLimit'] = NF
            try:
                df.loc[index, 'billDetails_creditCardPurchasesApr'] =  df.loc[index, 'billDetailsList'][0]['purchasesApr']
            except:
                df.loc[index, 'billDetails_creditCardPurchasesApr'] = NF
            try:
                df.loc[index, 'billDetails_creditCardStatus'] =  df.loc[index, 'billDetailsList'][0]['status']
            except:
                df.loc[index, 'billDetails_creditCardStatus'] = NF
        
        
        
        # Handle UTILITY Fields
                        
        elif  df.loc[index, 'billDetails_Type'] == 'UTILITY':
            
            try:
                
                df.loc[index,'billDetails_utilityName']  = df.loc[index, 'billDetailsList'][0]['name']
            
            except:
                df.loc[index, 'billDetails_utilityName'] = NF
            
            try:
                
                df.loc[index, 'billDetails_utilityValue'] = df.loc[index, 'billDetailsList'][0]['value']
            
            except:
                df[index, 'billDetails_utilityValue'] = NF

                
        # Handle LOAN Fields
                        
        elif df.loc[index, 'billDetails_Type'] == 'LOAN':
            
            try:
                df.loc[index, 'billDetails_loanNumber'] = df.loc[index, 'billDetailsList'][0]['name']
            
            except:
                df.loc[index, 'billDetails_loanNumber'] = NF
                
            try:   
                df.loc[index, 'billDetails_loanCurrentBalance'] = df.loc[index, 'billDetailsList'][0]['currentBalance']
            
            except:
                df.loc[index, 'billDetails_loanCurrentBalance'] = NF
                
            try:
                df.loc[index, 'billDetails_loanPayOffAmount'] = df.loc[index, 'billDetailsList'][0]['payoffAmount']
            
            except: 
                
                df.loc[index, 'billDetails_loanPayOffAmount'] = NF
                
            try:
                df.loc[index, 'billDetails_loanEndDate'] =  df.loc[index, 'billDetailsList'][0]['loanEndDate']
            
            except:
                df.loc[index, 'billDetails_loanEndDate'] = NF
            
            try:
                df.loc[index, 'billDetails_loanRemaningPayments'] = df.loc[index, 'billDetailsList'][0]['remainingPayments']
            
            except:
                df.loc[index, 'billDetails_loanRemaningPayments'] = NF

            try:
                df.loc[index, 'billDetails_loanStatus'] =  df.loc[index, 'billDetailsList'][0]['loanStatus']
            except:
                df.loc[index, 'billDetails_loanStatus'] = NF

    df = df.drop(['billDetailsList', 'allowedPaymentMethodOptions','contentAccountRef_connectedAccounts', 'providerRef_contentProviderRef','metaData_link'], axis=1)


    return df



def convert_bills_response_to_df(r_bills):
    
    flat_bills = nested_to_record(r_bills, sep='_')
    flat_bills_df = pd.DataFrame(flat_bills)
    flat_bills_df['allowedPaymentMethods'] = flat_bills_df.allowedPaymentMethodOptions.apply(lambda x: "-".join(x))
    flat_bills_df = flatten_bill_details(flat_bills_df)
    flat_bills_df = add_logging_fields_to_df(flat_bills_df)

    return flat_bills_df


def get_investmentAccount_Nums(info_dict):
    account_numbers = set()

    for key in info_dict.keys():
        if key.isnumeric():
            account_numbers.add(key)
    
    print(account_numbers)
    
    return account_numbers



def convert_investment_response_to_dfs(r_investments):
    info = json.loads(r_investments)

    account_nums =  get_investmentAccount_Nums(info)

    # Filter accounts_dict down to just the investment accounts you want (numeric)
    account_details = {key:value for key, value in info.items() if key in account_nums}
    account_details_df = pd.DataFrame(account_details)
    account_details_df = account_details_df.transpose()
    account_holding_details_df = account_details_df.copy()
    account_details_df = account_details_df.drop(['sorted','holdings'], axis=1) 
    
    account_details_df = add_logging_fields_to_df(account_details_df) # this is the first finished df



    account_holding_details_df = account_holding_details_df.drop('sorted',axis=1)
    holding_df = account_holding_details_df
    holding_df['holdings'].fillna('{}', inplace=True)
    
    
    # Expand out the holding records 

    holdings_expanded_df = pd.DataFrame()

    for i in holding_df.index:
        print(f'i is {i}')
        if holding_df.loc[i]['holdings'] == '{}':
            print('%s is = {} so skipping', i)
            continue
        else:
            helper_df = pd.DataFrame(holding_df.loc[i]['holdings'])
    #         print(helper_df.iloc[0])
            helper_df = helper_df.transpose()
            holdings_expanded_df = holdings_expanded_df.append(helper_df)


    # Add a primary key for the holdings table (account it comes from and the id of the specific holding)
    holdings_expanded_df['holdings_id'] = holdings_expanded_df['account'] + "-" + holdings_expanded_df['id']
    holdings_expanded_df.set_index('holdings_id',inplace=True)

    transactions_df = holdings_expanded_df.copy()

    holdings_expanded_df.drop('transactions',axis=1,inplace=True) 
    holdings_expanded_df = add_logging_fields_to_df(holdings_expanded_df) # This is the finished table


    # Expand out the transactions
    investment_tranasction_df = pd.DataFrame()

    for i in range(transactions_df.shape[0]):
        
        # Retain some of the holding header values 
        symbol = transactions_df.iloc[i]['symbol']
        h_id = transactions_df.iloc[i]['id']
        account  = transactions_df.iloc[i]['account']
        description = transactions_df.iloc[i]['description']
        
        temp = transactions_df.iloc[i]['transactions']
        if temp == []:
            continue
        else:
    #         print(temp)
            for b in range(len(temp)):
    #             print('b is', b)
                b = temp[b]
                b_df = pd.DataFrame(b, index=[0])
                b_df['symbol'] = symbol
                b_df['holding_number'] = h_id
                b_df['account'] = account
                b_df['description'] = description
                
                investment_tranasction_df = investment_tranasction_df.append(b_df)
                
    
    investment_tranasction_df = add_logging_fields_to_df(investment_tranasction_df) # This is the final table


    return account_details_df, holdings_expanded_df, investment_tranasction_df



# Write mint data to postgress
def write_to_postgres(clean_df, table_name):
# Connect to PostGres
    engine           = create_engine('postgresql+psycopg2://postgres:mysecretpassword@192.168.4.143/mint', pool_recycle=3600);
    postgreSQLConnection    = engine.connect()


    try:

        clean_df.to_sql(table_name, postgreSQLConnection, if_exists='append')

    except ValueError as vx:

        print(vx)

    except Exception as ex:  

        print(ex)

    else:

        print("PostgreSQL Table %s has been created successfully."%table_name)

    finally:

        postgreSQLConnection.close()





def main():


    r_accounts = mint.get_accounts(True)
    accounts_df = convert_response_to_clean_df(r_accounts)
    write_to_postgres(accounts_df, 'accounts')


    r_budgets = mint.get_budgets()
    budgets_df = convert_budgets_response_to_df(r_budgets)
    write_to_postgres(budgets_df, 'budgets')


    transactions_df = mint.get_transactions()
    transactions_df = add_logging_fields_to_df(transactions_df)
    write_to_postgres(transactions_df, 'transactions')

    r_networth = mint.get_net_worth()
    networth_df = convert_networth_to_df(r_networth)
    write_to_postgres(networth_df, 'networth')


    r_creditscore = mint.get_credit_score()
    creditscore_df = convert_creditscore_to_df(r_creditscore)
    write_to_postgres(creditscore_df, 'creditscore')


    # Get bills

    print("Bills:")
    r_bills = mint.get_bills()
    # print(r_bills)
    bills_df = convert_bills_response_to_df(r_bills)
    write_to_postgres(bills_df, 'bills')



    # print("Investments:")
    # Get investments (holdings and transactions)
    r_investments = mint.get_invests_json()

    accounts_df, holdings_df, investment_transactions_df = convert_investment_response_to_dfs(r_investments)
    write_to_postgres(accounts_df, 'investment_accounts')
    write_to_postgres(holdings_df, "investment_holdings")
    write_to_postgres(investment_transactions_df, "investment_transactions")


    # print(investments)


if __name__ == "__main__":
    main()