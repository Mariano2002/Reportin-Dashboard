import argon
import datetime
import calendar
import pandas as pd
import numpy as np
import math
from pandas.tseries.offsets import BDay
from datetime import datetime
from functools import reduce
import calendar
import socket

hostname = socket.gethostname()
print(hostname)
if 'btreves01-vdi.pc.ny2' in hostname:
    app_home = '/home/staff/nfantini/fc-product/data/'
elif 'PF17TEXK' in hostname:
    app_home = 'c:/Users/Nick/PycharmProjects/fc-product/data/'
elif 'btreves-p330' in hostname:
    app_home = 'path3'
else:
    app_home = "c:/Users/Aleksander/PycharmProjects/fc-product/data/"
    pass
print(app_home)
# Step 0
# a Prepare Pandas with number format and source input data
pd.options.display.float_format = '{:.2f}'.format

session = argon.Session(server='BT',  username='anussbaum')
#print("before query")
#print(datetime.datetime.now())
#
##Write
base_local = app_home + "base_bal_df.pkl"
# rev_local = app_home + "base_rev_df.pkl"
revRates_local = app_home + "base_revRate_df.pkl"
volume_local = app_home + "volume_df.pkl"
revRatesLookup_local = app_home + "revRate_Lookup_df"

df_base_bal = session.get_csv("get(activeBalance30Days)") #activeBalance and 1
df_base_bal.to_pickle(base_local)
# df_base_revenue = session.get_csv("file(revenue15)")
# df_base_revenue.to_pickle(rev_local)

df_base_revenue_rate_type_lookup = session.get_csv("fcQuery(select * from EBS_CX_RO.AUM_SETUP_RATE_TYPE)")
df_base_revenue_rate_type_lookup.to_pickle(revRatesLookup_local)
#
df_base_volume = session.get_csv("ordFCVolumeQuery()")
df_base_volume.to_pickle(volume_local)

df_base_revenueRates = session.get_csv("ordFCRatesQuery()")
df_base_revenueRates.to_pickle(revRates_local)


df_base_bal = pd.read_pickle(base_local)
# df_base_revenue = pd.read_pickle(rev_local)
df_base_revenueRates = pd.read_pickle(revRates_local)
df_base_volume = pd.read_pickle(volume_local)
df_base_revenue_rate_type_lookup = pd.read_pickle(revRatesLookup_local)


#
# b.
# Create unique columns for revenue RATE_TYPE and INVESTOR_ACCOUNT_NAME
# df_base_revenue.rename(columns = {'RATE_TYPE': 'RATE_TYPE', 'INVESTOR_ACCOUNT_NAME': 'REV_INVESTOR_ACCOUNT_NAME'}, inplace= True)
# df_base_revenueRates.rename(columns = {'RATE_TYPE': 'RATE_TYPE', 'INVESTOR_ACCOUNT_NAME': 'REV_INVESTOR_ACCOUNT_NAME'}, inplace= True)
df_base_volume.rename(columns = {'provider.provider_name': 'PROVIDER_NAME', 'ii.inv_institution_name':'INSTITUTION_NAME',
                                 'trade.order_type': 'TRADE_ORDER_TYPE','trade.sum_total_cost_usde': 'TRADE_SUM_USDE',
                                 'inv_location.inv_location_name': 'INV_LOCATION_NAME', 'trade.order_type__sort_': 'ORDER_TYPE_SORT'}, inplace=True)



# Step 1
# Derive all relevant dates for comparison

prior_bday = pd.datetime.today() - pd.tseries.offsets.BDay(1)
# prior_bday_string = prior_bday.strftime("%m/%d/%Y")
# today_string = pd.datetime.today().strftime("%m/%d/%Y")
is_leap = calendar.isleap(pd.datetime.today().year)
# prior_bday = pd.to_datetime(prior_bday_string[:10], format='%m/%d/%Y')
#
# print(prior_bday)
# print()
# exit()

# Step 2
# Exclude test and operational only institutions, fund providers and accounts
# a. Institutions to exclude
report_only_inst = ['opsssga', 'ssgmbh', 'ssgmbdtest', 'sttsma', 'sttsweep']
# b. Fund Providers to exclude
report_only_fp = ['State Street Internal Transfer', 'Alternative Investments', 'Separately Managed Accounts', 'Unit Dealing - SSBG Agent Fund Trading', 'Unit Dealing - Caceis Luxembourg', 'Unit Dealing - BP2S Luxembourg','Unit Dealing - Allianz Global Invest']
# c. Accounts to exclude
report_only_inv_acc = ['BV Cash Management - Sweep', 'CSSIL Cash Management - Sweep', '9G - Sweep', '6G - Sweep', '6E - Sweep', '96 - Sweep', '3G - Sweep']

# Step 2.5
## revenueRates Rate Type Validation
# df_revenue_revenueRates = pd.merge(df_base_revenueRates, df_base_revenue, how= 'left', left_on = ['FUND_PROVIDER_ACCOUNT_ID'], right_on = ['FUND_PROVIDER_ID'])
#
# # print(df_base_revenue.info())
# df_base_revenueRates['RATE_TYPE'] = df_base_revenueRates['RATE_TYPE'].describe()
# df_base_revenue_rate_type_lookup.info()
# df_base_revenueRates.info()
# exit()

#TODO: Check/ reference rate types

df_base_bal['BALANCE_USDE'] = pd.to_numeric(df_base_bal['BALANCE_USDE'])
# print(df_base_bal.loc[df_base_bal['BALANCE_DT'] == '2021.01.19.00:00:00.000.000'].describe())
# df_base_bal.to_csv('df_base_bal.csv', index=False)

# exit()

# df_baseBal_rateType_lookup = pd.merge(df_base_bal, df_base_revenue_rate_type_lookup, how= 'inner', left_on = ['PROVIDER_ACCOUNT_ID'], right_on = ['FUND_PROVIDER_ACCOUNT_ID'])
# df_baseBal_rateType_lookup = df_baseBal_rateType_lookup.loc[df_baseBal_rateType_lookup['BALANCE_DT'] == '2021.01.20.00:00:00.000.000']
# df_baseBal_rateType_lookup.to_csv('df_baseBal_rateType_lookup.csv', index=False)
# exit()
# print('df_baseBal_rateType_lookup')
# print(df_baseBal_rateType_lookup.loc[df_baseBal_rateType_lookup['BALANCE_DT'] == '2021.01.19.00:00:00.000.000'].describe())
# print(df_base_revenueRates.info())
# print('df_baseBal_rateType_lookup Balance USDE sum')
# print(df_baseBal_rateType_lookup.loc[df_baseBal_rateType_lookup['BALANCE_DT'] == '2021.01.19.00:00:00.000.000' , 'BALANCE_USDE'].sum())
# exit()

# df_baseBal_rateType_lookup['RATE_TYPE_x'] = df_baseBal_rateType_lookup['RATE_TYPE_x'].astype(str)
# df_baseBal_rateType_lookup['PROVIDER_ACCOUNT_ID'] = df_baseBal_rateType_lookup['PROVIDER_ACCOUNT_ID'].astype(str)
df_base_revenueRates['RATE_TYPE'] = df_base_revenueRates['RATE_TYPE'].astype(str)
df_base_revenueRates['FUND_PROVIDER_ID'] = df_base_revenueRates['FUND_PROVIDER_ID'].astype(str)
#
# df_base_revenueRates_exam = df_base_revenueRates
# df_base_revenueRates_exam.to_csv('df_base_revenueRates.csv', index=False)

# exit()
# df_baseBal_rateType_lookup.to_csv('df_baseBal_rateType_lookup.csv', index=False)
# df_base_revenueRates.to_csv('df_base_revenueRates.csv', index=False)
# exit()

df_rateTypeBalance = pd.merge(df_base_bal, df_base_revenueRates, how= 'left', left_on = ['RATE_TYPE', 'FUND_ID'], right_on = ['RATE_TYPE','FUND_ID'], indicator=True)
# print('df_rateTypeBalance **')
# print(df_rateTypeBalance.loc[df_rateTypeBalance['BALANCE_DT'] == '2021.01.20.00:00:00.000.000'])
# print('df_rateTypeBalance Balance USDE sum')
# df_rateTypeBalance = df_rateTypeBalance.loc[df_rateTypeBalance['BALANCE_DT'] == '2021.01.20.00:00:00.000.000']
# df_rateTypeBalance.to_csv('df_rateTypeBalance02.csv', index=False)
#
# exit()


df_rateTypeBalance['BALANCE_USDE'] = pd.to_numeric(df_rateTypeBalance['BALANCE_USDE'])
# df_rateTypeBalance['BILLING_RATE'] = df_rateTypeBalance['BILLING_RATE'].astype(float)
#TODO CREATE WAIVER RATE
df_rateTypeBalance['ESTIMATED_REVENUE'] = (df_rateTypeBalance['BALANCE_USDE'] * df_rateTypeBalance['BILLING_RATE'])

#waiverRate =  0.25
#df_merged['WAIVER_AMT'] = df_merged['ESTIMATED_REVENUE'] * waiverRate
#df_merged['EXPECTED_WAIVER_AMT'] = df_merged['ESTIMATED_REVENUE'] - df_merged['WAIVER_AMT']



# print(df_rateTypeBalance.info())
# exit()

# Step 3
# Create merged dataframe of Volume and Revenue sets
# df_merged_old = pd.merge(df_base_bal, df_base_revenue, how= 'left', left_on = ['FUND_ID', 'PROVIDER_ACCOUNT_ID'], right_on = ['FUND_ID', 'FUND_PROVIDER_ACCOUNT_ID'])   # old merge
# print('df_merged_old **')
# print(df_merged_old.loc[df_merged_old['BALANCE_DT'] == '2021.01.20.00:00:00.000.000'].describe())
# exit()
df_merged = df_rateTypeBalance


df_merged['BALANCE_USDE'] = pd.to_numeric(df_merged['BALANCE_USDE'])
# df_merged['RATE'] = pd.to_numeric(df_merged['RATE'])
df_merged['COUNTRY_NAME'] = df_merged['COUNTRY_NAME'].astype(str)
df_merged['RATE_TYPE_CATEGORY'] =""
df_merged['REVENUE_CATEGORY'] =""


# print(df_merged)
# exit()

# Step 4
# Create categories for Domicile and Billing Categories

# #set balances to 0 when null
# df_merged.loc[df_merged['RATE_TYPE'] == 'NB_UNIT_DEAL', 'BALANCE_USDE'] = 0
# df_merged.loc[df_merged['RATE_TYPE'] == 'NB_UNIT_DEAL', 'BALANCE_USDE'] = 0
# df_merged.loc[df_merged['RATE_TYPE'] == None, 'BALANCE_USDE'] = 0
# df_merged.loc[df_merged['RATE_TYPE'] == None, 'BALANCE_USDE'] = 0

# print(df_base_bal['RATE_TYPE'].unique())
# exit()

#domicile cats
df_merged.loc[df_merged['COUNTRY_NAME'] != "United States", "COUNTRY_NAME"] = "Offshore"
df_merged.loc[df_merged['COUNTRY_NAME'] == "United States", "COUNTRY_NAME"] = "Onshore"

#billable cats
df_merged.loc[df_merged['RATE_TYPE'] == 'EXT', 'RATE_TYPE'] = "Direct"            #direct
df_merged.loc[df_merged['RATE_TYPE'] == 'EXT_FULL', 'RATE_TYPE'] = "Direct"            #direct
df_merged.loc[df_merged['RATE_TYPE'] == 'E_EXT', 'RATE_TYPE'] = "Direct"            #direct
df_merged.loc[df_merged['RATE_TYPE'] == 'OMNI', 'RATE_TYPE'] = "Omni"            #omni
df_merged.loc[df_merged['RATE_TYPE'] == 'OMNI_EMEA', 'RATE_TYPE'] = "Omni"            #omni
df_merged.loc[df_merged['RATE_TYPE'] == 'IB_BOA', 'RATE_TYPE'] = "Omni"            #direct
df_merged.loc[df_merged['RATE_TYPE'] == 'INT', 'RATE_TYPE'] = "3rd Party Sweep"            #sweep
df_merged.loc[df_merged['RATE_TYPE'] == 'AFFINT', 'RATE_TYPE'] = "Affiliate Sweep"            #sweep
df_merged.loc[df_merged['RATE_TYPE'] == 'NO', 'RATE_TYPE'] = "Affiliate Sweep (Non Standard)"            #sweep
df_merged.loc[df_merged['RATE_TYPE'] == 'CSC', 'RATE_TYPE'] = "SSgA Sweep"            #sweep
df_merged.loc[df_merged['RATE_TYPE'] == 'NB_UNIT_DEAL', 'RATE_TYPE'] = "Non Billable"
df_merged.loc[df_merged['RATE_TYPE'] == 'NB_OMNI_HOUSE', 'RATE_TYPE'] = "Non Billable"
df_merged.loc[df_merged['RATE_TYPE'] == 'NB_SMA_REPORT', 'RATE_TYPE'] = "Non Billable"
df_merged.loc[df_merged['RATE_TYPE'] == 'NB_DEALER_CODE_EXCLUDED', 'RATE_TYPE'] = "Non Billable"
df_merged.loc[df_merged['RATE_TYPE'] == 'NB_SWEEP_REPORT', 'RATE_TYPE'] = "Non Billable"

df_merged.loc[df_merged['RATE_TYPE'].isna(), 'RATE_TYPE'] = "Null Rate Type"


df_merged.loc[df_merged['RATE_TYPE'] == 'Direct', 'RATE_TYPE_CATEGORY'] = "PORTAL"
df_merged.loc[df_merged['RATE_TYPE'] == 'Omni', 'RATE_TYPE_CATEGORY'] = "PORTAL"
df_merged.loc[df_merged['RATE_TYPE'] == '3rd Party Sweep', 'RATE_TYPE_CATEGORY'] = "SWEEP"
df_merged.loc[df_merged['RATE_TYPE'] == 'Affiliate Sweep', 'RATE_TYPE_CATEGORY'] = "SWEEP"
df_merged.loc[df_merged['RATE_TYPE'] == 'Affiliate Sweep (Non Standard)', 'RATE_TYPE_CATEGORY'] = "SWEEP"
df_merged.loc[df_merged['RATE_TYPE'] == 'SSgA Sweep', 'RATE_TYPE_CATEGORY'] = "SWEEP"
df_merged.loc[df_merged['RATE_TYPE'] == 'Non Billable', 'RATE_TYPE_CATEGORY'] = "Non Billable"
df_merged.loc[df_merged['RATE_TYPE'] == 'Null Rate Type', 'RATE_TYPE_CATEGORY'] = "Null Rate Type"
df_merged.loc[df_merged['RATE_TYPE'] == 'INT_ALT', 'RATE_TYPE_CATEGORY'] = "INT_ALT"


df_merged.loc[df_merged['RATE_TYPE'] == 'Direct', 'REVENUE_CATEGORY'] = "Distributed"
df_merged.loc[df_merged['RATE_TYPE'] == 'Omni', 'REVENUE_CATEGORY'] = "Distributed"
df_merged.loc[df_merged['RATE_TYPE'] == '3rd Party Sweep', 'REVENUE_CATEGORY'] = "Distributed"
df_merged.loc[df_merged['RATE_TYPE'] == 'Affiliate Sweep', 'REVENUE_CATEGORY'] = "Distributed"
df_merged.loc[df_merged['RATE_TYPE'] == 'Affiliate Sweep (Non Standard)', 'REVENUE_CATEGORY'] = "Non-Distributed"
df_merged.loc[df_merged['RATE_TYPE'] == 'SSgA Sweep', 'REVENUE_CATEGORY'] = "Non-Distributed"
df_merged.loc[df_merged['RATE_TYPE'] == 'Non Billable', 'REVENUE_CATEGORY'] = "Non Billable"
df_merged.loc[df_merged['RATE_TYPE'].isna(), 'REVENUE_CATEGORY'] = "Null Rate Type"
df_merged.loc[df_merged['RATE_TYPE'] == 'INT_ALT', 'REVENUE_CATEGORY'] = "INT_ALT"

#@TODO Create column waiver waiver rates in preprocessing
print(df_merged['REVENUE_CATEGORY'].unique())
print(df_merged['RATE_TYPE_CATEGORY'].unique())




# Fund Category Category reclassification
df_merged.loc[df_merged['FUND_CATEGORY'] == 'SMA', 'FUND_CATEGORY'] = "SMA (Report Only)"
df_merged.loc[df_merged['FUND_CATEGORY'] == '--', 'FUND_CATEGORY'] = "Uncategorized"


# Step 5
# Some date functions
# a. Handle the argon ts format
df_merged['BALANCE_DT'] = pd.to_datetime(df_merged['BALANCE_DT'].str[:10], format='%Y.%m.%d')
# df_merged['BALANCE_DT'] = df_merged['BALANCE_DT'].dt.date

# print(df_merged['BALANCE_DT'])

# b. filter our any data later than the current date
df_merged = df_merged.loc[df_merged['BALANCE_DT'] <= prior_bday]
# c. format BALANCE_DT for plots
df_merged['BALANCE_DT'] = df_merged['BALANCE_DT'].apply(lambda x: x.strftime("%Y-%m-%d"))


# Step 6
# Exclusions
# a. INT_ALT excluded due to 0 assets and Non Billable due to 0 revenue
df_merged = df_merged.loc[df_merged['RATE_TYPE'] != 'INT_ALT']
df_merged = df_merged.loc[df_merged['RATE_TYPE_CATEGORY'] != 'INT_ALT']

df_merged = df_merged.loc[df_merged['RATE_TYPE'] != 'Non Billable']

# print(df_merged['RATE_TYPE_CATEGORY'].unique())
# print(df_merged['RATE_TYPE'].unique())
# # print(df_merged.loc[df_merged['RATE_TYPE_CATEGORY'] == 'INT_ALT'])
# exit()

# b. Manual Date exclusions for Holidays
df_merged = df_merged.loc[df_merged['BALANCE_DT'] != '2021-01-15']
df_merged = df_merged.loc[df_merged['BALANCE_DT'] != '2021-01-18']
df_merged = df_merged.loc[df_merged['BALANCE_DT'] != '2021-01-01']
df_merged = df_merged.loc[df_merged['BALANCE_DT'] != '2020-12-25']
df_merged = df_merged.loc[df_merged['BALANCE_DT'] != '2020-11-26']
df_merged = df_merged.loc[df_merged['BALANCE_DT'] != '2020-11-11']
df_merged = df_merged.loc[df_merged['BALANCE_DT'] != '2020-10-12']
df_merged = df_merged.loc[df_merged['BALANCE_DT'] != '2020-09-28']
df_merged = df_merged.loc[df_merged['BALANCE_DT'] != '2020-09-29']
df_merged = df_merged.loc[df_merged['BALANCE_DT'] != '2020-09-30']

# c. exclude test institutions and globallink cobrand
df_merged = df_merged.loc[df_merged['INV_INSTITUTION_NAME'] != 'testbank']
# df_merged = df_merged.loc[df_merged['CB_DESCRIPTION'] != 'Globallink Co-Brand']


# Step 7
# Estimate Revenue and determine number of years based on if current date is in leap year
if is_leap:
    df_merged['ESTIMATED_REVENUE'] = (df_merged['ESTIMATED_REVENUE'])/366
else:
    df_merged['ESTIMATED_REVENUE'] = (df_merged['ESTIMATED_REVENUE'])/365

df_base_bal = df_merged
# Step 8
# Prep
# a.  prep Filter out exclusions from step 2
df_base_bal = df_base_bal.query('INV_INSTITUTION_NAME not in @report_only_inst & PROVIDER_NAME not in @report_only_fp & INVESTOR_ACCOUNT_NAME not in @report_only_inv_acc')

print('df_base_bal')
# df_base_bal = df_base_bal.loc[df_base_bal['BALANCE_DT'] == '2021-01-20']
# df_base_bal.to_csv('df_base_bal.csv', index=False)
#
# exit()

# b. prep number formats
# TODO try to do upstream and see if merge doesnt break it
df_base_bal['BALANCE_AMT'] = pd.to_numeric(df_base_bal['BALANCE_AMT'])
df_base_bal['FX_RATE'] = pd.to_numeric(df_base_bal['FX_RATE'])
# df_base_bal['FX_RATE'] = df_base_bal['FX_RATE'].astype(int)
# df_base_revenue['BPS_RECEIVED'] = pd.to_numeric(df_base_revenue['BPS_RECEIVED'])

# c. derive some min/max dates from the dataset and format
earliest_dt = df_base_bal['BALANCE_DT'].min()
latest_dt = df_base_bal['BALANCE_DT'].max()

# earliest_dt = pd.to_datetime(earliest_dt[:10], format='%m/%d/%Y')
# latest_dt = pd.to_datetime(latest_dt[:10], format='%m/%d/%Y')
# latest_dt_str = latest_dt.strftime("%m/%d/%Y")
# latest_dt_m1 = latest_dt - pd.tseries.offsets.BDay(1)
# latest_dt_m1_str = latest_dt_m1.strftime("%m/%d/%Y")



# Step 9
# Creating report categories
# a Portal
state_street_institutions_total = ['statestreet', 'ssbwms', 'aft', 'ssbl', 'sscsil', 'secfin']
RT_not_null = pd.notnull(df_base_bal['RATE_TYPE'])
df_total_bal = df_base_bal.query('(INV_INSTITUTION_NAME not in @state_street_institutions_total & RATE_TYPE != "NO" & RATE_TYPE != "AFFINT" & RATE_TYPE.str.contains("NB_") == False & ACCOUNT_TYPE != 1 & ACCOUNT_TYPE != 2) | ((ACCOUNT_TYPE == "1" | ACCOUNT_TYPE == "2") & RATE_TYPE not in @RT_not_null) | ((ACCOUNT_TYPE == "1" | ACCOUNT_TYPE == "2") & RATE_TYPE in @RT_not_null)')

pvt_portaltotal = pd.pivot_table(df_total_bal, values='BALANCE_AMT', columns='BALANCE_DT', aggfunc=np.sum)
portal_total_sum = df_total_bal[['BALANCE_AMT']].sum()

def get_portaltotal():
    print('Total')
    print(pvt_portaltotal)
    print("\n")
    print(portal_total_sum)

# b Portal Direct

# TODO add account type to backend table and add corresponding filter

state_street_institutions = ['statestreet', 'ssbwms', 'aft', 'ssbl', 'sscsil', 'secfin']
df_direct_bal = df_base_bal.query('RATE_TYPE != "AFFINT" & RATE_TYPE != "NO" & RATE_TYPE != "INT" & (RATE_TYPE.str.contains("NB_") == False) & ACCOUNT_TYPE != "1" & ACCOUNT_TYPE !="2" & INV_INSTITUTION_NAME not in @state_street_institutions')
# date_before = datetime.date(2020, 6, 9)
# df_direct_bal = df_direct_bal[df_direct_bal['BALANCE_DT'] == date_before]

pvt_portaldirect = pd.pivot_table(df_direct_bal, values='BALANCE_AMT', columns='BALANCE_DT', aggfunc=np.sum)
portal_direct_sum = df_direct_bal[['BALANCE_AMT']].sum()

def get_portaldirect():
    print('Direct')
    print(pvt_portaldirect)
    print("\n")
    print(portal_direct_sum)

# c Omni

df_omni_bal = df_base_bal.query('ACCOUNT_TYPE =="1" | ACCOUNT_TYPE =="2"')

pvt_portalomni = pd.pivot_table(df_omni_bal, values='BALANCE_AMT', columns='BALANCE_DT', aggfunc=np.sum)
portal_omni_sum = df_omni_bal[['BALANCE_AMT']].sum()

def get_portalomni():
    print('Omni')
    print(pvt_portalomni)
    print("\n")
    print(portal_omni_sum)

# d Internal

df_int = df_base_bal.loc[(df_base_bal.INV_INSTITUTION_NAME != 'statestreet')]

state_street_institutions_internal = ['ssbwms','ssbcw','secfin','ssbl','aft','sscsil','ssb','sssl','sttgt']
df_int = df_int.query('INV_INSTITUTION_NAME not in @state_street_institutions_internal')
df_int = df_int.query('RATE_TYPE == "INT"')

pvt = pd.pivot_table(df_int, values='BALANCE_AMT', index='INV_INSTITUTION_NAME', aggfunc=np.sum)
print(pvt)
# print(df_int['RATE_TYPE'])
# print(df_int['INV_INSTITUTION_NAME'], sum(df_int['BALANCE_USDE']))

pvt_portalinternal = pd.pivot_table(df_int, values='BALANCE_AMT', columns='BALANCE_DT', aggfunc=np.sum)
portal_internal_sum = df_int[['BALANCE_AMT']].sum()

def get_portalinternal():
    print("Portal Internal")
    print(pvt_portalinternal)
    print("\n")
    print(portal_internal_sum)
# e Portal Total

report_dfs_portal = [df_total_bal, df_direct_bal, df_omni_bal, df_int]
rate_categories_portal = ['Direct', 'Omni', 'Internal']
# for d in report_dataframes:
# df_total_bal['REPORT_CATEGORY'] = 'PORTAL TOTAL'
df_direct_bal['REPORT_CATEGORY'] = 'Direct'
df_omni_bal['REPORT_CATEGORY'] = 'Omni'
df_int['REPORT_CATEGORY'] = 'Internal'
master_data_portal = pd.concat(report_dfs_portal)
# master_data['CATEGORY_CHECK_TOTAL'] = master_data.query('REPORT_CATEGORY in @rate_categories')[['BALANCE_USDE']].sum()
# print(master_data['CATEGORY_CHECK_TOTAL'])

pvt2 = pd.pivot_table(master_data_portal, values='ESTIMATED_REVENUE', index='REPORT_CATEGORY', columns='BALANCE_DT', aggfunc=np.sum, margins=True)
# pvt_rev1 = pd.pivot_table(master_data_portal, values='BALANCE_USDE', index='REPORT_CATEGORY', columns='BALANCE_DT', aggfunc=np.sum)

def get_portalreport():
    print("PORTAL REPORT")
    print(pvt2)

# f Sweep Total

df_sweep_total = df_base_bal.query('((INV_INSTITUTION_NAME == "statestreet" & (RATE_TYPE.str.contains("NB") == False & RATE_TYPE != "NO")) | (INV_INSTITUTION_NAME == "ssbwms") |  (INV_INSTITUTION_NAME ==  "secfin") | (PROVIDER_NAME == "CalPers Self Sweep") | (PROVIDER_NAME == "John Hancock Investments") |  (INV_INSTITUTION_NAME == "statestreet" & PROVIDER_NAME == "Goldman Sachs Asset Mgmt Grp" & RATE_TYPE == "E_EXT") | (INV_INSTITUTION_NAME == "statestreet" & PROVIDER_NAME == "Fidelity Investment Managers" & RATE_TYPE == "EXT") | (INV_INSTITUTION_NAME == "statestreet" & PROVIDER_NAME == "Western Asset Management Co. Ltd." & RATE_TYPE == "EXT") | RATE_TYPE == "AFFINT") & RATE_TYPE != ""')

pvt_sweeptotal = pd.pivot_table(df_sweep_total, values='BALANCE_AMT', columns='BALANCE_DT', aggfunc=np.sum)
sweep_total_sum = df_sweep_total[['BALANCE_AMT']].sum()


# g Affiliate Sweep


df_sweep_affilitate = df_base_bal.query('PROVIDER_NAME == "CalPers Self Sweep" | PROVIDER_NAME == "John Hancock Investments" | RATE_TYPE == "AFFINT" & PROVIDER_NAME != "SSgA Funds" ')

pvt_sweepaffiliate = pd.pivot_table(df_sweep_affilitate, values='BALANCE_AMT', columns='BALANCE_DT', aggfunc=np.sum)

sweep_affiliate_sum = df_sweep_affilitate[['BALANCE_AMT']].sum()


# h SSgA Sweep

df_sweep_ssga = df_base_bal.query('((INV_INSTITUTION_NAME == "statestreet" & (RATE_TYPE == "INT" | RATE_TYPE == "AFFINT" | RATE_TYPE == "CSC")) | INV_INSTITUTION_NAME == "ssbwms" | INV_INSTITUTION_NAME == "secfin") & PROVIDER_NAME == "SSgA Funds"')

pvt_sweepssga = pd.pivot_table(df_sweep_ssga, values='BALANCE_AMT', columns='BALANCE_DT', aggfunc=np.sum)
sweep_ssga_sum = df_sweep_ssga[['BALANCE_AMT']].sum()


# i 3rd Party Sweep

df_sweep_3rd = df_base_bal.query('(INV_INSTITUTION_NAME == "statestreet" & RATE_TYPE == "INT" & PROVIDER_NAME != "SSgA Funds" & PROVIDER_NAME != "CalPers Self Sweep" & PROVIDER_NAME != "John Hancock Investments")  |  (INV_INSTITUTION_NAME == "statestreet" & PROVIDER_NAME == "Goldman Sachs Asset Mgmt Grp" & RATE_TYPE == "E_EXT") | (INV_INSTITUTION_NAME == "statestreet" & PROVIDER_NAME == "Fidelity Investment Managers" & RATE_TYPE == "EXT") | (INV_INSTITUTION_NAME == "statestreet" & PROVIDER_NAME == "Western Asset Management Co. Ltd." & RATE_TYPE == "EXT") | (INV_INSTITUTION_NAME == "secfin" & PROVIDER_NAME != "SSgA Funds" & RATE_TYPE == "INT") & (RATE_TYPE != "AFFINT")')

# pvt_sweep3rd = pd.pivot_table(df_sweep_3rd, values='BALANCE_AMT', columns='BALANCE_DT', aggfunc=np.sum)
# pvt_sweep3rd = pd.pivot_table(df_sweep_3rd, values='BALANCE_AMT', columns='BALANCE_DT', index='INV_INSTITUTION_NAME', margins= True, aggfunc=np.sum)
# sweep_3rd_sum = df_sweep_3rd[['BALANCE_USDE']].sum()


# j Total Sweep

report_dfs_sweep = [df_sweep_total, df_sweep_affilitate, df_sweep_ssga, df_sweep_3rd]
rate_categories_sweep = ['Affiliate', 'SSgA', '3rd Party']

df_sweep_affilitate['REPORT_CATEGORY'] = 'Affiliate'
df_sweep_ssga['REPORT_CATEGORY'] = 'SSgA'
df_sweep_3rd['REPORT_CATEGORY'] = '3rd Party'
master_data_sweep = pd.concat(report_dfs_sweep)
pvt5 = pd.pivot_table(master_data_sweep, columns= 'BALANCE_DT', values='ESTIMATED_REVENUE', index='REPORT_CATEGORY', aggfunc=np.sum, margins= True)


# k Portal and Sweep

portal_sweep_concat = [pvt2, pvt5]
pvt10 = pd.concat(portal_sweep_concat)

# rev_concat = [pvt_rev1, pvt_rev2]
# rev_pvt = pd.concat(rev_concat)

# Step 10
# Establish dfs for Billing categories

#billable cats
# df_bcat_Portal = df_base_bal['RATE_TYPE'] = 'Portal'
# df_bcat_3rdPartySweep = df_base_bal.loc[df_base_bal['RATE_TYPE'] == '3rd Party Sweep']
# df_bcat_AffiliateSweep = df_base_bal['RATE_TYPE'] = 'Affiliate Sweep'
# df_bcat_AffiliateSweepNS = df_base_bal['RATE_TYPE'] = 'Affiliate Sweep (Non Standard)'
# df_bcat_SSgASweep = df_base_bal['RATE_TYPE'] = 'SSgA Sweep'
# df_bcat_NonBillable = df_base_bal['RATE_TYPE'] = 'Non Billable'
# df_bcat_NullRateType = df_base_bal['RATE_TYPE'] = 'Null Rate Type'

df_bcat_Portal = df_base_bal.loc[(df_base_bal['RATE_TYPE'] == 'Direct') | (df_base_bal['RATE_TYPE'] == 'Omni')]
df_bcat_3rdPartySweep = df_base_bal.loc[df_base_bal['RATE_TYPE'] == '3rd Party Sweep']
df_bcat_AffiliateSweep = df_base_bal.loc[df_base_bal['RATE_TYPE'] == 'Affiliate Sweep']
df_bcat_AffiliateSweepNS = df_base_bal.loc[df_base_bal['RATE_TYPE'] == 'Affiliate Sweep (Non Standard)'] #*
df_bcat_SSgASweep = df_base_bal.loc[df_base_bal['RATE_TYPE'] == 'SSgA Sweep']
df_bcat_NonBillable = df_base_bal.loc[df_base_bal['RATE_TYPE'] == 'Non Billable']  #*
df_bcat_NullRateType = df_base_bal.loc[df_base_bal['RATE_TYPE'] == 'Null Rate Type']
df_bcat_ExWaiver = df_base_bal.loc[df_base_bal['Rate_TYPE'] == 'Expected Waiver Rate']

print(df_base_bal['RATE_TYPE'].unique())
# exit()


# TODO: look at validation again
#
#
# portal_sums = portal_direct_sum + portal_omni_sum + portal_internal_sum
# sweep_sums = sweep_3rd_sum + sweep_affiliate_sum + sweep_ssga_sum
# print("\n")
#
# def validation():
#     print("VALIDATION")
#     print("Tests if sum of account balances is equal to total account balance.")
#     print("If TRUE, balance is equal. If FALSE, balance is not equal. Margin of error is (1).")
#     print("\n")
#     print("Portal Check")
#     print(math.isclose(portal_total_sum, portal_sums, abs_tol = 1))
#     print("\n")
#     print("Sweep Check")
#     print(math.isclose(sweep_total_sum, sweep_sums, abs_tol=1))
#     print("\n")
#
#
# validation()


# Step 11
# Establish foundation and pickle necessary dataframes for use in panel_process.py
# TODO: pickle this and unpickle it within the panel funciton; split out the panel function to a new file
#TODO Go over with blake
df_mdm = pd.concat([df_direct_bal, df_omni_bal, df_int, df_sweep_affilitate, df_sweep_ssga, df_sweep_3rd,])
df_mdm.to_pickle(app_home + "df_mdm.pkl")
df_direct_bal.to_pickle(app_home + "df_direct_bal.pkl")
df_omni_bal.to_pickle(app_home + "df_omni_bal.pkl")
df_int.to_pickle(app_home + "df_int.pkl")
df_sweep_affilitate.to_pickle(app_home + "df_sweep_affilitate.pkl")
df_sweep_ssga.to_pickle(app_home + "df_sweep_ssga.pkl")
df_sweep_3rd.to_pickle(app_home + "df_sweep_3rd.pkl")


df_mdm_bcat = pd.concat([df_bcat_Portal, df_bcat_3rdPartySweep, df_bcat_AffiliateSweep, df_bcat_AffiliateSweepNS, df_bcat_SSgASweep, df_bcat_NonBillable, df_bcat_NullRateType,])

df_mdm_bcat.to_pickle(app_home + "df_mdm_bcat.pkl")
df_bcat_Portal.to_pickle(app_home + "df_bcat_Portal.pkl")
df_bcat_3rdPartySweep.to_pickle(app_home + "df_bcat_3rdPartySweep.pkl")
df_bcat_AffiliateSweep.to_pickle(app_home + "df_bcat_AffiliateSweep.pkl")
df_bcat_AffiliateSweepNS.to_pickle(app_home + "df_bcat_AffiliateSweepNS.pkl")
df_bcat_SSgASweep.to_pickle(app_home + "df_bcat_SSgASweep.pkl")
df_bcat_NonBillable.to_pickle(app_home + "df_bcat_NonBillable.pkl")
df_bcat_NullRateType.to_pickle(app_home + "df_bcat_NullRateType.pkl")

df_base_volume.to_pickle(app_home + "df_base_volume.pkl")


print(df_base_bal['BALANCE_USDE'].count())
print(df_base_bal['BALANCE_USDE'].count())
print(df_base_bal['BALANCE_USDE'].count())
print('pre_processing complete')

exit()

