import argon
import datetime
import calendar
import pandas as pd
import numpy as np
import math
import panel as pn
import holoviews as hv
import dask.dataframe as dd
from bokeh.plotting import figure, output_file, show
from bokeh.layouts import column, grid
from bokeh.models import ColumnDataSource, CDSView, GroupFilter, Legend
from bokeh.models import NumeralTickFormatter, Toggle
from bokeh.models.tools import HoverTool
from bokeh.models.widgets import DataTable, DateFormatter, TableColumn, NumberFormatter, HTMLTemplateFormatter, \
    DataCube, GroupingInfo, SumAggregator, StringFormatter, NumberFormatter
from bokeh.models.widgets.tables import SelectEditor, NumberFormatter
from bokeh.models import LinearAxis, Range1d, LassoSelectTool
from bokeh.palettes import magma, Category20, Spectral
from pandas.tseries.offsets import BDay
from datetime import datetime
from functools import reduce
from bokeh.resources import INLINE
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email import encoders
import smtplib
import argparse
import locale

# TODO: pickle this (df_mdm renamed to d_master_for_panel)and unpickle it within the panel function, split out the panel function to a new file

# Step 1
# Import Pickled Files

# app_home = '/home/staff/nfantini/fc-product/data/'
app_home = "C:/Users/Mariano/PycharmProjects/Pune/fc-product/data/"  # running locally

# df_mdm_import = "df_mdm.pkl"
df_direct_bal_import = app_home + "df_direct_bal.pkl"
df_omni_bal_import = app_home + "df_omni_bal.pkl"
df_int_import = app_home + "df_int.pkl"
df_sweep_affilitate_import = app_home + "df_sweep_affilitate.pkl"
df_sweep_ssga_for_panel_import = app_home + "df_sweep_ssga.pkl"
df_sweep_3rd_import = app_home + "df_sweep_3rd.pkl"

# df_master_for_panel = pd.read_pickle(df_mdm_import)
df_direct_bal_for_panel = pd.read_pickle(df_direct_bal_import)
df_omni_bal_for_panel = pd.read_pickle(df_omni_bal_import)
df_int_for_panel = pd.read_pickle(df_int_import)
df_sweep_affilitate_for_panel = pd.read_pickle(df_sweep_affilitate_import)
df_sweep_ssga_for_panel = pd.read_pickle(df_sweep_ssga_for_panel_import)
df_sweep_3rd_for_panel = pd.read_pickle(df_sweep_3rd_import)

df_mdm_bcat_import = app_home + "df_mdm_bcat.pkl"
df_bcat_Portal_import = app_home + "df_bcat_Portal.pkl"
df_bcat_3rdPartySweep_import = app_home + "df_bcat_3rdPartySweep.pkl"
df_bcat_AffiliateSweep_import = app_home + "df_bcat_AffiliateSweep.pkl"
df_bcat_AffiliateSweepNS_import = app_home + "df_bcat_AffiliateSweepNS.pkl"
df_bcat_SSgASweep_import = app_home + "df_bcat_SSgASweep.pkl"
df_bcat_NonBillable_import = app_home + "df_bcat_NonBillable.pkl"
df_bcat_NullRateType_import = app_home + "df_bcat_NullRateType.pkl"

df_master_for_panel = pd.read_pickle(df_mdm_bcat_import)  # main data
df_bcat_Portal_for_panel = pd.read_pickle(df_bcat_Portal_import)
df_bcat_3rdPartySweep_for_panel = pd.read_pickle(df_bcat_3rdPartySweep_import)
df_bcat_AffiliateSweep_for_panel = pd.read_pickle(df_bcat_AffiliateSweep_import)
df_bcat_AffiliateSweepNS_for_panel = pd.read_pickle(df_bcat_AffiliateSweepNS_import)
df_bcat_SSgASweep_for_panel = pd.read_pickle(df_bcat_SSgASweep_import)
df_bcat_NonBillable_for_panel = pd.read_pickle(df_bcat_NonBillable_import)
df_bcat_NullRateType_for_panel = pd.read_pickle(df_bcat_NullRateType_import)

df_base_volume_import = app_home + "df_base_volume.pkl"
df_base_volume_for_panel = pd.read_pickle(df_base_volume_import)



# Step 2
# A. Redeclare necessary date variables
earliest_dt = df_master_for_panel['BALANCE_DT'].min()
latest_dt = df_master_for_panel['BALANCE_DT'].max()
latest_dt = pd.to_datetime(latest_dt[:10], format="%Y-%m-%d")
earliest_dt = pd.to_datetime(earliest_dt[:10], format="%Y-%m-%d")
latest_dt_str = latest_dt.strftime("%Y-%m-%d")
earliest_dt_str = earliest_dt.strftime("%Y-%m-%d")
latest_dt_m1 = latest_dt - pd.tseries.offsets.BDay(1)
latest_dt_m1_str = latest_dt_m1.strftime("%Y-%m-%d")
prior_bday = datetime.today() - pd.tseries.offsets.BDay(1)
prior_bday_string = prior_bday.strftime("%Y-%m-%d")


print(latest_dt)
# Step 3
# Concatenate all portal and sweep dataframes

df_portal = df_bcat_Portal_for_panel
df_sweep = pd.concat(
    [df_bcat_3rdPartySweep_for_panel, df_bcat_AffiliateSweep_for_panel, df_bcat_AffiliateSweepNS_for_panel,
     df_bcat_SSgASweep_for_panel])

# Step 4
# Declare "df_master_for_panel_current_dt" dataframes, same data as df_master_for_panel only the most recent day's data. This is used for the header.
header_ld_filter = (df_master_for_panel['BALANCE_DT'] == latest_dt_str)
df_master_for_panel_current_dt = df_master_for_panel.loc[header_ld_filter]

# Step 5
# Calculate change variables for header
filtered_total_bal_usde = (df_master_for_panel.loc[
                               df_master_for_panel['BALANCE_DT'] == latest_dt_str, 'BALANCE_USDE'].sum()) / 1000000000
filtered_total_bal_usde_fullvalue = (
    df_master_for_panel.loc[df_master_for_panel['BALANCE_DT'] == latest_dt_str, 'BALANCE_USDE'].sum())
filtered_total_bal_chg = ((filtered_total_bal_usde - (df_master_for_panel.loc[df_master_for_panel[
                                                                                  'BALANCE_DT'] == latest_dt_m1_str, 'BALANCE_USDE'].sum() / 1000000000)) / (
                                      df_master_for_panel.loc[df_master_for_panel[
                                                                  'BALANCE_DT'] == latest_dt_m1_str, 'BALANCE_USDE'].sum() / 1000000000)) * 100
filtered_total_bal_chg_usd = ((filtered_total_bal_usde - (df_master_for_panel.loc[df_master_for_panel[
                                                                                      'BALANCE_DT'] == latest_dt_m1_str, 'BALANCE_USDE'].sum() / 1000000000)))
filtered_total_er = df_master_for_panel.loc[
                        df_master_for_panel['BALANCE_DT'] == latest_dt_str, 'ESTIMATED_REVENUE'].sum() / 1000
filtered_total_er_chg = ((filtered_total_er - (df_master_for_panel.loc[df_master_for_panel[
                                                                           'BALANCE_DT'] == latest_dt_m1_str, 'ESTIMATED_REVENUE'].sum() / 1000)) / (
                                     df_master_for_panel.loc[df_master_for_panel[
                                                                 'BALANCE_DT'] == latest_dt_m1_str, 'ESTIMATED_REVENUE'].sum() / 1000)) * 100

# volume
buy_order_sum = (
    df_base_volume_for_panel.loc[df_base_volume_for_panel['TRADE_ORDER_TYPE'] == 'BUY', 'TRADE_SUM_USDE'].sum())
sell_order_sum = (
    df_base_volume_for_panel.loc[df_base_volume_for_panel['TRADE_ORDER_TYPE'] == 'SELL', 'TRADE_SUM_USDE'].sum())
net_activity = (buy_order_sum - sell_order_sum) / 1000000
total_activity = (df_base_volume_for_panel['TRADE_SUM_USDE'].sum()) / 1000000000
projected_balance = ((net_activity * 1000000) + filtered_total_bal_usde_fullvalue) / 1000000000
projected_balance_chg = ((projected_balance - filtered_total_bal_usde) / (filtered_total_bal_usde)) * 100


# Step 6
# Create pivot tables that will be used for the plots. Next 7 lines are old categories saved in case they are needed again

mdm_pvt = pd.pivot_table(df_master_for_panel, values=['BALANCE_USDE', 'ESTIMATED_REVENUE'], index=['BALANCE_DT'],
                         aggfunc=np.sum)
pvt_Portal_plot = pd.pivot_table(df_bcat_Portal_for_panel, values=['BALANCE_USDE', 'ESTIMATED_REVENUE'],
                                 index=['BALANCE_DT'], aggfunc=np.sum)
pvt_3rdPartySweep_plot = pd.pivot_table(df_bcat_3rdPartySweep_for_panel, values=['BALANCE_USDE', 'ESTIMATED_REVENUE'],
                                        index=['BALANCE_DT'], aggfunc=np.sum)
pvt_AffiliateSweep_plot = pd.pivot_table(df_bcat_AffiliateSweep_for_panel, values=['BALANCE_USDE', 'ESTIMATED_REVENUE'],
                                         index=['BALANCE_DT'], aggfunc=np.sum)
pvt_AffiliateSweepNS_plot = pd.pivot_table(df_bcat_AffiliateSweepNS_for_panel,
                                           values=['BALANCE_USDE', 'ESTIMATED_REVENUE'], index=['BALANCE_DT'],
                                           aggfunc=np.sum)
pvt_SSgASweep_plot = pd.pivot_table(df_bcat_SSgASweep_for_panel, values=['BALANCE_USDE', 'ESTIMATED_REVENUE'],
                                    index=['BALANCE_DT'], aggfunc=np.sum)

# Step 7
# Calculate and establish one day change columns for balance and estimated revenue  (for plots)
mdm_pvt['BALANCE_DAY_CHANGE_PERCENT'] = mdm_pvt['BALANCE_USDE'].pct_change(periods=1)
mdm_pvt['ESTIMATED_REVENUE_DAY_CHANGE_PERCENT'] = mdm_pvt['ESTIMATED_REVENUE'].pct_change(periods=1)
mdm_pvt['BALANCE_DAY_CHANGE'] = mdm_pvt['BALANCE_USDE'].shift(periods=1) - mdm_pvt['BALANCE_USDE'].shift(periods=0)
mdm_pvt['ESTIMATED_REVENUE_DAY_CHANGE'] = mdm_pvt['ESTIMATED_REVENUE'].shift(periods=1) - mdm_pvt[
    'ESTIMATED_REVENUE'].shift(periods=0)

# Step 8
# a. Create multi-index dataframe by date and report category
mdm_pvt_alt = pd.pivot_table(df_master_for_panel, values=['BALANCE_USDE', 'ESTIMATED_REVENUE'],
                             index=['BALANCE_DT', 'RATE_TYPE'], aggfunc=np.sum)
mdm_pvt_alt2 = pd.pivot_table(df_master_for_panel, values=['BALANCE_USDE', 'ESTIMATED_REVENUE'],
                              index=['BALANCE_DT', 'FUND_CATEGORY'], aggfunc=np.sum)
mdm_pvt_alt02 = pd.pivot_table(df_master_for_panel, values=['BALANCE_USDE', 'ESTIMATED_REVENUE'],
                               index=['BALANCE_DT', 'FUND_CATEGORY', 'INV_INSTITUTION_NAME', 'FUND_ID'], aggfunc=np.sum)
mdm_pvt_alt002 = mdm_pvt_alt02.reset_index()


data_for_df_whitelabel = df_master_for_panel.loc[df_master_for_panel['CB_DESCRIPTION'] != 'Globallink Co-Brand']
mdm_pvt_alt3 = pd.pivot_table(data_for_df_whitelabel, values=['BALANCE_USDE', 'ESTIMATED_REVENUE'],
                              index=['BALANCE_DT', 'CB_DESCRIPTION'], aggfunc=np.sum)

# b. Establish one day change columns for balance and estimated revenue
mdm_pvt_alt['BALANCE_DAY_CHANGE_PERCENT'] = mdm_pvt_alt.groupby(level='RATE_TYPE')['BALANCE_USDE'].pct_change(periods=1)
mdm_pvt_alt['ESTIMATED_REVENUE_DAY_CHANGE_PERCENT'] = mdm_pvt_alt.groupby(level='RATE_TYPE')[
    'ESTIMATED_REVENUE'].pct_change(periods=1)
mdm_pvt_alt['BALANCE_DAY_CHANGE'] = mdm_pvt_alt.groupby(level='RATE_TYPE')['BALANCE_USDE'].shift(periods=0) - \
                                    mdm_pvt_alt.groupby(level='RATE_TYPE')['BALANCE_USDE'].shift(periods=1)
mdm_pvt_alt['ESTIMATED_REVENUE_DAY_CHANGE'] = mdm_pvt_alt.groupby(level='RATE_TYPE')['ESTIMATED_REVENUE'].shift(
    periods=0) - mdm_pvt_alt.groupby(level='RATE_TYPE')['ESTIMATED_REVENUE'].shift(periods=1)

# c. Establish seven day change columns (currently unused but serve as template for future features
mdm_pvt_alt['BALANCE_7_DAY_CHANGE_PERCENT'] = mdm_pvt_alt.groupby(level='RATE_TYPE')['BALANCE_USDE'].pct_change(
    periods=7)
mdm_pvt_alt['ESTIMATED_REVENUE_7_DAY_CHANGE_PERCENT'] = mdm_pvt_alt.groupby(level='RATE_TYPE')[
    'ESTIMATED_REVENUE'].pct_change(periods=7)
mdm_pvt_alt['BALANCE_7_DAY_CHANGE'] = mdm_pvt_alt.groupby(level='RATE_TYPE')['BALANCE_USDE'].shift(periods=0) - \
                                      mdm_pvt_alt.groupby(level='RATE_TYPE')['BALANCE_USDE'].shift(periods=7)
mdm_pvt_alt['ESTIMATED_REVENUE_7_DAY_CHANGE'] = mdm_pvt_alt.groupby(level='RATE_TYPE')['ESTIMATED_REVENUE'].shift(
    periods=0) - mdm_pvt_alt.groupby(level='RATE_TYPE')['ESTIMATED_REVENUE'].shift(periods=7)

# d. Calculate Billable Basis Points Column by dividing estimated revenue by balance
mdm_pvt_alt['BILLABLE_BPS'] = (mdm_pvt_alt.groupby(level='RATE_TYPE')['ESTIMATED_REVENUE'].shift(0) /
                               mdm_pvt_alt.groupby(level='RATE_TYPE')['BALANCE_USDE'].shift(0)) * 1000

# e. Calculate category percent of total
mdm_pvt_alt['PCT_OF_TOTAL'] = (
            mdm_pvt_alt.groupby(level='RATE_TYPE')['BALANCE_USDE'].shift(0) / filtered_total_bal_usde_fullvalue)

# e. repeat steps A, B, and D with current date dataframe for header

mdm_pvt_current_dt = pd.pivot_table(df_master_for_panel_current_dt, values=['BALANCE_USDE', 'ESTIMATED_REVENUE'],
                                    index=['BALANCE_DT', 'RATE_TYPE'], aggfunc=np.sum)

mdm_pvt_current_dt['BALANCE_DAY_CHANGE_PERCENT'] = mdm_pvt_alt.groupby(level='RATE_TYPE')['BALANCE_USDE'].pct_change(
    periods=1)
mdm_pvt_current_dt['ESTIMATED_REVENUE_DAY_CHANGE_PERCENT'] = mdm_pvt_alt.groupby(level='RATE_TYPE')[
    'ESTIMATED_REVENUE'].pct_change(periods=1)
mdm_pvt_current_dt['BALANCE_DAY_CHANGE'] = mdm_pvt_alt.groupby(level='RATE_TYPE')['BALANCE_USDE'].shift(periods=0) - \
                                           mdm_pvt_alt.groupby(level='RATE_TYPE')['BALANCE_USDE'].shift(periods=1)
mdm_pvt_current_dt['ESTIMATED_REVENUE_DAY_CHANGE'] = mdm_pvt_alt.groupby(level='RATE_TYPE')['ESTIMATED_REVENUE'].shift(
    periods=0) - mdm_pvt_alt.groupby(level='RATE_TYPE')['ESTIMATED_REVENUE'].shift(periods=1)
mdm_pvt_current_dt['BILLABLE_BPS'] = (mdm_pvt_current_dt.groupby(level='RATE_TYPE')['ESTIMATED_REVENUE'].shift(0) /
                                      mdm_pvt_current_dt.groupby(level='RATE_TYPE')['BALANCE_USDE'].shift(
                                          0)) * 1000  # mdm_pvt_current_dt.groupby(level='RATE_TYPE')['BALANCE_USDE'].sum()      ## create weights here
mdm_pvt_current_dt['PCT_OF_TOTAL'] = (
            mdm_pvt_current_dt.groupby(level='RATE_TYPE')['BALANCE_USDE'].shift(0) / filtered_total_bal_usde_fullvalue)

mdm_pvt_alt.sort_values('BALANCE_DAY_CHANGE_PERCENT', inplace=True, ascending=False)
mdm_pvt_current_dt.sort_values('BALANCE_DAY_CHANGE_PERCENT', inplace=True, ascending=False)

mdm_pvt_current_dt.loc[('', 'TOTAL'), :] = (
mdm_pvt_current_dt['PCT_OF_TOTAL'].sum(), mdm_pvt_current_dt['BALANCE_USDE'].sum(),
mdm_pvt_current_dt['ESTIMATED_REVENUE'].sum(), filtered_total_bal_chg / 100, filtered_total_er_chg / 100,
mdm_pvt_current_dt['BALANCE_DAY_CHANGE'].sum(), mdm_pvt_current_dt['ESTIMATED_REVENUE_DAY_CHANGE'].sum(),
mdm_pvt_current_dt['BILLABLE_BPS'].mean())
#############################################################################################################

mdm_pvt_billing = pd.pivot_table(df_master_for_panel, values=['BALANCE_USDE', 'ESTIMATED_REVENUE'],
                                 index=['BALANCE_DT', 'RATE_TYPE'], aggfunc=np.sum)
mdm_pvt_billing.groupby(level='RATE_TYPE')

mdm_pvt_billing_current_dt = pd.pivot_table(df_master_for_panel_current_dt,
                                            values=['BALANCE_USDE', 'ESTIMATED_REVENUE'],
                                            index=['BALANCE_DT', 'RATE_TYPE'],
                                            aggfunc=np.sum)  # , margins= True, margins_name="Subtotal")
mdm_pvt_billing_current_dt.groupby(level='RATE_TYPE')

##one day

mdm_pvt_billing['BALANCE_DAY_CHANGE_PERCENT'] = mdm_pvt_billing.groupby(level='RATE_TYPE')['BALANCE_USDE'].pct_change(
    periods=1)
mdm_pvt_billing['ESTIMATED_REVENUE_DAY_CHANGE_PERCENT'] = mdm_pvt_billing.groupby(level='RATE_TYPE')[
    'ESTIMATED_REVENUE'].pct_change(periods=1)
mdm_pvt_billing['BALANCE_DAY_CHANGE'] = mdm_pvt_billing.groupby(level='RATE_TYPE')['BALANCE_USDE'].shift(periods=0) - \
                                        mdm_pvt_billing.groupby(level='RATE_TYPE')['BALANCE_USDE'].shift(periods=1)
mdm_pvt_billing['ESTIMATED_REVENUE_DAY_CHANGE'] = mdm_pvt_billing.groupby(level='RATE_TYPE')['ESTIMATED_REVENUE'].shift(
    periods=0) - mdm_pvt_billing.groupby(level='RATE_TYPE')['ESTIMATED_REVENUE'].shift(periods=1)

mdm_pvt_billing_current_dt['BALANCE_DAY_CHANGE_PERCENT'] = mdm_pvt_billing.groupby(level='RATE_TYPE')[
    'BALANCE_USDE'].pct_change(periods=1)
mdm_pvt_billing_current_dt['ESTIMATED_REVENUE_DAY_CHANGE_PERCENT'] = mdm_pvt_billing.groupby(level='RATE_TYPE')[
    'ESTIMATED_REVENUE'].pct_change(periods=1)
mdm_pvt_billing_current_dt['BALANCE_DAY_CHANGE'] = mdm_pvt_billing.groupby(level='RATE_TYPE')['BALANCE_USDE'].shift(
    periods=0) - mdm_pvt_billing.groupby(level='RATE_TYPE')['BALANCE_USDE'].shift(periods=1)
mdm_pvt_billing_current_dt['ESTIMATED_REVENUE_DAY_CHANGE'] = mdm_pvt_billing.groupby(level='RATE_TYPE')[
                                                                 'ESTIMATED_REVENUE'].shift(periods=0) - \
                                                             mdm_pvt_billing.groupby(level='RATE_TYPE')[
                                                                 'ESTIMATED_REVENUE'].shift(periods=1)

mdm_pvt_billing_current_dt['BILLABLE_BPS'] = (mdm_pvt_billing.groupby(level='RATE_TYPE')['ESTIMATED_REVENUE'].shift(
    periods=0) / mdm_pvt_billing.groupby(level='RATE_TYPE')['BALANCE_USDE'].shift(periods=0)) * 1000
mdm_pvt_billing_current_dt['PCT_OF_TOTAL'] = (
            mdm_pvt_billing_current_dt.groupby(level='RATE_TYPE')['BALANCE_USDE'].shift(
                periods=0) / filtered_total_bal_usde_fullvalue)

mdm_pvt_billing_current_dt['RRT_INDEX'] = [6, 1, 4, 2, 3, 5]

mdm_pvt_billing_current_dt.sort_values('RRT_INDEX', inplace=True, ascending=True)


mdm_pvt_billing_current_dt.loc[('', 'TOTAL'), :] = (
mdm_pvt_billing_current_dt['BALANCE_USDE'].sum(), mdm_pvt_billing_current_dt['ESTIMATED_REVENUE'].sum(),
filtered_total_bal_chg / 100, filtered_total_er_chg / 100, mdm_pvt_billing_current_dt['BALANCE_DAY_CHANGE'].sum(),
mdm_pvt_billing_current_dt['ESTIMATED_REVENUE_DAY_CHANGE'].sum(), mdm_pvt_billing_current_dt['BILLABLE_BPS'].mean(),
mdm_pvt_billing_current_dt['PCT_OF_TOTAL'].sum(), 0)

#############################################################################################################

hist_billing_cat_multiindex = pd.pivot_table(df_master_for_panel, values=['BALANCE_USDE'],
                                             index=['BALANCE_DT', 'RATE_TYPE_CATEGORY', 'RATE_TYPE'], aggfunc=np.sum)
hist_billing_cat_multiindex.groupby(level=['RATE_TYPE_CATEGORY', 'RATE_TYPE'])

billing_cat_multiindex = pd.pivot_table(df_master_for_panel_current_dt, values=['BALANCE_USDE'],
                                        index=['BALANCE_DT', 'RATE_TYPE_CATEGORY', 'RATE_TYPE'], aggfunc=np.sum)
billing_cat_multiindex.groupby(level=['RATE_TYPE_CATEGORY', 'RATE_TYPE'])


billing_cat_multiindex['BALANCE_DAY_CHANGE_PERCENT'] = \
hist_billing_cat_multiindex.groupby(level=['RATE_TYPE_CATEGORY', 'RATE_TYPE'])['BALANCE_USDE'].pct_change(
    periods=1) * 100

billing_cat_multiindex['BALANCE_DAY_CHANGE'] = (hist_billing_cat_multiindex.groupby(
    level=['RATE_TYPE_CATEGORY', 'RATE_TYPE'])['BALANCE_USDE'].shift(periods=0) - hist_billing_cat_multiindex.groupby(
    level=['RATE_TYPE_CATEGORY', 'RATE_TYPE'])['BALANCE_USDE'].shift(periods=1)) / 1000000



billing_cat_multiindex['PCT_OF_TOTAL'] = ((billing_cat_multiindex.groupby(level=['RATE_TYPE_CATEGORY', 'RATE_TYPE'])[
                                               'BALANCE_USDE'].shift(
    periods=0) / filtered_total_bal_usde_fullvalue) * 100)

billing_cat_multiindex['BALANCE_USDE'] = billing_cat_multiindex['BALANCE_USDE'] / 1000000


billing_cat_multiindex['EXAMPLE'] = 120


def color_negative_red(value):
    """
    Colors elements in a dateframe
    green if positive and red if
    negative. Does not color NaN
    values.
    """

    if value < 0:
        color = 'red'
    elif value > 0:
        color = 'green'
    else:
        color = 'black'

    return 'color: %s' % color


# drops date
billing_cat_multiindex = pd.pivot_table(billing_cat_multiindex, index=['RATE_TYPE_CATEGORY', 'RATE_TYPE'])

# re arranges columns
billing_cat_multiindex = billing_cat_multiindex[
    ['PCT_OF_TOTAL', 'BALANCE_USDE', 'BALANCE_DAY_CHANGE_PERCENT', 'BALANCE_DAY_CHANGE', 'EXAMPLE']]

# sets temporary index which will later be removed to set order of rows
billing_cat_multiindex['TEMP_INDEX'] = [1, 2, 3, 4, 5, 6]
billing_cat_multiindex.sort_values('TEMP_INDEX', inplace=True, ascending=True)

del billing_cat_multiindex['TEMP_INDEX']

# appends total row
billing_cat_multiindex.loc[('', 'Total'), :] = (
billing_cat_multiindex['PCT_OF_TOTAL'].sum(), billing_cat_multiindex['BALANCE_USDE'].sum(), filtered_total_bal_chg,
billing_cat_multiindex['BALANCE_DAY_CHANGE'].sum(), 200)

# renames columns in a visually appealing way
billing_cat_multiindex.columns = ['Pct of Total', 'Balance USDE', 'Balance Change (%)', 'Balance Change ($)', 'Example']
billing_cat_multiindex.index.names = ['', '']
###################################################################################################################

# applys styling to multiindex
billing_cat_multiindex = billing_cat_multiindex.style.applymap(color_negative_red, subset=['Balance Change (%)',
                                                                                           'Balance Change ($)']).format(
    {'Balance Change ($)': '${0:,.2f} MM', 'Balance Change (%)': '{0:.2f}%', 'Pct of Total': '{0:.2f}%',
     'Balance USDE': '${0:,.2f} MM'}).set_table_styles([
    {'selector': '.bk-root.div.bk.div.bk.div.bk.div.bk.div.bk',
     'props': [('width', '1201px')]},
    {'selector': 'table',
     'props': [('margin-left', 'auto'),
               ('margin-right', 'auto'),
               ('border', 'none'),
               ('border-collapse', 'collapse'),
               ('border-spacing', '0'),
               ('font-size', '12px'),  # does not work
               ('table-layout', 'fixed'),
               ('width', '1000px')]},  # does not work
    {'selector': ['tr', 'th', 'td'],
     'props': [('text-align', 'right'),
               ('vertical-align', 'middle'),
               ('padding', '0.5em 0.5em !important'),
               ('line-height', 'normal'),
               ('white-space', 'normal'),
               ('width', '1100px'),  # does not work
               ('max-width', 'none'),  # does not work
               ('border', 'none')]},
    {'selector': 'tbody',
     'props': [('display', 'table-row-group'),
               ('vertical-align', 'middle'),
               ('border-color', 'inherit')]},
    {'selector': 'tbody tr:nth-child(odd)',
     'props': [('background', '#f5f5f5')]},
    {'selector': 'tr:last-child',
     'props': [('font-weight', 'bold'),
               ('border-bottom', '1px double black')]},
    {'selector': 'thead',
     'props': [('border-bottom', '1px solid black'),  # does not work
               ('vertical-align', 'bottom')]},  # does not work
    {'selector': 'tr:hover',
     'props': [('background', 'lightblue !important'),
               ('cursor', 'pointer')]}])

# TODO distribution multiindex
# hist_distribution_multiindex = pd.pivot_table(df_master_for_panel, values=['BALANCE_USDE'], index= ['BALANCE_DT','REVENUE_CATEGORY', 'RATE_TYPE'], aggfunc=np.sum)
# hist_distribution_multiindex.groupby(level=['REVENUE_CATEGORY', 'RATE_TYPE'])
#
# distribution_multiindex = pd.pivot_table(df_master_for_panel_current_dt, values=['BALANCE_USDE'], index= ['BALANCE_DT','REVENUE_CATEGORY', 'RATE_TYPE'], aggfunc=np.sum)
# distribution_multiindex.groupby(level=['REVENUE_CATEGORY', 'RATE_TYPE'])
#
# print(distribution_multiindex)
# exit()


top10institutions = pd.pivot_table(df_master_for_panel_current_dt, values=['BALANCE_USDE'],
                                   index=['BALANCE_DT', 'INV_INSTITUTION_NAME', 'CB_DESCRIPTION', 'RATE_TYPE'],
                                   aggfunc=np.sum)
top10institutions.groupby(level=['INV_INSTITUTION_NAME', 'CB_DESCRIPTION', 'RATE_TYPE'])

hist_top10institutions = pd.pivot_table(df_master_for_panel, values=['BALANCE_USDE'],
                                        index=['BALANCE_DT', 'INV_INSTITUTION_NAME', 'CB_DESCRIPTION', 'RATE_TYPE'],
                                        aggfunc=np.sum)
hist_top10institutions.groupby(level=['INV_INSTITUTION_NAME', 'CB_DESCRIPTION', 'RATE_TYPE'])

top10institutions = top10institutions.reset_index()
hist_top10institutions = hist_top10institutions.reset_index()

top10institutions.loc[top10institutions['CB_DESCRIPTION'] != "Globallink Co-Brand", "INV_INSTITUTION_NAME"] = \
top10institutions['INV_INSTITUTION_NAME'] + ' ??'
hist_top10institutions.loc[hist_top10institutions['CB_DESCRIPTION'] != "Globallink Co-Brand", "INV_INSTITUTION_NAME"] = \
hist_top10institutions['INV_INSTITUTION_NAME'] + ' ??'


# drops cobrand
top10institutions = pd.pivot_table(top10institutions, index=['BALANCE_DT', 'INV_INSTITUTION_NAME', 'RATE_TYPE'])
hist_top10institutions = pd.pivot_table(hist_top10institutions,
                                        index=['BALANCE_DT', 'INV_INSTITUTION_NAME', 'RATE_TYPE'])

top10institutions['BALANCE_DAY_CHANGE_PERCENT'] = \
hist_top10institutions.groupby(level=['INV_INSTITUTION_NAME', 'RATE_TYPE'])['BALANCE_USDE'].pct_change(periods=1) * 100
# top10institutions['ESTIMATED_REVENUE_DAY_CHANGE_PERCENT'] = hist_billing_cat_multiindex.groupby(level=['RATE_TYPE_CATEGORY', 'RATE_TYPE'])['ESTIMATED_REVENUE'].pct_change(periods=1)
top10institutions['BALANCE_DAY_CHANGE'] = (hist_top10institutions.groupby(level=['INV_INSTITUTION_NAME', 'RATE_TYPE'])[
                                               'BALANCE_USDE'].shift(periods=0) -
                                           hist_top10institutions.groupby(level=['INV_INSTITUTION_NAME', 'RATE_TYPE'])[
                                               'BALANCE_USDE'].shift(periods=1)) / 1000000

top10institutions['BALANCE_USDE'] = top10institutions['BALANCE_USDE'] / 1000000

# drops date
top10institutions = pd.pivot_table(top10institutions, index=['INV_INSTITUTION_NAME', 'RATE_TYPE'])

# re arranges columns
top10institutions = top10institutions[['BALANCE_USDE', 'BALANCE_DAY_CHANGE_PERCENT', 'BALANCE_DAY_CHANGE']]

top10institutions.sort_values('BALANCE_USDE', inplace=True, ascending=False)
top10institutions = top10institutions.head(10)

top10institutions.columns = ['Balance USDE', 'Balance Change (%)', 'Balance Change ($)']
top10institutions.index.names = ['Institution', 'Rate Type']
# print(top10institutions)
# exit()

top10institutions = top10institutions.style.applymap(color_negative_red,
                                                     subset=['Balance Change (%)', 'Balance Change ($)']).format(
    {'Balance Change ($)': '${0:,.2f} MM', 'Balance Change (%)': '{0:.2f}%',
     'Balance USDE': '${0:,.2f} MM'}).set_table_styles([
    {'selector': '.bk-root.div.bk.div.bk.div.bk.div.bk.div.bk',
     'props': [('width', '1201px')]},
    {'selector': 'table',
     'props': [('margin-left', 'auto'),
               ('margin-right', 'auto'),
               ('border', 'none'),
               ('border-collapse', 'collapse'),
               ('border-spacing', '0'),
               ('font-size', '12px'),  # does not work
               ('table-layout', 'fixed'),
               ('width', '1000px')]},  # does not work
    {'selector': ['tr', 'th', 'td'],
     'props': [('text-align', 'right'),
               ('vertical-align', 'middle'),
               ('padding', '0.5em 0.5em !important'),
               ('line-height', 'normal'),
               ('white-space', 'normal'),
               ('width', '1100px'),  # does not work
               ('max-width', 'none'),  # does not work
               ('border', 'none')]},
    {'selector': 'tbody',
     'props': [('display', 'table-row-group'),
               ('vertical-align', 'middle'),
               ('border-color', 'inherit')]},
    {'selector': 'tbody tr:nth-child(odd)',
     'props': [('background', '#f5f5f5')]},
    {'selector': 'tr:last-child',
     'props': [('border-bottom', '1px double black')]},
    {'selector': 'thead',
     'props': [('border-bottom', '1px solid black'),  # does not work
               ('vertical-align', 'bottom')]},  # does not work
    {'selector': 'tr:hover',
     'props': [('background', 'lightblue !important'),
               ('cursor', 'pointer')]}])

top10providers = pd.pivot_table(df_master_for_panel_current_dt, values=['BALANCE_USDE'],
                                index=['BALANCE_DT', 'PROVIDER_NAME', 'RATE_TYPE'], aggfunc=np.sum)
top10providers.groupby(level=['PROVIDER_NAME', 'RATE_TYPE'])

hist_top10providers = pd.pivot_table(df_master_for_panel, values=['BALANCE_USDE'],
                                     index=['BALANCE_DT', 'PROVIDER_NAME', 'RATE_TYPE'], aggfunc=np.sum)
top10providers.groupby(level=['PROVIDER_NAME', 'RATE_TYPE'])

top10providers['BALANCE_DAY_CHANGE_PERCENT'] = hist_top10providers.groupby(level=['PROVIDER_NAME', 'RATE_TYPE'])[
                                                   'BALANCE_USDE'].pct_change(periods=1) * 100
# top10providers['ESTIMATED_REVENUE_DAY_CHANGE_PERCENT'] = hist_billing_cat_multiindex.groupby(level=['RATE_TYPE_CATEGORY', 'RATE_TYPE'])['ESTIMATED_REVENUE'].pct_change(periods=1)
top10providers['BALANCE_DAY_CHANGE'] = (hist_top10providers.groupby(level=['PROVIDER_NAME', 'RATE_TYPE'])[
                                            'BALANCE_USDE'].shift(periods=0) -
                                        hist_top10providers.groupby(level=['PROVIDER_NAME', 'RATE_TYPE'])[
                                            'BALANCE_USDE'].shift(periods=1)) / 1000000

top10providers['BALANCE_USDE'] = top10providers['BALANCE_USDE'] / 1000000

# drops date
top10providers = pd.pivot_table(top10providers, index=['PROVIDER_NAME', 'RATE_TYPE'])

# re arranges columns
top10providers = top10providers[['BALANCE_USDE', 'BALANCE_DAY_CHANGE_PERCENT', 'BALANCE_DAY_CHANGE']]

top10providers.sort_values('BALANCE_USDE', inplace=True, ascending=False)
top10providers = top10providers.head(10)

top10providers.columns = ['Balance USDE', 'Balance Change (%)', 'Balance Change ($)']
top10providers.index.names = ['Provider', 'Rate Type']

top10providers = top10providers.style.applymap(color_negative_red,
                                               subset=['Balance Change (%)', 'Balance Change ($)']).format(
    {'Balance Change ($)': '${0:,.2f} MM', 'Balance Change (%)': '{0:.2f}%',
     'Balance USDE': '${0:,.2f} MM'}).set_table_styles([
    {'selector': '.bk-root.div.bk.div.bk.div.bk.div.bk.div.bk',
     'props': [('width', '1201px')]},
    {'selector': 'table',
     'props': [('margin-left', 'auto'),
               ('margin-right', 'auto'),
               ('border', 'none'),
               ('border-collapse', 'collapse'),
               ('border-spacing', '0'),
               ('font-size', '12px'),  # does not work
               ('table-layout', 'fixed'),
               ('width', '1000px')]},  # does not work
    {'selector': ['tr', 'th', 'td'],
     'props': [('text-align', 'right'),
               ('vertical-align', 'middle'),
               ('padding', '0.5em 0.5em !important'),
               ('line-height', 'normal'),
               ('white-space', 'normal'),
               ('width', '1100px'),  # does not work
               ('max-width', 'none'),  # does not work
               ('border', 'none')]},
    {'selector': 'tbody',
     'props': [('display', 'table-row-group'),
               ('vertical-align', 'middle'),
               ('border-color', 'inherit')]},
    {'selector': 'tbody tr:nth-child(odd)',
     'props': [('background', '#f5f5f5')]},
    {'selector': 'tr:last-child',
     'props': [('border-bottom', '1px double black')]},
    {'selector': 'thead',
     'props': [('border-bottom', '1px solid black'),  # does not work
               ('vertical-align', 'bottom')]},  # does not work
    {'selector': 'tr:hover',
     'props': [('background', 'lightblue !important'),
               ('cursor', 'pointer')]}])

script = """
<script>
if (document.readyState === "complete") {
  $('.example').DataTable();
} else {
  $(document).ready(function () {
    $('.example').DataTable();
  })
}
</script>
"""

mdm_pvt_country = pd.pivot_table(df_master_for_panel, values=['BALANCE_USDE', 'ESTIMATED_REVENUE'],
                                 index=['BALANCE_DT', 'COUNTRY_NAME'], aggfunc=np.sum)
mdm_pvt_country.groupby(level='COUNTRY_NAME')

mdm_pvt_country_current_dt = pd.pivot_table(df_master_for_panel_current_dt,
                                            values=['BALANCE_USDE', 'ESTIMATED_REVENUE'],
                                            index=['BALANCE_DT', 'COUNTRY_NAME'],
                                            aggfunc=np.sum)  # , margins= True, margins_name="Subtotal")
mdm_pvt_country_current_dt.groupby(level='COUNTRY_NAME')

##one day
mdm_pvt_country['BALANCE_DAY_CHANGE_PERCENT'] = mdm_pvt_country.groupby(level='COUNTRY_NAME')[
    'BALANCE_USDE'].pct_change(periods=1)
mdm_pvt_country['ESTIMATED_REVENUE_DAY_CHANGE_PERCENT'] = mdm_pvt_country.groupby(level='COUNTRY_NAME')[
    'ESTIMATED_REVENUE'].pct_change(periods=1)
mdm_pvt_country['BALANCE_DAY_CHANGE'] = mdm_pvt_country.groupby(level='COUNTRY_NAME')['BALANCE_USDE'].shift(periods=0) - \
                                        mdm_pvt_country.groupby(level='COUNTRY_NAME')['BALANCE_USDE'].shift(periods=1)
mdm_pvt_country['ESTIMATED_REVENUE_DAY_CHANGE'] = mdm_pvt_country.groupby(level='COUNTRY_NAME')[
                                                      'ESTIMATED_REVENUE'].shift(periods=0) - \
                                                  mdm_pvt_country.groupby(level='COUNTRY_NAME')[
                                                      'ESTIMATED_REVENUE'].shift(periods=1)

mdm_pvt_country_current_dt['BALANCE_DAY_CHANGE_PERCENT'] = mdm_pvt_country.groupby(level='COUNTRY_NAME')[
    'BALANCE_USDE'].pct_change(periods=1)
mdm_pvt_country_current_dt['ESTIMATED_REVENUE_DAY_CHANGE_PERCENT'] = mdm_pvt_country.groupby(level='COUNTRY_NAME')[
    'ESTIMATED_REVENUE'].pct_change(periods=1)
mdm_pvt_country_current_dt['BALANCE_DAY_CHANGE'] = mdm_pvt_country.groupby(level='COUNTRY_NAME')['BALANCE_USDE'].shift(
    periods=0) - mdm_pvt_country.groupby(level='COUNTRY_NAME')['BALANCE_USDE'].shift(periods=1)
mdm_pvt_country_current_dt['ESTIMATED_REVENUE_DAY_CHANGE'] = mdm_pvt_country.groupby(level='COUNTRY_NAME')[
                                                                 'ESTIMATED_REVENUE'].shift(periods=0) - \
                                                             mdm_pvt_country.groupby(level='COUNTRY_NAME')[
                                                                 'ESTIMATED_REVENUE'].shift(periods=1)
mdm_pvt_country_current_dt['PCT_OF_TOTAL'] = (
            mdm_pvt_country_current_dt.groupby(level='COUNTRY_NAME')['BALANCE_USDE'].shift(
                periods=0) / filtered_total_bal_usde_fullvalue)

mdm_pvt_country.sort_values('BALANCE_DAY_CHANGE_PERCENT', inplace=True, ascending=False)
mdm_pvt_country_current_dt.sort_values('BALANCE_DAY_CHANGE_PERCENT', inplace=True, ascending=False)

mdm_pvt_country_current_dt.loc[('', 'TOTAL'), :] = (
mdm_pvt_country_current_dt['BALANCE_USDE'].sum(), mdm_pvt_country_current_dt['ESTIMATED_REVENUE'].sum(),
filtered_total_bal_chg / 100, filtered_total_er_chg / 100, mdm_pvt_country_current_dt['BALANCE_DAY_CHANGE'].sum(),
mdm_pvt_country_current_dt['ESTIMATED_REVENUE_DAY_CHANGE'].sum(), mdm_pvt_country_current_dt['PCT_OF_TOTAL'].sum())

# style pct of total as percentage
mdm_pvt_country_current_dt.PCT_OF_TOTAL = (mdm_pvt_country_current_dt.PCT_OF_TOTAL * 100).round(2)
mdm_pvt_country_current_dt.loc[mdm_pvt_country_current_dt['PCT_OF_TOTAL'] < 1, 'PCT_OF_TOTAL'] = "< 1.00"
mdm_pvt_country_current_dt['PCT_OF_TOTAL'] = mdm_pvt_country_current_dt['PCT_OF_TOTAL'].astype(str) + '%'

mdm_pvt_currency = pd.pivot_table(df_master_for_panel, values=['BALANCE_USDE', 'ESTIMATED_REVENUE'],
                                  index=['BALANCE_DT', 'CURRENCY_NAME'], aggfunc=np.sum)  # , margins= True)
mdm_pvt_currency.groupby(level='CURRENCY_NAME')

mdm_pvt_currency_current_dt = pd.pivot_table(df_master_for_panel_current_dt,
                                             values=['BALANCE_USDE', 'ESTIMATED_REVENUE'],
                                             index=['BALANCE_DT', 'CURRENCY_NAME'], aggfunc=np.sum)  # , margins= True)
mdm_pvt_currency_current_dt.groupby(level='CURRENCY_NAME')

##one day
mdm_pvt_currency['BALANCE_DAY_CHANGE_PERCENT'] = mdm_pvt_currency.groupby(level='CURRENCY_NAME')[
    'BALANCE_USDE'].pct_change(periods=1)
mdm_pvt_currency['ESTIMATED_REVENUE_DAY_CHANGE_PERCENT'] = mdm_pvt_currency.groupby(level='CURRENCY_NAME')[
    'ESTIMATED_REVENUE'].pct_change(periods=1)
mdm_pvt_currency['BALANCE_DAY_CHANGE'] = mdm_pvt_currency.groupby(level='CURRENCY_NAME')['BALANCE_USDE'].shift(
    periods=0) - mdm_pvt_currency.groupby(level='CURRENCY_NAME')['BALANCE_USDE'].shift(periods=1)
mdm_pvt_currency['ESTIMATED_REVENUE_DAY_CHANGE'] = mdm_pvt_currency.groupby(level='CURRENCY_NAME')[
                                                       'ESTIMATED_REVENUE'].shift(periods=0) - \
                                                   mdm_pvt_currency.groupby(level='CURRENCY_NAME')[
                                                       'ESTIMATED_REVENUE'].shift(periods=1)

mdm_pvt_currency_current_dt['BALANCE_DAY_CHANGE_PERCENT'] = mdm_pvt_currency.groupby(level='CURRENCY_NAME')[
    'BALANCE_USDE'].pct_change(periods=1)
mdm_pvt_currency_current_dt['ESTIMATED_REVENUE_DAY_CHANGE_PERCENT'] = mdm_pvt_currency.groupby(level='CURRENCY_NAME')[
    'ESTIMATED_REVENUE'].pct_change(periods=1)
mdm_pvt_currency_current_dt['BALANCE_DAY_CHANGE'] = mdm_pvt_currency.groupby(level='CURRENCY_NAME')[
                                                        'BALANCE_USDE'].shift(periods=0) - \
                                                    mdm_pvt_currency.groupby(level='CURRENCY_NAME')[
                                                        'BALANCE_USDE'].shift(periods=1)
mdm_pvt_currency_current_dt['ESTIMATED_REVENUE_DAY_CHANGE'] = mdm_pvt_currency.groupby(level='CURRENCY_NAME')[
                                                                  'ESTIMATED_REVENUE'].shift(periods=0) - \
                                                              mdm_pvt_currency.groupby(level='CURRENCY_NAME')[
                                                                  'ESTIMATED_REVENUE'].shift(periods=1)
mdm_pvt_currency_current_dt['PCT_OF_TOTAL'] = (
            mdm_pvt_currency_current_dt.groupby(level='CURRENCY_NAME')['BALANCE_USDE'].shift(
                periods=0) / filtered_total_bal_usde_fullvalue)

mdm_pvt_currency.sort_values('BALANCE_USDE', inplace=True, ascending=False)
mdm_pvt_currency_current_dt.sort_values('BALANCE_USDE', inplace=True, ascending=False)

mdm_pvt_currency_current_dt.loc[('', 'TOTAL'), :] = (
mdm_pvt_currency_current_dt['BALANCE_USDE'].sum(), mdm_pvt_currency_current_dt['ESTIMATED_REVENUE'].sum(),
filtered_total_bal_chg / 100, filtered_total_er_chg / 100, mdm_pvt_currency_current_dt['BALANCE_DAY_CHANGE'].sum(),
mdm_pvt_currency_current_dt['ESTIMATED_REVENUE_DAY_CHANGE'].sum(), mdm_pvt_currency_current_dt['PCT_OF_TOTAL'].sum())

# style pct of total as percentage
mdm_pvt_currency_current_dt.PCT_OF_TOTAL = (mdm_pvt_currency_current_dt.PCT_OF_TOTAL * 100).round(2)
mdm_pvt_currency_current_dt.loc[mdm_pvt_currency_current_dt['PCT_OF_TOTAL'] < 1, 'PCT_OF_TOTAL'] = "< 1.00"
mdm_pvt_currency_current_dt['PCT_OF_TOTAL'] = mdm_pvt_currency_current_dt['PCT_OF_TOTAL'].astype(str) + '%'

mdm_pvt_clients = pd.pivot_table(df_master_for_panel, values=['BALANCE_USDE', 'ESTIMATED_REVENUE'],
                                 index=['BALANCE_DT', 'PROVIDER_NAME'], aggfunc=np.sum)  # , margins= True)
mdm_pvt_clients.groupby(level='PROVIDER_NAME')

mdm_pvt_clients_current_dt = pd.pivot_table(df_master_for_panel_current_dt,
                                            values=['BALANCE_USDE', 'ESTIMATED_REVENUE'],
                                            index=['BALANCE_DT', 'PROVIDER_NAME'], aggfunc=np.sum)  # , margins= True)
mdm_pvt_clients_current_dt.groupby(level='PROVIDER_NAME')

##one day
mdm_pvt_clients['BALANCE_DAY_CHANGE_PERCENT'] = mdm_pvt_clients.groupby(level='PROVIDER_NAME')[
    'BALANCE_USDE'].pct_change(periods=1)
mdm_pvt_clients['ESTIMATED_REVENUE_DAY_CHANGE_PERCENT'] = mdm_pvt_clients.groupby(level='PROVIDER_NAME')[
    'ESTIMATED_REVENUE'].pct_change(periods=1)
mdm_pvt_clients['BALANCE_DAY_CHANGE'] = mdm_pvt_clients.groupby(level='PROVIDER_NAME')['BALANCE_USDE'].shift(
    periods=0) - mdm_pvt_clients.groupby(level='PROVIDER_NAME')['BALANCE_USDE'].shift(periods=1)
mdm_pvt_clients['ESTIMATED_REVENUE_DAY_CHANGE'] = mdm_pvt_clients.groupby(level='PROVIDER_NAME')[
                                                      'ESTIMATED_REVENUE'].shift(periods=0) - \
                                                  mdm_pvt_clients.groupby(level='PROVIDER_NAME')[
                                                      'ESTIMATED_REVENUE'].shift(periods=1)

mdm_pvt_clients_current_dt['BALANCE_DAY_CHANGE_PERCENT'] = mdm_pvt_clients.groupby(level='PROVIDER_NAME')[
    'BALANCE_USDE'].pct_change(periods=1)
mdm_pvt_clients_current_dt['ESTIMATED_REVENUE_DAY_CHANGE_PERCENT'] = mdm_pvt_clients.groupby(level='PROVIDER_NAME')[
    'ESTIMATED_REVENUE'].pct_change(periods=1)
mdm_pvt_clients_current_dt['BALANCE_DAY_CHANGE'] = mdm_pvt_clients.groupby(level='PROVIDER_NAME')['BALANCE_USDE'].shift(
    periods=0) - mdm_pvt_clients.groupby(level='PROVIDER_NAME')['BALANCE_USDE'].shift(periods=1)
mdm_pvt_clients_current_dt['ESTIMATED_REVENUE_DAY_CHANGE'] = mdm_pvt_clients.groupby(level='PROVIDER_NAME')[
                                                                 'ESTIMATED_REVENUE'].shift(periods=0) - \
                                                             mdm_pvt_clients.groupby(level='PROVIDER_NAME')[
                                                                 'ESTIMATED_REVENUE'].shift(periods=1)
mdm_pvt_clients_current_dt['PCT_OF_TOTAL'] = (
            mdm_pvt_clients_current_dt.groupby(level='PROVIDER_NAME')['BALANCE_USDE'].shift(
                periods=0) / filtered_total_bal_usde_fullvalue)

mdm_pvt_clients.sort_values('BALANCE_DAY_CHANGE', inplace=True, ascending=False)
mdm_pvt_clients_current_dt.sort_values('BALANCE_DAY_CHANGE', inplace=True, ascending=False)



mdm_pvt_direct = pd.pivot_table(df_direct_bal_for_panel, values=['BALANCE_USDE', 'ESTIMATED_REVENUE'],
                                index=['BALANCE_DT', 'REPORT_CATEGORY'], aggfunc=np.sum)


provider_vol_pvt = pd.pivot_table(df_base_volume_for_panel, values=['TRADE_SUM_USDE'],
                                  index=['PROVIDER_NAME', 'TRADE_ORDER_TYPE'], aggfunc=np.sum)  # , margins= True)
provider_vol_pvt = provider_vol_pvt.reset_index()

provider_vol_pvt['TRADE_SUM_USDE'] = (provider_vol_pvt.TRADE_SUM_USDE * (-1)).where(
    provider_vol_pvt.TRADE_ORDER_TYPE == 'SELL', provider_vol_pvt.TRADE_SUM_USDE)
provider_vol_pvt['TRADE_SUM_USDE'] = provider_vol_pvt['TRADE_SUM_USDE'] / 1000

provider_netActivity_pvt = pd.pivot_table(provider_vol_pvt, values=['TRADE_SUM_USDE'], index=['PROVIDER_NAME'],
                                          aggfunc=np.sum)
provider_netActivity_pvt.columns = ['Net Activity']
provider_netActivity_pvt.index.names = ['Provider Name']
provider_netActivity_pvt.sort_values('Net Activity', inplace=True, ascending=False)
provider_netActivity_pvt_head = provider_netActivity_pvt.head(10)
provider_netActivity_pvt_tail = provider_netActivity_pvt.tail(10)
provider_netActivity_pvt_head.index.names = ['Top 10 Providers']
provider_netActivity_pvt_tail.index.names = ['Bottom 10 Providers']


provider_netActivity_pvt_head = provider_netActivity_pvt_head.style.applymap(color_negative_red,
                                                                             subset=['Net Activity']).format(
    {'Net Activity': '${0:,.2f} K'}).set_table_styles([
    {'selector': '.bk-root.div.bk.div.bk.div.bk.div.bk.div.bk',
     'props': [('width', '1201px')]},
    {'selector': 'table',
     'props': [('margin-left', 'auto'),
               ('margin-right', 'auto'),
               ('border', 'none'),
               ('border-collapse', 'collapse'),
               ('border-spacing', '0'),
               ('font-size', '12px'),  # does not work
               ('table-layout', 'fixed'),
               ('width', '1000px')]},  # does not work
    {'selector': ['tr', 'th', 'td'],
     'props': [('text-align', 'right'),
               ('vertical-align', 'middle'),
               ('padding', '0.5em 0.5em !important'),
               ('line-height', 'normal'),
               ('white-space', 'normal'),
               ('width', '1100px'),  # does not work
               ('max-width', 'none'),  # does not work
               ('border', 'none')]},
    {'selector': 'tbody',
     'props': [('display', 'table-row-group'),
               ('vertical-align', 'middle'),
               ('border-color', 'inherit')]},
    {'selector': 'tbody tr:nth-child(odd)',
     'props': [('background', '#f5f5f5')]},
    {'selector': 'tr:last-child',
     'props': [('border-bottom', '1px double black')]},
    {'selector': 'thead',
     'props': [('border-bottom', '1px solid black'),  # does not work
               ('vertical-align', 'bottom')]},  # does not work
    {'selector': 'tr:hover',
     'props': [('background', 'lightblue !important'),
               ('cursor', 'pointer')]}])

provider_netActivity_pvt_tail = provider_netActivity_pvt_tail.style.applymap(color_negative_red,
                                                                             subset=['Net Activity']).format(
    {'Net Activity': '${0:,.2f} K'}).set_table_styles([
    {'selector': '.bk-root.div.bk.div.bk.div.bk.div.bk.div.bk',
     'props': [('width', '1201px')]},
    {'selector': 'table',
     'props': [('margin-left', 'auto'),
               ('margin-right', 'auto'),
               ('border', 'none'),
               ('border-collapse', 'collapse'),
               ('border-spacing', '0'),
               ('font-size', '12px'),  # does not work
               ('table-layout', 'fixed'),
               ('width', '1000px')]},  # does not work
    {'selector': ['tr', 'th', 'td'],
     'props': [('text-align', 'right'),
               ('vertical-align', 'middle'),
               ('padding', '0.5em 0.5em !important'),
               ('line-height', 'normal'),
               ('white-space', 'normal'),
               ('width', '1100px'),  # does not work
               ('max-width', 'none'),  # does not work
               ('border', 'none')]},
    {'selector': 'tbody',
     'props': [('display', 'table-row-group'),
               ('vertical-align', 'middle'),
               ('border-color', 'inherit')]},
    {'selector': 'tbody tr:nth-child(odd)',
     'props': [('background', '#f5f5f5')]},
    {'selector': 'tr:last-child',
     'props': [('border-bottom', '1px double black')]},
    {'selector': 'thead',
     'props': [('border-bottom', '1px solid black'),  # does not work
               ('vertical-align', 'bottom')]},  # does not work
    {'selector': 'tr:hover',
     'props': [('background', 'lightblue !important'),
               ('cursor', 'pointer')]}])

df_provider_bc = pd.pivot_table(df_master_for_panel, values=['BALANCE_USDE', 'ESTIMATED_REVENUE'],
                                index=['BALANCE_DT', 'PROVIDER_NAME', 'RATE_TYPE'], aggfunc=np.sum)  # , margins= True)
df_provider_bc.groupby(level=['PROVIDER_NAME', 'RATE_TYPE'])

##one day
df_provider_bc['BALANCE_DAY_CHANGE_PERCENT'] = df_provider_bc.groupby(level=['PROVIDER_NAME', 'RATE_TYPE'])[
    'BALANCE_USDE'].pct_change(periods=1)
df_provider_bc['ESTIMATED_REVENUE_DAY_CHANGE_PERCENT'] = df_provider_bc.groupby(level=['PROVIDER_NAME', 'RATE_TYPE'])[
    'ESTIMATED_REVENUE'].pct_change(periods=1)
df_provider_bc['BALANCE_DAY_CHANGE'] = df_provider_bc.groupby(level=['PROVIDER_NAME', 'RATE_TYPE'])[
                                           'BALANCE_USDE'].shift(periods=0) - \
                                       df_provider_bc.groupby(level=['PROVIDER_NAME', 'RATE_TYPE'])[
                                           'BALANCE_USDE'].shift(periods=1)
df_provider_bc['ESTIMATED_REVENUE_DAY_CHANGE'] = df_provider_bc.groupby(level=['PROVIDER_NAME', 'RATE_TYPE'])[
                                                     'ESTIMATED_REVENUE'].shift(periods=0) - \
                                                 df_provider_bc.groupby(level=['PROVIDER_NAME', 'RATE_TYPE'])[
                                                     'ESTIMATED_REVENUE'].shift(periods=1)


df_provider_bc['BILLABLE_BPS'] = (df_provider_bc.groupby(level=['PROVIDER_NAME', 'RATE_TYPE'])[
                                      'ESTIMATED_REVENUE'].shift(periods=0) /
                                  df_provider_bc.groupby(level=['PROVIDER_NAME', 'RATE_TYPE'])['BALANCE_USDE'].shift(
                                      periods=0)) * 1000
df_provider_bc.sort_values('BALANCE_DT', inplace=True, ascending=False)
df_provider_bc = df_provider_bc.fillna(0)

df_ins_bc = pd.pivot_table(df_master_for_panel, values=['BALANCE_USDE', 'ESTIMATED_REVENUE'],
                           index=['BALANCE_DT', 'INV_INSTITUTION_NAME', 'RATE_TYPE'],
                           aggfunc=np.sum)  # , margins= True)
df_ins_bc.groupby(level=['INV_INSTITUTION_NAME', 'RATE_TYPE'])

##one day
df_ins_bc['BALANCE_DAY_CHANGE_PERCENT'] = df_ins_bc.groupby(level=['INV_INSTITUTION_NAME', 'RATE_TYPE'])[
    'BALANCE_USDE'].pct_change(periods=1)
df_ins_bc['ESTIMATED_REVENUE_DAY_CHANGE_PERCENT'] = df_ins_bc.groupby(level=['INV_INSTITUTION_NAME', 'RATE_TYPE'])[
    'ESTIMATED_REVENUE'].pct_change(periods=1)
df_ins_bc['BALANCE_DAY_CHANGE'] = df_ins_bc.groupby(level=['INV_INSTITUTION_NAME', 'RATE_TYPE'])['BALANCE_USDE'].shift(
    periods=0) - df_ins_bc.groupby(level=['INV_INSTITUTION_NAME', 'RATE_TYPE'])['BALANCE_USDE'].shift(periods=1)
df_ins_bc['ESTIMATED_REVENUE_DAY_CHANGE'] = df_ins_bc.groupby(level=['INV_INSTITUTION_NAME', 'RATE_TYPE'])[
                                                'ESTIMATED_REVENUE'].shift(periods=0) - \
                                            df_ins_bc.groupby(level=['INV_INSTITUTION_NAME', 'RATE_TYPE'])[
                                                'ESTIMATED_REVENUE'].shift(periods=1)


df_ins_bc['BILLABLE_BPS'] = (df_ins_bc.groupby(level=['INV_INSTITUTION_NAME', 'RATE_TYPE'])['ESTIMATED_REVENUE'].shift(
    periods=0) / df_ins_bc.groupby(level=['INV_INSTITUTION_NAME', 'RATE_TYPE'])['BALANCE_USDE'].shift(periods=0)) * 1000
df_ins_bc.sort_values('BALANCE_DT', inplace=True, ascending=False)
df_ins_bc = df_ins_bc.fillna(0)

ins_cats = df_master_for_panel['INV_INSTITUTION_NAME'].unique()
unique_fps = df_master_for_panel['PROVIDER_NAME'].unique()



unique_billing_cats = df_master_for_panel['RATE_TYPE'].unique()

df_whitelabel = pd.pivot_table(data_for_df_whitelabel, values=['BALANCE_USDE', 'ESTIMATED_REVENUE'],
                               index=['BALANCE_DT', 'CB_DESCRIPTION', 'RATE_TYPE'], aggfunc=np.sum)  # , margins= True)
df_whitelabel.groupby(level=['CB_DESCRIPTION', 'RATE_TYPE'])

##one day
df_whitelabel['BALANCE_DAY_CHANGE_PERCENT'] = df_whitelabel.groupby(level=['CB_DESCRIPTION', 'RATE_TYPE'])[
    'BALANCE_USDE'].pct_change(periods=1)
df_whitelabel['ESTIMATED_REVENUE_DAY_CHANGE_PERCENT'] = df_whitelabel.groupby(level=['CB_DESCRIPTION', 'RATE_TYPE'])[
    'ESTIMATED_REVENUE'].pct_change(periods=1)
df_whitelabel['BALANCE_DAY_CHANGE'] = df_whitelabel.groupby(level=['CB_DESCRIPTION', 'RATE_TYPE'])[
                                          'BALANCE_USDE'].shift(periods=0) - \
                                      df_whitelabel.groupby(level=['CB_DESCRIPTION', 'RATE_TYPE'])[
                                          'BALANCE_USDE'].shift(periods=1)
df_whitelabel['ESTIMATED_REVENUE_DAY_CHANGE'] = df_whitelabel.groupby(level=['CB_DESCRIPTION', 'RATE_TYPE'])[
                                                    'ESTIMATED_REVENUE'].shift(periods=0) - \
                                                df_whitelabel.groupby(level=['CB_DESCRIPTION', 'RATE_TYPE'])[
                                                    'ESTIMATED_REVENUE'].shift(periods=1)


df_whitelabel['BILLABLE_BPS'] = (df_whitelabel.groupby(level=['CB_DESCRIPTION', 'RATE_TYPE'])[
                                     'ESTIMATED_REVENUE'].shift(periods=0) /
                                 df_whitelabel.groupby(level=['CB_DESCRIPTION', 'RATE_TYPE'])['BALANCE_USDE'].shift(
                                     periods=0)) * 1000
df_whitelabel.sort_values('BALANCE_DT', inplace=True, ascending=False)

unique_whitelabels = df_master_for_panel['CB_DESCRIPTION'].unique()

df_whitelabel = df_whitelabel.fillna(0)



direct_7d_pct_difference = (mdm_pvt_direct.pct_change(periods=7))
direct_7d_difference = mdm_pvt_direct.shift(periods=7) - mdm_pvt_direct
pd.options.display.float_format = '{:.4f}'.format




mdm_pvt_omni = pd.pivot_table(df_omni_bal_for_panel, values=['BALANCE_USDE', 'ESTIMATED_REVENUE'],
                              index=['BALANCE_DT', 'REPORT_CATEGORY'], aggfunc=np.sum)


mdm_pvt_int = pd.pivot_table(df_int_for_panel, values=['BALANCE_USDE', 'ESTIMATED_REVENUE'],
                             index=['BALANCE_DT', 'REPORT_CATEGORY'], aggfunc=np.sum)


mdm_pvt_sweep_affiliate = pd.pivot_table(df_sweep_affilitate_for_panel, values=['BALANCE_USDE', 'ESTIMATED_REVENUE'],
                                         index=['BALANCE_DT', 'REPORT_CATEGORY'], aggfunc=np.sum)


mdm_pvt_sweep_ssga = pd.pivot_table(df_sweep_ssga_for_panel, values=['BALANCE_USDE', 'ESTIMATED_REVENUE'],
                                    index=['BALANCE_DT', 'REPORT_CATEGORY'], aggfunc=np.sum)


mdm_pvt_3rd = pd.pivot_table(df_sweep_3rd_for_panel, values=['BALANCE_USDE', 'ESTIMATED_REVENUE'],
                             index=['BALANCE_DT', 'REPORT_CATEGORY'], aggfunc=np.sum)


df_for_plot = mdm_pvt.reset_index()
df_for_plot = df_for_plot.fillna(0)

df_for_stkbar = mdm_pvt_alt2.reset_index()
df_for_stkbar = df_for_stkbar.fillna(0)

df_for_stkbar2 = mdm_pvt_alt3.reset_index()
df_for_stkbar2 = df_for_stkbar2.fillna(0)

df_for_table = mdm_pvt_alt.reset_index()
df_for_stkbar2 = df_for_stkbar2.fillna(0)

df_for_table2 = mdm_pvt_country.reset_index()
df_for_stkbar2 = df_for_stkbar2.fillna(0)

df_for_table_billing = mdm_pvt_billing.reset_index()
df_for_table_billing = df_for_table_billing.fillna(0)

df_for_table3 = mdm_pvt_currency.reset_index()
df_for_table3 = df_for_table3.fillna(0)

df_for_table4 = mdm_pvt_clients.reset_index()
df_for_table4 = df_for_table4.fillna(0)

df_for_rc_header = mdm_pvt_current_dt.reset_index()
df_for_rc_header = df_for_rc_header.fillna(0)

df_for_country_header = mdm_pvt_country_current_dt.reset_index()
df_for_country_header = df_for_country_header.fillna(0)

df_for_billing_header = mdm_pvt_billing_current_dt.reset_index()
df_for_billing_header = df_for_billing_header.fillna(0)

df_for_ccy_header = mdm_pvt_currency_current_dt.reset_index()
df_for_ccy_header = df_for_ccy_header.fillna(0)  # , inplace=True)

df_for_clients_header = mdm_pvt_clients_current_dt.reset_index()
df_for_clients_header = df_for_clients_header.fillna(0)

df_for_ins_5_movers = df_ins_bc.reset_index()

df_for_ins_5_movers.sort_values('BALANCE_DAY_CHANGE', inplace=True, ascending=False)

df_for_t5m = df_for_clients_header.head(5)
df_for_t5m = df_for_t5m.fillna(0)
df_for_t5m = pd.pivot_table(df_for_t5m, values=['BALANCE_USDE', 'BALANCE_DAY_CHANGE_PERCENT', 'BALANCE_DAY_CHANGE'],
                            index=['PROVIDER_NAME'], aggfunc=np.sum)
df_for_t5m.sort_values('BALANCE_DAY_CHANGE', inplace=True, ascending=False)

preProviders = pd.pivot_table(df_for_clients_header,
                              values=['BALANCE_USDE', 'BALANCE_DAY_CHANGE_PERCENT', 'BALANCE_DAY_CHANGE'],
                              index=['PROVIDER_NAME'], aggfunc=np.sum)
preProviders = preProviders[['BALANCE_USDE', 'BALANCE_DAY_CHANGE_PERCENT', 'BALANCE_DAY_CHANGE']]
preProviders.sort_values('BALANCE_DAY_CHANGE', inplace=True, ascending=False)
preProviders['BALANCE_USDE'] = preProviders['BALANCE_USDE'] / 1000000
preProviders['BALANCE_DAY_CHANGE'] = preProviders['BALANCE_DAY_CHANGE'] / 1000000
preProviders.columns = ['Balance USDE', 'Balance Change (%)', 'Balance Change ($)']
preProviders.index.names = ['Provider Name']
preProviders.sort_values('Balance Change ($)', inplace=True, ascending=False)
top5providers = preProviders.head(5)
bot5providers = preProviders.tail(5)
top5providers.index.names = ['Greatest Provider Balance Increase']
bot5providers.index.names = ['Greatest Provider Balance Decrease']



df_for_ins_t5m = df_for_ins_5_movers.head(5)
df_for_ins_t5m = df_for_ins_t5m.fillna(0)

df_for_b5m = df_for_clients_header.tail(5)
df_for_b5m = df_for_b5m.fillna(0)
df_for_b5m = pd.pivot_table(df_for_b5m, values=['BALANCE_USDE', 'BALANCE_DAY_CHANGE_PERCENT', 'BALANCE_DAY_CHANGE'],
                            index=['PROVIDER_NAME'], aggfunc=np.sum)
df_for_b5m.sort_values('BALANCE_DAY_CHANGE', inplace=True, ascending=True)



df_for_ins_b5m = df_for_ins_5_movers.loc[df_for_ins_5_movers['INV_INSTITUTION_NAME'] != 'TOTAL']
df_for_ins_b5m.sort_values('BALANCE_DAY_CHANGE', inplace=True, ascending=True)
df_for_ins_b5m = df_for_ins_b5m.reset_index()
df_for_ins_b5m = df_for_ins_b5m.head(5)

df_for_t5m.columns = ['Balance USDE', 'Balance Change (%)', 'Balance Change ($)']
df_for_t5m['Balance Change ($)'] = df_for_t5m['Balance Change ($)'] / 1000000

df_for_b5m.columns = ['Balance USDE', 'Balance Change (%)', 'Balance Change ($)']
df_for_b5m['Balance Change ($)'] = df_for_b5m['Balance Change ($)'] / 1000000

top5providers = top5providers.style.applymap(color_negative_red,
                                             subset=['Balance Change (%)', 'Balance Change ($)']).format(
    {'Balance Change ($)': '${0:,.2f} MM', 'Balance Change (%)': '{0:.2f}%',
     'Balance USDE': '${0:,.2f} MM'}).set_table_styles([
    {'selector': '.bk-root.div.bk.div.bk.div.bk.div.bk.div.bk',
     'props': [('width', '1201px')]},
    {'selector': 'table',
     'props': [('margin-left', 'auto'),
               ('margin-right', 'auto'),
               ('border', 'none'),
               ('border-collapse', 'collapse'),
               ('border-spacing', '0'),
               ('font-size', '12px'),  # does not work
               ('table-layout', 'fixed'),
               ('width', '1000px')]},  # does not work
    {'selector': ['tr', 'th', 'td'],
     'props': [('text-align', 'right'),
               ('vertical-align', 'middle'),
               ('padding', '0.5em 0.5em !important'),
               ('line-height', 'normal'),
               ('white-space', 'normal'),
               ('width', '1100px'),  # does not work
               ('max-width', 'none'),  # does not work
               ('border', 'none')]},
    {'selector': 'tbody',
     'props': [('display', 'table-row-group'),
               ('vertical-align', 'middle'),
               ('border-color', 'inherit')]},
    {'selector': 'tbody tr:nth-child(odd)',
     'props': [('background', '#f5f5f5')]},
    {'selector': 'tr:last-child',
     'props': [('border-bottom', '1px double black')]},
    {'selector': 'thead',
     'props': [('border-bottom', '1px solid black'),  # does not work
               ('vertical-align', 'bottom')]},  # does not work
    {'selector': 'tr:hover',
     'props': [('background', 'lightblue !important'),
               ('cursor', 'pointer')]}])

bot5providers = bot5providers.style.applymap(color_negative_red,
                                             subset=['Balance Change (%)', 'Balance Change ($)']).format(
    {'Balance Change ($)': '${0:,.2f} MM', 'Balance Change (%)': '{0:.2f}%',
     'Balance USDE': '${0:,.2f} MM'}).set_table_styles([
    {'selector': '.bk-root.div.bk.div.bk.div.bk.div.bk.div.bk',
     'props': [('width', '1201px')]},
    {'selector': 'table',
     'props': [('margin-left', 'auto'),
               ('margin-right', 'auto'),
               ('border', 'none'),
               ('border-collapse', 'collapse'),
               ('border-spacing', '0'),
               ('font-size', '12px'),  # does not work
               ('table-layout', 'fixed'),
               ('width', '1000px')]},  # does not work
    {'selector': ['tr', 'th', 'td'],
     'props': [('text-align', 'right'),
               ('vertical-align', 'middle'),
               ('padding', '0.5em 0.5em !important'),
               ('line-height', 'normal'),
               ('white-space', 'normal'),
               ('width', '1100px'),  # does not work
               ('max-width', 'none'),  # does not work
               ('border', 'none')]},
    {'selector': 'tbody',
     'props': [('display', 'table-row-group'),
               ('vertical-align', 'middle'),
               ('border-color', 'inherit')]},
    {'selector': 'tbody tr:nth-child(odd)',
     'props': [('background', '#f5f5f5')]},
    {'selector': 'tr:last-child',
     'props': [('border-bottom', '1px double black')]},
    {'selector': 'thead',
     'props': [('border-bottom', '1px solid black'),  # does not work
               ('vertical-align', 'bottom')]},  # does not work
    {'selector': 'tr:hover',
     'props': [('background', 'lightblue !important'),
               ('cursor', 'pointer')]}])

ct_for_stkbar_bal = pd.crosstab(index=df_for_stkbar['BALANCE_DT'], columns=df_for_stkbar['FUND_CATEGORY'],
                                values=df_for_stkbar['BALANCE_USDE'], aggfunc=np.sum)


stkbar_src = ColumnDataSource(data=ct_for_stkbar_bal)


unique_fund_categories = df_master_for_panel['FUND_CATEGORY'].unique()


stkbar_cats = df_for_stkbar['FUND_CATEGORY'].unique()



p3 = figure(x_range=(df_for_stkbar['BALANCE_DT'].unique()), sizing_mode='stretch_width', plot_height=400,
            title="Balance By Fund Category", toolbar_location=None, tools="", margin=(50, 5, 5, 5))
colors = ["navy", "green", "red", "yellow", "orange", "teal", "purple"]
renderers = p3.vbar_stack(stkbar_cats, x='BALANCE_DT', source=stkbar_src, width=0.6,
                          color=Spectral[9])  # , legend_label = legend)




legend = Legend(items=[(x, [renderers[i]]) for i, x in enumerate(stkbar_cats)], location=(0, -30))
p3.add_layout(legend, 'right')

# import magma
p3.y_range.start = 0
p3.y_range.end = filtered_total_bal_usde_fullvalue + 25000000000
p3.x_range.range_padding = 0.1
p3.xgrid.grid_line_color = None
p3.axis.minor_tick_line_color = None
p3.outline_line_color = None
p3.xaxis.major_label_orientation = math.pi / 2
p3.yaxis[0].formatter = NumeralTickFormatter(format="$ 0.00 a")
p3.yaxis.axis_label = 'Balance USDE (Solid)'

hover = HoverTool()
hover.tooltips = [
    ('BALANCE_DATE', "@BALANCE_DT"),
    ('RATE_TYPE', "@RATE_TYPE"),
    ('BALANCE_USDE', "@BALANCE_USDE{$ 0.00 a}"),
]




ct_for_stkbar_bal2 = pd.crosstab(index=df_for_stkbar2['BALANCE_DT'], columns=df_for_stkbar2['CB_DESCRIPTION'],
                                 values=df_for_stkbar2['BALANCE_USDE'], aggfunc=np.sum)

stkbar_src2 = ColumnDataSource(data=ct_for_stkbar_bal2)



p4 = figure(x_range=(df_for_stkbar2['BALANCE_DT'].unique()), sizing_mode='stretch_width', plot_height=600,
            title="Balance By White Label", toolbar_location=None, tools="")


unique_whitelabels_m_gl = df_for_stkbar2['CB_DESCRIPTION'].unique()

renderers = p4.vbar_stack(unique_whitelabels_m_gl, x='BALANCE_DT', source=stkbar_src2, width=0.6,
                          color=Spectral[6])  # , legend_label = legend)




legend = Legend(items=[(x, [renderers[i]]) for i, x in enumerate(unique_whitelabels_m_gl)], location=(0, -30))
p4.add_layout(legend, 'right')

# import magma
p4.y_range.start = 0
p4.y_range.end = (df_for_stkbar2.loc[df_for_stkbar2['BALANCE_DT'] == latest_dt_str, 'BALANCE_USDE'].sum()) + 5000000000
p4.x_range.range_padding = 0.1
p4.xgrid.grid_line_color = None
p4.axis.minor_tick_line_color = None
p4.outline_line_color = None
p4.xaxis.major_label_orientation = math.pi / 2
p4.yaxis[0].formatter = NumeralTickFormatter(format="$ 0.00 a")
# p4.xaxis.axis_label = 'Balance Date'
p4.yaxis.axis_label = 'Balance USDE (Solid)'

hover = HoverTool()
hover.tooltips = [
    ('BALANCE_DATE', "@BALANCE_DT"),
    ('RATE_TYPE', "@RATE_TYPE"),
    ('BALANCE_USDE', "@BALANCE_USDE{$ 0.00 a}"),
]



source = ColumnDataSource(df_for_plot)

max_y = (df_for_plot.BALANCE_USDE.max()) + 10000000000

min_y = (df_for_plot.BALANCE_USDE.min()) - 10000000000

p = figure(sizing_mode='stretch_width', plot_height=500, y_range=(min_y, max_y),
           x_range=list(df_for_plot.BALANCE_DT.unique()))
# p = figure()
p.line(x='BALANCE_DT', y='BALANCE_USDE', source=source, line_width=1.5, color='green', legend_label="BALANCE_USDE")

p.title.text = 'Fund Connect Balance'
# p.xaxis.axis_label = 'BALANCE_DT'
p.yaxis.axis_label = 'BALANCE_USDE'
# ax2.axis_label = 'ESTIMATED_REVENUE'
p.xaxis.major_label_orientation = math.pi / 2
p.yaxis[0].formatter = NumeralTickFormatter(format="$ 0.00 a")
# ax2.formatter = NumeralTickFormatter(format="$ 0.00 a")

hover = HoverTool()
hover.tooltips = [
    ('BALANCE_DATE', "@BALANCE_DT"),
    ('BALANCE_USDE', "@BALANCE_USDE{$ 0.00 a}")
    # ('ESTIMATED_REVENUE', "@ESTIMATED_REVENUE{$ 0.00 a}")
]


p.add_tools(hover)

cat_choice_plot = pn.widgets.CheckButtonGroup(name='Category Analytics', options=list(unique_billing_cats),
                                              margin=(0, 20, 0, 0), button_type='primary')
ctcp = cat_choice_plot.value


@pn.depends(cat_choice_plot)
def function_for_plot(ctcp):
    df_for_plot = mdm_pvt.reset_index()
    source = ColumnDataSource(df_for_plot)

    df_for_Portal = pvt_Portal_plot.reset_index()
    Pplot = ColumnDataSource(df_for_Portal)

    df_for_3ps = pvt_3rdPartySweep_plot.reset_index()
    Thirdplot = ColumnDataSource(df_for_3ps)

    df_for_AffSweep = pvt_AffiliateSweep_plot.reset_index()
    AffSweepplot = ColumnDataSource(df_for_AffSweep)

    df_for_AffSweepNS = pvt_AffiliateSweepNS_plot.reset_index()
    AffNSplot = ColumnDataSource(df_for_AffSweepNS)

    df_for_ssga = pvt_SSgASweep_plot.reset_index()
    splot = ColumnDataSource(df_for_ssga)



    if cat_choice_plot.value:
        df_for_plot = df_for_plot[df_for_plot.RATE_TYPE.isin(ctcp)]

    max_y = (df_for_plot.BALANCE_USDE.max()) + 12500000000

    min_y = (df_for_plot.BALANCE_USDE.min()) - 12500000000

    p = figure(sizing_mode='stretch_width', plot_height=400, y_range=(min_y, max_y),
               x_range=list(df_for_plot.BALANCE_DT.unique()), toolbar_location="left")

    # Balance Lines
    p.line(x='BALANCE_DT', y='BALANCE_USDE', source=source, line_width=1.5, color='navy',
           legend_label='Total Balance USDE')

    p.legend.location = 'top_left'
    p.legend.click_policy = "hide"
    p.title.text = 'FC Balance'

    p.yaxis.axis_label = 'Balance USDE (Solid)'

    p.xaxis.major_label_orientation = math.pi / 2
    p.yaxis[0].formatter = NumeralTickFormatter(format="$ 0.00 a")


    p.toolbar.autohide = True
    hover = HoverTool()
    hover.tooltips = [
        ('BALANCE_DATE', "@BALANCE_DT"),
        ('BALANCE_USDE', "@BALANCE_USDE{$ 0.00 a}")
        # ('ESTIMATED_REVENUE', "@ESTIMATED_REVENUE{$ 0.00 a}")
    ]



    p.add_tools(hover)


    return p


cat_choice_plot = pn.widgets.CheckButtonGroup(name='Category Analytics', options=list(unique_billing_cats),
                                              margin=(0, 20, 0, 0), button_type='primary')
ctcp = cat_choice_plot.value


@pn.depends(cat_choice_plot)
def function_for_plot_balance(ctcp):
    df_for_plot = mdm_pvt.reset_index()
    source = ColumnDataSource(df_for_plot)

    df_for_plot = mdm_pvt.reset_index()
    source = ColumnDataSource(df_for_plot)

    df_for_Portal = pvt_Portal_plot.reset_index()
    Pplot = ColumnDataSource(df_for_Portal)

    df_for_3ps = pvt_3rdPartySweep_plot.reset_index()
    Thirdplot = ColumnDataSource(df_for_3ps)

    df_for_AffSweep = pvt_AffiliateSweep_plot.reset_index()
    AffSweepplot = ColumnDataSource(df_for_AffSweep)

    df_for_AffSweepNS = pvt_AffiliateSweepNS_plot.reset_index()
    AffNSplot = ColumnDataSource(df_for_AffSweepNS)

    df_for_ssga = pvt_SSgASweep_plot.reset_index()
    splot = ColumnDataSource(df_for_ssga)



    if cat_choice_plot.value:
        df_for_plot = df_for_plot[df_for_plot.REV_RATE_TYPE.isin(ctcp)]

    min_list = [df_for_Portal.BALANCE_USDE.min(), df_for_3ps.BALANCE_USDE.min(), df_for_AffSweep.BALANCE_USDE.min(),
                df_for_AffSweepNS.BALANCE_USDE.min(), df_for_ssga.BALANCE_USDE.min()]

    max_list = [df_for_Portal.BALANCE_USDE.max(), df_for_3ps.BALANCE_USDE.max(), df_for_AffSweep.BALANCE_USDE.max(),
                df_for_AffSweepNS.BALANCE_USDE.max(), df_for_ssga.BALANCE_USDE.max()]

    min_y2 = min(min_list) - 10000000000

    max_y2 = max(max_list) + 10000000000

    # max_y2

    p = figure(sizing_mode='stretch_width', plot_height=450, y_range=(min_y2, max_y2),
               x_range=list(df_for_plot.BALANCE_DT.unique()), toolbar_location="left")

    # Balance Lines

    p2 = p.line(x='BALANCE_DT', y='BALANCE_USDE', source=Pplot, line_width=1.5, color='red')
    p3 = p.line(x='BALANCE_DT', y='BALANCE_USDE', source=Thirdplot, line_width=1.5, color='green')
    p4 = p.line(x='BALANCE_DT', y='BALANCE_USDE', source=AffSweepplot, line_width=1.5, color='darkviolet')
    p5 = p.line(x='BALANCE_DT', y='BALANCE_USDE', source=AffNSplot, line_width=1.5, color='blue')

    p8 = p.line(x='BALANCE_DT', y='BALANCE_USDE', source=splot, line_width=1.5, color='teal')


    p.title.text = 'Performance by Business Line'

    p.yaxis.axis_label = 'Balance USDE (Solid)'

    p.xaxis.major_label_orientation = math.pi / 2
    p.yaxis[0].formatter = NumeralTickFormatter(format="$ 0.00 a")
    p.toolbar.autohide = True

    legend = Legend(items=[
        ("Portal", [p2]),
        ("3rd Party", [p3]),
        ("Affiliate Sweep", [p4]),
        ("SSgA Sweep", [p8]),
        ("Affiliate Non Standard", [p5]),
    ], location="center", orientation="horizontal")

    p.add_layout(legend, 'below')
    p.legend.click_policy = "hide"

    hover = HoverTool()
    hover.tooltips = [
        ('BALANCE_DATE', "@BALANCE_DT"),
        ('BALANCE_USDE', "@BALANCE_USDE{$ 0.00 a}")
        # ('ESTIMATED_REVENUE', "@ESTIMATED_REVENUE{$ 0.00 a}")
    ]

    p.add_tools(hover)


    return p


cat_choice_plot = pn.widgets.CheckButtonGroup(name='Category Analytics', options=list(unique_billing_cats),
                                              margin=(0, 20, 0, 0), button_type='primary')
ctcp = cat_choice_plot.value


@pn.depends(cat_choice_plot)
def function_for_plot_er(ctcp):
    df_for_plot = mdm_pvt.reset_index()
    source = ColumnDataSource(df_for_plot)

    df_for_Portal = pvt_Portal_plot.reset_index()
    Pplot = ColumnDataSource(df_for_Portal)

    df_for_3ps = pvt_3rdPartySweep_plot.reset_index()
    Thirdplot = ColumnDataSource(df_for_3ps)

    df_for_AffSweep = pvt_AffiliateSweep_plot.reset_index()
    AffSweepplot = ColumnDataSource(df_for_AffSweep)

    df_for_AffSweepNS = pvt_AffiliateSweepNS_plot.reset_index()
    AffNSplot = ColumnDataSource(df_for_AffSweepNS)

    df_for_ssga = pvt_SSgASweep_plot.reset_index()
    splot = ColumnDataSource(df_for_ssga)


    if cat_choice_plot.value:
        df_for_plot = df_for_plot[df_for_plot.REV_RATE_TYPE.isin(ctcp)]

    p = figure(sizing_mode='stretch_width', plot_height=750, x_range=list(df_for_plot.BALANCE_DT.unique()),
               toolbar_location="left")

    p2 = p.line(x='BALANCE_DT', y='BALANCE_USDE', source=Pplot, line_width=1.5, color='red')
    p3 = p.line(x='BALANCE_DT', y='BALANCE_USDE', source=Thirdplot, line_width=1.5, color='green')
    p4 = p.line(x='BALANCE_DT', y='BALANCE_USDE', source=AffSweepplot, line_width=1.5, color='darkviolet')
    p5 = p.line(x='BALANCE_DT', y='BALANCE_USDE', source=AffNSplot, line_width=1.5, color='blue')
    p8 = p.line(x='BALANCE_DT', y='BALANCE_USDE', source=splot, line_width=1.5, color='teal')


    p.title.text = 'FC Estimated Revenue'

    p.yaxis.axis_label = 'Estimated Revenue USDE'

    p.xaxis.major_label_orientation = math.pi / 2
    p.yaxis[0].formatter = NumeralTickFormatter(format="$ 0.00 a")

    p.toolbar.autohide = True


    legend = Legend(items=[

        ("Portal", [p2]),
        ("3rd Party", [p3]),
        ("Affiliate Sweep", [p4]),
        ("SSgA Sweep", [p8]),
        ("Affiliate Non Standard", [p5]),

    ], location="center")

    p.add_layout(legend, 'right')
    p.legend.click_policy = "hide"

    hover = HoverTool()
    hover.tooltips = [
        ('BALANCE_DATE', "@BALANCE_DT"),
        ('BALANCE_USDE', "@BALANCE_USDE{$ 0.00 a}")

    ]



    p.add_tools(hover)



    return p


p3Source = ColumnDataSource(mdm_pvt_alt)

data1 = dict(
    date=df_for_table['BALANCE_DT'],
    revcat=df_for_table['RATE_TYPE'],
    b_usde=df_for_table['BALANCE_USDE'],
    e_rev=df_for_table['ESTIMATED_REVENUE'],
    balance_usde_1_day_chg_pct=df_for_table['BALANCE_DAY_CHANGE_PERCENT'],
    estimated_revenue_1_day_chg_pct=df_for_table['ESTIMATED_REVENUE_DAY_CHANGE_PERCENT'],
    balance_usde_1_day_chg=df_for_table['BALANCE_DAY_CHANGE'],
    estimated_revenue_1_day_chg=df_for_table['ESTIMATED_REVENUE_DAY_CHANGE'],

)
source1 = ColumnDataSource(data1)



data2 = dict(
    date=df_for_table2['BALANCE_DT'],

    b_usde=df_for_table2['BALANCE_USDE'],
    e_rev=df_for_table2['ESTIMATED_REVENUE'],
    balance_usde_1_day_chg_pct=df_for_table2['BALANCE_DAY_CHANGE_PERCENT'],
    estimated_revenue_1_day_chg_pct=df_for_table2['ESTIMATED_REVENUE_DAY_CHANGE_PERCENT'],
    balance_usde_1_day_chg=df_for_table2['BALANCE_DAY_CHANGE'],
    estimated_revenue_1_day_chg=df_for_table2['ESTIMATED_REVENUE_DAY_CHANGE'],
    country=df_for_table2['COUNTRY_NAME'],
)

data_country_header = dict(
    date=df_for_country_header['BALANCE_DT'],
    pct_of_total=df_for_country_header['PCT_OF_TOTAL'],
    b_usde=df_for_country_header['BALANCE_USDE'],
    e_rev=df_for_country_header['ESTIMATED_REVENUE'],
    balance_usde_1_day_chg_pct=df_for_country_header['BALANCE_DAY_CHANGE_PERCENT'],
    estimated_revenue_1_day_chg_pct=df_for_country_header['ESTIMATED_REVENUE_DAY_CHANGE_PERCENT'],
    balance_usde_1_day_chg=df_for_country_header['BALANCE_DAY_CHANGE'],
    estimated_revenue_1_day_chg=df_for_country_header['ESTIMATED_REVENUE_DAY_CHANGE'],
    country=df_for_country_header['COUNTRY_NAME'],
)
source_country_header = ColumnDataSource(data_country_header)

data_b = dict(
    date=df_for_billing_header['BALANCE_DT'],
    pct_of_total=df_for_billing_header['PCT_OF_TOTAL'],
    b_cat=df_for_billing_header['RATE_TYPE'],
    b_usde=df_for_billing_header['BALANCE_USDE'],
    e_rev=df_for_billing_header['ESTIMATED_REVENUE'],
    balance_usde_1_day_chg_pct=df_for_billing_header['BALANCE_DAY_CHANGE_PERCENT'],
    estimated_revenue_1_day_chg_pct=df_for_billing_header['ESTIMATED_REVENUE_DAY_CHANGE_PERCENT'],
    balance_usde_1_day_chg=df_for_billing_header['BALANCE_DAY_CHANGE'],
    estimated_revenue_1_day_chg=df_for_billing_header['ESTIMATED_REVENUE_DAY_CHANGE'],

)

data_billing_header = dict(
    date=df_for_billing_header['BALANCE_DT'],
    pct_of_total=df_for_billing_header['PCT_OF_TOTAL'],
    b_cat=df_for_billing_header['RATE_TYPE'],
    b_usde=df_for_billing_header['BALANCE_USDE'],
    e_rev=df_for_billing_header['ESTIMATED_REVENUE'],
    billable_bps=df_for_billing_header['BILLABLE_BPS'],
    balance_usde_1_day_chg_pct=df_for_billing_header['BALANCE_DAY_CHANGE_PERCENT'],
    estimated_revenue_1_day_chg_pct=df_for_billing_header['ESTIMATED_REVENUE_DAY_CHANGE_PERCENT'],
    balance_usde_1_day_chg=df_for_billing_header['BALANCE_DAY_CHANGE'],
    estimated_revenue_1_day_chg=df_for_billing_header['ESTIMATED_REVENUE_DAY_CHANGE'],

)
source_billing_header = ColumnDataSource(data_billing_header)

data3 = dict(
    date=df_for_table3['BALANCE_DT'],

    b_usde=df_for_table3['BALANCE_USDE'],
    e_rev=df_for_table3['ESTIMATED_REVENUE'],
    balance_usde_1_day_chg_pct=df_for_table3['BALANCE_DAY_CHANGE_PERCENT'],
    estimated_revenue_1_day_chg_pct=df_for_table3['ESTIMATED_REVENUE_DAY_CHANGE_PERCENT'],
    balance_usde_1_day_chg=df_for_table3['BALANCE_DAY_CHANGE'],
    estimated_revenue_1_day_chg=df_for_table3['ESTIMATED_REVENUE_DAY_CHANGE'],

    ccy_name=df_for_table3['CURRENCY_NAME'],
)
source3 = ColumnDataSource(data3)

data_ccy_header = dict(
    date=df_for_ccy_header['BALANCE_DT'],
    pct_of_total=df_for_ccy_header['PCT_OF_TOTAL'],
    b_usde=df_for_ccy_header['BALANCE_USDE'],
    e_rev=df_for_ccy_header['ESTIMATED_REVENUE'],
    balance_usde_1_day_chg_pct=df_for_ccy_header['BALANCE_DAY_CHANGE_PERCENT'],
    estimated_revenue_1_day_chg_pct=df_for_ccy_header['ESTIMATED_REVENUE_DAY_CHANGE_PERCENT'],
    balance_usde_1_day_chg=df_for_ccy_header['BALANCE_DAY_CHANGE'],
    estimated_revenue_1_day_chg=df_for_ccy_header['ESTIMATED_REVENUE_DAY_CHANGE'],

    ccy_name=df_for_ccy_header['CURRENCY_NAME'],
)
source_ccy_header = ColumnDataSource(data_ccy_header)

data4 = dict(
    date=df_for_table4['BALANCE_DT'],

    b_usde=df_for_table4['BALANCE_USDE'],
    e_rev=df_for_table4['ESTIMATED_REVENUE'],
    balance_usde_1_day_chg_pct=df_for_table4['BALANCE_DAY_CHANGE_PERCENT'],
    estimated_revenue_1_day_chg_pct=df_for_table4['ESTIMATED_REVENUE_DAY_CHANGE_PERCENT'],
    balance_usde_1_day_chg=df_for_table4['BALANCE_DAY_CHANGE'],
    estimated_revenue_1_day_chg=df_for_table4['ESTIMATED_REVENUE_DAY_CHANGE'],

    clients=df_for_table4['PROVIDER_NAME'],
)
source4 = ColumnDataSource(data4)

data_clients_header = dict(
    date=df_for_clients_header['BALANCE_DT'],
    pct_of_total=df_for_clients_header['PCT_OF_TOTAL'],

    b_usde=df_for_clients_header['BALANCE_USDE'],
    e_rev=df_for_clients_header['ESTIMATED_REVENUE'],
    balance_usde_1_day_chg_pct=df_for_clients_header['BALANCE_DAY_CHANGE_PERCENT'],
    estimated_revenue_1_day_chg_pct=df_for_clients_header['ESTIMATED_REVENUE_DAY_CHANGE_PERCENT'],
    balance_usde_1_day_chg=df_for_clients_header['BALANCE_DAY_CHANGE'],
    estimated_revenue_1_day_chg=df_for_clients_header['ESTIMATED_REVENUE_DAY_CHANGE'],

    clients=df_for_clients_header['PROVIDER_NAME'],
)
source_clients_header = ColumnDataSource(data_clients_header)


ins_top_5m = dict(
    date=df_for_ins_t5m['BALANCE_DT'],

    b_usde=df_for_ins_t5m['BALANCE_USDE'],
    e_rev=df_for_ins_t5m['ESTIMATED_REVENUE'],
    balance_usde_1_day_chg_pct=df_for_ins_t5m['BALANCE_DAY_CHANGE_PERCENT'],
    estimated_revenue_1_day_chg_pct=df_for_ins_t5m['ESTIMATED_REVENUE_DAY_CHANGE_PERCENT'],
    balance_usde_1_day_chg=df_for_ins_t5m['BALANCE_DAY_CHANGE'],
    estimated_revenue_1_day_chg=df_for_ins_t5m['ESTIMATED_REVENUE_DAY_CHANGE'],
    
    ins=df_for_ins_t5m['INV_INSTITUTION_NAME'],
)
source_ins_top_5m = ColumnDataSource(ins_top_5m)

ins_bottom_5m = dict(
    date=df_for_ins_b5m['BALANCE_DT'],

    b_usde=df_for_ins_b5m['BALANCE_USDE'],
    e_rev=df_for_ins_b5m['ESTIMATED_REVENUE'],
    balance_usde_1_day_chg_pct=df_for_ins_b5m['BALANCE_DAY_CHANGE_PERCENT'],
    estimated_revenue_1_day_chg_pct=df_for_ins_b5m['ESTIMATED_REVENUE_DAY_CHANGE_PERCENT'],
    balance_usde_1_day_chg=df_for_ins_b5m['BALANCE_DAY_CHANGE'],
    estimated_revenue_1_day_chg=df_for_ins_b5m['ESTIMATED_REVENUE_DAY_CHANGE'],
    
    ins=df_for_ins_b5m['INV_INSTITUTION_NAME'],
)
source_ins_bottom_5m = ColumnDataSource(ins_bottom_5m)

df_for_table = mdm_pvt_alt.reset_index()
df_for_dm_header = mdm_pvt_country_current_dt.reset_index()
df_for_rc_header = mdm_pvt_current_dt.reset_index()
df_for_ccy_header = mdm_pvt_currency_current_dt.reset_index()
df_for_clients_header = mdm_pvt_clients_current_dt.reset_index()


source2 = ColumnDataSource(data2)

template1 = """
<div style="color:<%=
    (function colorfromint(){
        if(value > 0){
            return("green")}
        else if (value < 0 ){
            return("red")}
         else if (value == 0 ){
            return("black")}
        else{return("black")}
        }()) %>;
    ">
<%= parseFloat(value*100).toFixed(2) +" %" %></div>
"""

template2 = """
<div style="color:<%=
    (function colorfromint(){
    nfObject = new Intl.NumberFormat('en-US', { style: 'currency', currency: 'USD' })
        if(value > 0){
            return("green")}
        else if (value < 0 ){
            return("red")}
         else if (value == 0 ){
            return("black")}
        else{return("black")}
        }()) %>;
    ">
<%= nfObject.format(value/1000000) +" MM" %></div>
"""

template3 = """
<div style="color:<%=
    (function colorfromint(){
    nfObject = new Intl.NumberFormat('en-US', { style: 'currency', currency: 'USD' })
        if(value > 0){
            return("green")}
        else if (value < 0 ){
            return("red")}
         else if (value == 0 ){
            return("black")}
        else{return("black")}
        }()) %>;
    >
<%= nfObject.format(value)  %></div>
"""

template4 = """
<div style="color:<%=
    (function erf(){
    nfObject = new Intl.NumberFormat('en-US', { style: 'currency', currency: 'USD' })
        }()) %>;
    ">
<%= nfObject.format(value)  %></div>
"""

template5 = """
<div style="color:<%=
    (function erf(){
    nfObject = new Intl.NumberFormat('en-US', { style: 'currency', currency: 'USD' })
        }()) %>;
    ">
<%= nfObject.format(value/1000000) +" MM" %></div>
"""

template6 = """
<div style="color:<%=
    (function colorfromint(){
        if(value > 0){
            return("black")}
        else if (value < 0 ){
            return("black")}
         else if (value == 0 ){
            return("black")}
        else{return("black")}
        }()) %>;
    ">
<%= parseFloat(value*1).toFixed(6)%></div>
"""

template7 = """
<div style="color:<%=
    (function colorfromint(){
        if(value > 0){
            return("black")}
        else if (value < 0 ){
            return("black")}
         else if (value == 0 ){
            return("black")}
        else{return("black")}
        }()) %>;
    ">
<%= parseFloat(value*100).toFixed(2) +"%" %></div>
"""

template8 = """
<div style="color:<%=
    (function ifLessThan(){
        if(value < 0.01){
            return("red")}
        }()) %>;
    ">
<%= parseFloat(value*100).toFixed(4) +"%" %></div>
"""

bring_to_front = """
.content {
    position: absolute;
}
"""

mi_script = """
<script>
if (document.readyState === "complete") {
  $('.example').DataTable();
} else {
  $(document).ready(function () {
    $('.example').DataTable();
  })
}

</script>
"""

table_style = """


.table {
    margin-left: auto;
    margin-right: auto;
    border: none;
    border-collapse: collapse;
    border-spacing: 0;
    color: black;
    font-size: 12px;
    table-layout: fixed;
    width: 100%;
}

.tr, th, td {
    text-align: right;
    vertical-align: middle;
    padding: 0.5em 0.5em !important;
    line-height: normal;
    white-space: normal;
    max-width: none;
    border: none;
}

.tbody {
    display: table-row-group;
    vertical-align: middle;
    border-color: inherit;
}

.tbody tr:nth-child(odd) {
    background: #f5f5f5;
}

.thead {
    border-bottom: 1px solid black;
    vertical-align: bottom;
}

.tr:hover {
    background: lightblue !important;
    cursor: pointer;
}



"""

rgb_formatter_pct = HTMLTemplateFormatter(template=template1)
rgb_formatter_ccy_mm = HTMLTemplateFormatter(template=template2)
rgb_formatter_ccy = HTMLTemplateFormatter(template=template3)
erf = HTMLTemplateFormatter(template=template4)
bf = HTMLTemplateFormatter(template=template5)
bps_formatter = HTMLTemplateFormatter(template=template6)
pct_of_total_formatter = HTMLTemplateFormatter(template=template7)
pct_of_total_formatter_ccy = HTMLTemplateFormatter(template=template7)

columns = [
    TableColumn(field="date", title="Date"),
    TableColumn(field="revcat", title="Category"),

    TableColumn(field="b_usde", title="Balance USDE", formatter=bf),
    
    TableColumn(field="balance_usde_1_day_chg_pct", title="Balance Change", formatter=rgb_formatter_pct),

    TableColumn(field="balance_usde_1_day_chg", title="Balance Change", formatter=rgb_formatter_ccy_mm),


]
columns_hrc = [

    TableColumn(field="revcat", title="Category"),

    TableColumn(field="b_usde", title="Balance USDE", formatter=bf),

    
    TableColumn(field="balance_usde_1_day_chg_pct", title="Balance Change", formatter=rgb_formatter_pct),

    TableColumn(field="balance_usde_1_day_chg", title="Balance Change", formatter=rgb_formatter_ccy_mm),


]

view1 = CDSView(source=source1, filters=[GroupFilter(column_name='date', group=latest_dt_str)])

columns2 = [

    TableColumn(field="country", title="Domicile"),
    TableColumn(field="pct_of_total", title="Pct of Total Balance"),

    TableColumn(field="b_usde", title="Balance USDE", formatter=bf),

    TableColumn(field="balance_usde_1_day_chg_pct", title="Balance Change", formatter=rgb_formatter_pct),

    TableColumn(field="balance_usde_1_day_chg", title="Balance Change", formatter=rgb_formatter_ccy_mm),


]

columns_bcat = [

    TableColumn(field="b_cat", title="Category"),
    TableColumn(field="pct_of_total", title="Pct of Total Balance"),
    TableColumn(field="b_usde", title="Balance USDE", formatter=bf),


    TableColumn(field="balance_usde_1_day_chg_pct", title="Balance Change", formatter=rgb_formatter_pct),

    TableColumn(field="balance_usde_1_day_chg", title="Balance Change", formatter=rgb_formatter_ccy_mm),


]

columns3 = [
    
    TableColumn(field="ccy_name", title="Currency Name"),
    TableColumn(field="pct_of_total", title="Pct of Total Balance"),
    
    TableColumn(field="b_usde", title="Balance USDE", formatter=bf),
    
    TableColumn(field="balance_usde_1_day_chg_pct", title="Balance  Change", formatter=rgb_formatter_pct),
    
    TableColumn(field="balance_usde_1_day_chg", title="Balance  Change", formatter=rgb_formatter_ccy_mm),
    

]

columns4 = [

    TableColumn(field="clients", title="Provider Name"),
    TableColumn(field="pct_of_total", title="Pct of Total Balance"),
    
    TableColumn(field="b_usde", title="Balance USDE", formatter=bf),

    TableColumn(field="balance_usde_1_day_chg_pct", title="Balance  Change", formatter=rgb_formatter_pct),

    TableColumn(field="balance_usde_1_day_chg", title="Balance  Change", formatter=rgb_formatter_ccy_mm),


]

columns5 = [
    
    TableColumn(field="clients", title="Provider Name"),
    
    TableColumn(field="b_usde", title="Balance USDE", formatter=bf),

    TableColumn(field="balance_usde_1_day_chg_pct", title="Balance  Change", formatter=rgb_formatter_pct),

    TableColumn(field="balance_usde_1_day_chg", title="Balance  Change", formatter=rgb_formatter_ccy_mm),


]

columns6 = [

    TableColumn(field="ins", title="Institution Name"),
    
    TableColumn(field="b_usde", title="Balance USDE", formatter=bf),

    
    TableColumn(field="balance_usde_1_day_chg_pct", title="Balance  Change", formatter=rgb_formatter_pct),

    TableColumn(field="balance_usde_1_day_chg", title="Balance  Change", formatter=rgb_formatter_ccy_mm),


]

columns_01 = [
    TableColumn(field="date", title="Date"),
    TableColumn(field="pname", title="Provider Name"),

    TableColumn(field="bcat", title="Category"),
    TableColumn(field="b_usde", title="Balance USDE", formatter=bf),
    
    TableColumn(field="balance_usde_1_day_chg_pct", title="Balance Change", formatter=rgb_formatter_pct),

    TableColumn(field="balance_usde_1_day_chg", title="Balance Change", formatter=rgb_formatter_ccy_mm),

]

columns_02 = [
    TableColumn(field="date", title="Date"),

    TableColumn(field="ins", title="Institution Name"),
    TableColumn(field="bcat", title="Category"),

    TableColumn(field="b_usde", title="Balance USDE", formatter=bf),
    
    TableColumn(field="balance_usde_1_day_chg_pct", title="Balance Change", formatter=rgb_formatter_pct),

    TableColumn(field="balance_usde_1_day_chg", title="Balance Change", formatter=rgb_formatter_ccy_mm),

]

columns_03 = [
    TableColumn(field="date", title="Date"),

    TableColumn(field="whitelabel", title="White Label Name"),
    TableColumn(field="bcat", title="Category"),

    TableColumn(field="b_usde", title="Balance USDE", formatter=bf),
    
    TableColumn(field="balance_usde_1_day_chg_pct", title="Balance Change", formatter=rgb_formatter_pct),

    TableColumn(field="balance_usde_1_day_chg", title="Balance Change", formatter=rgb_formatter_ccy_mm),

]

view2 = CDSView(source=source2, filters=[GroupFilter(column_name='date', group=latest_dt_str)])

view3 = CDSView(source=source3, filters=[GroupFilter(column_name='date', group=latest_dt_str)])

view4 = CDSView(source=source4, filters=[GroupFilter(column_name='date', group=latest_dt_str)])

data_table = DataTable(source=ColumnDataSource(df_for_table), columns=columns, sizing_mode='stretch_width', height=200,
                       index_position=None, editable=True, reorderable=False)  # update view here

cat_choice = pn.widgets.CheckButtonGroup(name='Category Analytics', options=list(unique_billing_cats),
                                         margin=(2, 20, 2, 2), button_type='primary')
ctc = cat_choice.value

prov_bc_choice = pn.widgets.MultiChoice(name='Category Filter', options=list(unique_billing_cats), margin=(2, 20, 2, 2),
                                        css_classes=['panel-hover-tool2'])
prbc = prov_bc_choice.value




prov_ins_choice = pn.widgets.MultiChoice(name='Performance by Institution', options=list(ins_cats),
                                         margin=(2, 30, 20, 2), css_classes=['panel-hover-tool'])
pri = prov_ins_choice.value

provider_choice = pn.widgets.MultiChoice(name='Performance by Provider', options=list(unique_fps),
                                         margin=(2, 30, 20, 2), css_classes=['panel-hover-tool'])
fpc = provider_choice.value

whitelabel_choice = pn.widgets.MultiChoice(name='White Label Filter', options=list(unique_whitelabels),
                                           margin=(2, 20, 2, 2), css_classes=['panel-hover-tool'])
wlc = whitelabel_choice.value



data_table2 = DataTable(source=source2, columns=columns2, sizing_mode='stretch_width', height=100, index_position=None,
                        editable=True, reorderable=False)  # update view here


@pn.depends(cat_choice)
def category_callback(ctc):
    df_for_table = mdm_pvt_alt.reset_index()

    if cat_choice.value:
        df_for_table = df_for_table[df_for_table.RATE_TYPE.isin(ctc)]

    data1 = dict(
        date=df_for_table['BALANCE_DT'],
        rcat=df_for_table['RATE_TYPE'],

        b_usde=df_for_table['BALANCE_USDE'],
        e_rev=df_for_table['ESTIMATED_REVENUE'],
        balance_usde_1_day_chg_pct=df_for_table['BALANCE_DAY_CHANGE_PERCENT'],
        estimated_revenue_1_day_chg_pct=df_for_table['ESTIMATED_REVENUE_DAY_CHANGE_PERCENT'],
        balance_usde_1_day_chg=df_for_table['BALANCE_DAY_CHANGE'],
        estimated_revenue_1_day_chg=df_for_table['ESTIMATED_REVENUE_DAY_CHANGE'],

        bps=df_for_table['BILLABLE_BPS'],
    )
    source1 = ColumnDataSource(data1)

    data_table = DataTable(source=source1, columns=columns, sizing_mode='stretch_width',
                           height=500, index_position=None, editable=True, reorderable=False)  # update view here


    return data_table


@pn.depends(prov_bc_choice, provider_choice)
def client_rc_callback(prbc, fps):
    df_for_bc_callback = df_provider_bc.reset_index()

    if prov_bc_choice.value:
        df_for_bc_callback = df_for_bc_callback[df_for_bc_callback.RATE_TYPE.isin(prbc)]

    elif provider_choice.value:
        df_for_bc_callback = df_for_bc_callback[df_for_bc_callback.PROVIDER_NAME.isin(fps)]

    data_01 = dict(
        date=df_for_bc_callback['BALANCE_DT'],
        pname=df_for_bc_callback['PROVIDER_NAME'],

        bcat=df_for_bc_callback['RATE_TYPE'],
        b_usde=df_for_bc_callback['BALANCE_USDE'],
        bps=df_for_bc_callback['BILLABLE_BPS'],

        balance_usde_1_day_chg_pct=df_for_bc_callback['BALANCE_DAY_CHANGE_PERCENT'],

        balance_usde_1_day_chg=df_for_bc_callback['BALANCE_DAY_CHANGE'],
        
    )
    source01 = ColumnDataSource(data_01)

    data_table_01 = DataTable(source=source01, columns=columns_01, sizing_mode='stretch_width',
                              height=500, index_position=None, editable=True, reorderable=False,
                              margin=(2, 2, 20, 2))  # update view here


    return data_table_01


#########################
@pn.depends(prov_ins_choice)
def client_inst_callback(pri):
    
    df_inst_callback = df_ins_bc.reset_index()


    print("pre evaluating callback function for %s" % str(prov_ins_choice.value))
    if prov_ins_choice.value:
        print("evaluating callback function")

        df_for_inst_callback = df_inst_callback[df_inst_callback.INV_INSTITUTION_NAME.isin(prov_ins_choice.value)]

        ins_for_graph = df_for_inst_callback
        ins_for_graph = ins_for_graph.drop(['RATE_TYPE'], axis=1)
        ins_for_graph = ins_for_graph.drop(['INV_INSTITUTION_NAME'], axis=1)
        ins_for_graph.sort_values('BALANCE_DT', inplace=True, ascending=True)

        ins_for_pivot = pd.pivot_table(ins_for_graph, values=['BALANCE_USDE', 'ESTIMATED_REVENUE'],
                                       index=['BALANCE_DT'], aggfunc=np.sum)

        ins_plot = ColumnDataSource(ins_for_pivot)

        p01 = figure(sizing_mode='stretch_width', plot_height=500, x_range=list(ins_for_graph.BALANCE_DT.unique()),
                     toolbar_location="left")

        p01.line(x='BALANCE_DT', y='BALANCE_USDE', color="navy", source=ins_plot, line_dash='solid', line_width=1.5)
        p01.line(x='BALANCE_DT', y='ESTIMATED_REVENUE', color="orange", source=ins_plot, line_dash='dashdot',
                 line_width=1.5, y_range_name="ESTIMATED_REVENUE")

        p01.extra_y_ranges = {"ESTIMATED_REVENUE": Range1d(150000, 350000)}



        p01.title.text = 'Institution Plot'

        p01.yaxis.axis_label = 'Balance USDE (Solid)'

        p01.xaxis.major_label_orientation = math.pi / 2
        p01.yaxis[0].formatter = NumeralTickFormatter(format="$ 0.00 a")
        
        p01.toolbar.autohide = True

        hover = HoverTool()
        hover.tooltips = [
            ('BALANCE_DATE', "@BALANCE_DT"),
            ('BALANCE_USDE', "@BALANCE_USDE{$ 0.00 a}"),
            ('ESTIMATED_REVENUE', "@ESTIMATED_REVENUE{$ 0.00 a}")
        ]

        p01.add_tools(hover)

        return p01


################################
##

@pn.depends(provider_choice)
def client_prov_plot_callback(pri):

    df_prov_plot_callback = df_provider_bc.reset_index()


    print("pre evaluating prov callback function for %s" % str(provider_choice.value))
    if provider_choice.value:
        print("evaluating prov callback function")

        df_for_prov_plot_callback = df_prov_plot_callback[
            df_prov_plot_callback.PROVIDER_NAME.isin(provider_choice.value)]

        prov_for_graph = df_for_prov_plot_callback
        prov_for_graph = prov_for_graph.drop(['RATE_TYPE'], axis=1)
        prov_for_graph = prov_for_graph.drop(['PROVIDER_NAME'], axis=1)
        prov_for_graph.sort_values('BALANCE_DT', inplace=True, ascending=True)

        prov_for_pivot = pd.pivot_table(prov_for_graph, values=['BALANCE_USDE', 'ESTIMATED_REVENUE'],
                                        index=['BALANCE_DT'], aggfunc=np.sum)

        prov_plot = ColumnDataSource(prov_for_pivot)

        p02 = figure(sizing_mode='stretch_width', plot_height=500, x_range=list(prov_for_graph.BALANCE_DT.unique()),
                     toolbar_location="left")

        p02.line(x='BALANCE_DT', y='BALANCE_USDE', color="navy", source=prov_plot, line_dash='solid', line_width=1.5)
        p02.line(x='BALANCE_DT', y='ESTIMATED_REVENUE', color="orange", source=prov_plot, line_dash='dashdot',
                 line_width=1.5, y_range_name="ESTIMATED_REVENUE")

        p02.extra_y_ranges = {"ESTIMATED_REVENUE": Range1d(150000, 350000)}



        p02.title.text = 'Provider Plot'

        p02.yaxis.axis_label = 'Balance USDE (Solid)'

        p02.xaxis.major_label_orientation = math.pi / 2
        p02.yaxis[0].formatter = NumeralTickFormatter(format="$ 0.00 a")
        
        p02.toolbar.autohide = True

        hover = HoverTool()
        hover.tooltips = [
            ('BALANCE_DATE', "@BALANCE_DT"),
            ('BALANCE_USDE', "@BALANCE_USDE{$ 0.00 a}"),
            ('ESTIMATED_REVENUE', "@ESTIMATED_REVENUE{$ 0.00 a}")
        ]

        p02.add_tools(hover)

        return p02


##


##

@pn.depends(whitelabel_choice)
def client_cb_plot_callback(wlc):

    df_whitelabel_plot_callback = df_whitelabel.reset_index()


    print("pre evaluating whitelabel callback function for %s" % str(whitelabel_choice.value))
    if whitelabel_choice.value:
        print("evaluating whitelabel callback function")

        df_for_whitelabel_plot_callback = df_whitelabel_plot_callback[
            df_whitelabel_plot_callback.CB_DESCRIPTION.isin(whitelabel_choice.value)]

        whitelabel_for_graph = df_for_whitelabel_plot_callback
        whitelabel_for_graph = whitelabel_for_graph.drop(['RATE_TYPE'], axis=1)
        whitelabel_for_graph = whitelabel_for_graph.drop(['CB_DESCRIPTION'], axis=1)
        whitelabel_for_graph.sort_values('BALANCE_DT', inplace=True, ascending=True)

        whitelabel_for_pivot = pd.pivot_table(whitelabel_for_graph, values=['BALANCE_USDE', 'ESTIMATED_REVENUE'],
                                              index=['BALANCE_DT'], aggfunc=np.sum)

        whitelabel_plot = ColumnDataSource(whitelabel_for_pivot)

        p03 = figure(sizing_mode='stretch_width', plot_height=500,
                     x_range=list(whitelabel_for_graph.BALANCE_DT.unique()), toolbar_location="left")

        p03.line(x='BALANCE_DT', y='BALANCE_USDE', color="navy", source=whitelabel_plot, line_dash='solid',
                 line_width=1.5)
        p03.line(x='BALANCE_DT', y='ESTIMATED_REVENUE', color="orange", source=whitelabel_plot, line_dash='dashdot',
                 line_width=1.5, y_range_name="ESTIMATED_REVENUE")

        p03.extra_y_ranges = {"ESTIMATED_REVENUE": Range1d(150000, 350000)}



        p03.title.text = 'Provider Plot'

        p03.yaxis.axis_label = 'Balance USDE (Solid)'

        p03.xaxis.major_label_orientation = math.pi / 2
        p03.yaxis[0].formatter = NumeralTickFormatter(format="$ 0.00 a")


        p03.toolbar.autohide = True

        hover = HoverTool()
        hover.tooltips = [
            ('BALANCE_DATE', "@BALANCE_DT"),
            ('BALANCE_USDE', "@BALANCE_USDE{$ 0.00 a}"),
            ('ESTIMATED_REVENUE', "@ESTIMATED_REVENUE{$ 0.00 a}")
        ]

        p03.add_tools(hover)

        return p03


##
################################
@pn.depends(prov_ins_choice, prov_bc_choice)
def client_ins_callback(pri, prbc):

    df_for_ins_callback = df_ins_bc.reset_index()


    if prov_ins_choice.value:
        df_for_ins_callback = df_for_ins_callback[df_for_ins_callback.INV_INSTITUTION_NAME.isin(pri)]
    elif prov_bc_choice.value:
        df_for_ins_callback = df_for_ins_callback[df_for_ins_callback.RATE_TYPE.isin(prbc)]

    data_02 = dict(
        date=df_for_ins_callback['BALANCE_DT'],

        ins=df_for_ins_callback['INV_INSTITUTION_NAME'],
        bcat=df_for_ins_callback['RATE_TYPE'],
        b_usde=df_for_ins_callback['BALANCE_USDE'],
        bps=df_for_ins_callback['BILLABLE_BPS'],

        balance_usde_1_day_chg_pct=df_for_ins_callback['BALANCE_DAY_CHANGE_PERCENT'],

        balance_usde_1_day_chg=df_for_ins_callback['BALANCE_DAY_CHANGE'],

    )
    source02 = ColumnDataSource(data_02)

    data_table_02 = DataTable(source=source02, columns=columns_02, sizing_mode='stretch_width',
                              height=500, index_position=None, editable=True, reorderable=False,
                              margin=(2, 2, 20, 2))  # update view here


    return data_table_02


@pn.depends(prov_bc_choice, whitelabel_choice)
def whitelabel_callback(prbc, wlc):
    df_whitelabel_callback = df_whitelabel.reset_index()

    if prov_bc_choice.value:
        df_whitelabel_callback = df_whitelabel_callback[df_whitelabel_callback.RATE_TYPE.isin(prbc)]

    elif whitelabel_choice.value:
        df_whitelabel_callback = df_whitelabel_callback[df_whitelabel_callback.CB_DESCRIPTION.isin(wlc)]

    data_03 = dict(
        date=df_whitelabel_callback['BALANCE_DT'],
        whitelabel=df_whitelabel_callback['CB_DESCRIPTION'],

        bcat=df_whitelabel_callback['RATE_TYPE'],
        b_usde=df_whitelabel_callback['BALANCE_USDE'],
        bps=df_whitelabel_callback['BILLABLE_BPS'],

        balance_usde_1_day_chg_pct=df_whitelabel_callback['BALANCE_DAY_CHANGE_PERCENT'],

        balance_usde_1_day_chg=df_whitelabel_callback['BALANCE_DAY_CHANGE'],

    )
    source03 = ColumnDataSource(data_03)

    data_table_03 = DataTable(source=source03, columns=columns_03, sizing_mode='stretch_width',
                              height=500, index_position=None, editable=True, reorderable=False,
                              margin=20)  # update view here


    return data_table_03



dm_header = DataTable(source=ColumnDataSource(data_country_header), columns=columns2, sizing_mode='stretch_width',
                      height=115, index_position=None, editable=True,
                      reorderable=False)  # .format({'selector': 'tr:last-child', 'props' :[('font-weight', 'bold'),('border-bottom', '1px double black')]}) #, view=view2) # update view here
bcat_header = DataTable(source=ColumnDataSource(data_billing_header), columns=columns_bcat, sizing_mode='stretch_width',
                        height=225, index_position=None, editable=True, reorderable=False)
ccy_header = DataTable(source=ColumnDataSource(data_ccy_header), columns=columns3, sizing_mode='stretch_width',
                       height=260, index_position=None, editable=True, reorderable=False)  # , view=view3)
clients_header = DataTable(source=ColumnDataSource(data_clients_header), columns=columns4, sizing_mode='stretch_width',
                           height=225, index_position=None, editable=True, reorderable=False)  # , view=view4)


ins_t5m = DataTable(source=ColumnDataSource(ins_top_5m), columns=columns6, sizing_mode='stretch_width', height=200,
                    index_position=None, editable=True, reorderable=False)  # , view=view4)
ins_b5m = DataTable(source=ColumnDataSource(ins_bottom_5m), columns=columns6, sizing_mode='stretch_width', height=200,
                    index_position=None, editable=True, reorderable=False)  # , view=view4)



chg_slicer_header = pn.widgets.RadioButtonGroup(name='Category Analytics',
                                                options=['1 Day', '7 Day', '30 Day', '60 Day', '90 Day'],
                                                margin=(0, 20, 0, 0), button_type='primary')
csh = chg_slicer_header.value

####### for cube

target = ColumnDataSource(data=dict(row_indicies=[], labels=[]))

formatter = StringFormatter(font_style='bold')

cube_columns = columns_hrc



grouping = [
    GroupingInfo(getter='revcat', aggregators=[SumAggregator(field_='b_usde')], collapsed=False)
]

rc_cube = DataCube(source=source1, columns=cube_columns, grouping=grouping, target=target, view=view1)

mdm_pvt_alt.groupby(level='RATE_TYPE')['BALANCE_USDE'].pct_change(periods=1)



show(column(p, p3, data_table2, data_table))

filtered_total_bal_usde = (df_master_for_panel.loc[
                               df_master_for_panel['BALANCE_DT'] == latest_dt_str, 'BALANCE_USDE'].sum()) / 1000000000
filtered_total_bal_chg = ((filtered_total_bal_usde - (df_master_for_panel.loc[df_master_for_panel[
                                                                                  'BALANCE_DT'] == latest_dt_m1_str, 'BALANCE_USDE'].sum() / 1000000000)) / (
                                      df_master_for_panel.loc[df_master_for_panel[
                                                                  'BALANCE_DT'] == latest_dt_m1_str, 'BALANCE_USDE'].sum() / 1000000000)) * 100
filtered_total_bal_usde_str = str('{filtered_total_bal_usde:,.2f}').format(
    filtered_total_bal_usde=filtered_total_bal_usde)
filtered_total_bal_chg_str = str('{filtered_total_bal_chg:,.2f}').format(filtered_total_bal_chg=filtered_total_bal_chg)
filtered_total_bal_chg_usd_str = str('{filtered_total_bal_chg_usd:,.2f}').format(
    filtered_total_bal_chg_usd=filtered_total_bal_chg_usd)



total_portal = df_portal.loc[df_portal['BALANCE_DT'] == latest_dt_str, 'BALANCE_USDE'].sum() / 1000000000
total_portal_chg = ((total_portal - (
            df_portal.loc[df_portal['BALANCE_DT'] == latest_dt_m1_str, 'BALANCE_USDE'].sum() / 1000000000)) / (
                                df_portal.loc[df_portal[
                                                  'BALANCE_DT'] == latest_dt_m1_str, 'BALANCE_USDE'].sum() / 1000000000)) * 100

total_sweep = df_sweep.loc[df_sweep['BALANCE_DT'] == latest_dt_str, 'BALANCE_USDE'].sum() / 1000000000
total_sweep_chg = ((total_sweep - (
            df_sweep.loc[df_sweep['BALANCE_DT'] == latest_dt_m1_str, 'BALANCE_USDE'].sum() / 1000000000)) / (
                               df_sweep.loc[df_sweep[
                                                'BALANCE_DT'] == latest_dt_m1_str, 'BALANCE_USDE'].sum() / 1000000000)) * 100


total_whitelabel = df_for_stkbar2.loc[df_for_stkbar2['BALANCE_DT'] == latest_dt_str, 'BALANCE_USDE'].sum() / 1000000000
total_whitelabel_chg = ((total_whitelabel - (df_for_stkbar2.loc[df_for_stkbar2[
                                                                    'BALANCE_DT'] == latest_dt_m1_str, 'BALANCE_USDE'].sum() / 1000000000)) / (
                                    df_for_stkbar2.loc[df_for_stkbar2[
                                                           'BALANCE_DT'] == latest_dt_m1_str, 'BALANCE_USDE'].sum() / 1000000000)) * 100


pvt_mtd = pd.pivot_table(df_master_for_panel, values=['ESTIMATED_REVENUE'], index=['BALANCE_DT'],
                         aggfunc=np.sum).reset_index()

pvt_mtd['BALANCE_DT'] = pd.to_datetime(pvt_mtd['BALANCE_DT'])
pvt_mtd.set_index('BALANCE_DT')



current_month = datetime.now().month
current_mtd_sum = pvt_mtd.loc[pvt_mtd[
                                  'BALANCE_DT'].dt.month == current_month, 'ESTIMATED_REVENUE'].sum() / 1000000  ##this is the mtd estimated revenue calculation


def prior_month():
    pm = datetime.now().month - 1
    if pm == 0:
        return 12
    else:
        return pm


prior_month_revenue = pvt_mtd.loc[pvt_mtd[
                                      'BALANCE_DT'].dt.month == prior_month(), 'ESTIMATED_REVENUE'].sum() / 1000000  ##this is the prior month's estimated revenue calculation




def color_choice_total():
    if (filtered_total_bal_chg > 0):
        return ('green')
    elif (filtered_total_bal_chg < 0):
        return ('red')
    else:
        return ('black')


def color_choice_er():
    if (filtered_total_er_chg > 0):
        return ('green')
    elif (filtered_total_er_chg < 0):
        return ('red')
    else:
        return ('black')


def color_choice_portal():
    if (total_portal_chg > 0):
        return ('green')
    elif (total_portal_chg < 0):
        return ('red')
    else:
        return ('black')


def color_choice_sweep():
    if (total_sweep_chg > 0):
        return ('green')
    elif (total_sweep_chg < 0):
        return ('red')
    else:
        return ('black')


def color_choice_whitelabel():
    if (total_whitelabel_chg > 0):
        return ('green')
    elif (total_whitelabel_chg < 0):
        return ('red')
    else:
        return ('black')


def color_choice_mtd_er():
    if (total_sweep_chg > 0):
        return ('green')
    elif (total_sweep_chg < 0):
        return ('red')
    else:
        return ('black')


def border_col_change(value):
    if value < 0:
        return '2px solid red'
    elif value > 0:
        return '2px solid green'
    else:
        return '2px solid black'




css = '''
.panel-widget-box {
  background: #ffffff;
  border-radius: 5px;
  border: 1px black solid;
}
.panel-hover-tool {
  z-index: 10;
  padding-bottom: 15px;
}
.panel-hover-tool2 {
  z-index: 11;
}
.table-style {
width: 120%;
}
.table-style td {
width: 120%;
}

table.panel-df {
        margin-left: auto;
        margin-right: auto;
        color: black;
        border: none;
        border-collapse: collapse;
        border-spacing: 0;
        font-size: 12px;
        table-layout: auto;
        width: 120%;
    }

    .panel-df tr, .panel-df th, .panel-df td {
        text-align: right;
        vertical-align: middle;
        padding: 0.5em 0.5em !important;
        line-height: normal;
        white-space: normal;
        max-width: none;
        border: none;
        width: 15%;
    }

    .panel-df tbody {
        display: table-row-group;
        vertical-align: middle;
        border-color: inherit;
    }

    .panel-df tbody tr:nth-child(odd) {
        background: #f5f5f5;
    }

    .panel-df thead {
        border-bottom: 1px solid black;
        vertical-align: bottom;
    }

    .panel-df tr:hover {
        background: lightblue !important;
        cursor: pointer;
    }




'''

pn.extension(raw_css=[css])

css2 = '''

table.panel-df {
        margin-left: auto;
        margin-right: auto;
        color: black;
        border: none;
        border-collapse: collapse;
        border-spacing: 0;
        font-size: 12px;
        table-layout: fixed;
        width: 100%;
    }

    .panel-df tr, .panel-df th, .panel-df td {
        text-align: right;
        vertical-align: middle;
        padding: 0.5em 0.5em !important;
        line-height: normal;
        white-space: normal;
        max-width: none;
        border: none;
    }

    .panel-df tbody {
        display: table-row-group;
        vertical-align: middle;
        border-color: inherit;
    }

    .panel-df tbody tr:nth-child(odd) {
        background: #f5f5f5;
    }

    .panel-df thead {
        border-bottom: 1px solid black;
        vertical-align: bottom;
    }

    .panel-df tr:hover {
        background: lightblue !important;
        cursor: pointer;
    }

'''

pn.extension(raw_css=[css2])

html_pane = pn.pane.HTML("""


<h2>Total Balance</h2>


<code>

<h1>${filtered_total_bal_usde:,.2f}B | {filtered_total_bal_chg:.2f}%</h1>

""".format(filtered_total_bal_usde=filtered_total_bal_usde, filtered_total_bal_chg=filtered_total_bal_chg),
                         style={'background-color': 'e1e7e3', 'border': border_col_change(filtered_total_bal_chg),
                                'border-radius': '5px', 'font-size': '12px', 'text-align': 'center', 'width': '300px',
                                'color': color_choice_total()}, width_policy='max')




html_pane2 = pn.pane.HTML("""
<h2>Estimated Revenue</h2>

<code>

<h1>${filtered_total_er:,.2f}B | {filtered_total_er_chg:.2f}%</h1>
""".format(filtered_total_er=filtered_total_er, filtered_total_er_chg=filtered_total_er_chg),
                          style={'background-color': 'e1e7e3', 'border': border_col_change(filtered_total_er_chg),
                                 'border-radius': '5px', 'font-size': '12px', 'text-align': 'center', 'width': '300px',
                                 'color': color_choice_er()}, width_policy='max')

html_pane3 = pn.pane.HTML("""
<h2>Portal Balance </h2>

<code>

<h1>${total_portal:,.2f}B | {total_portal_chg:.2f}%</h1>


""".format(total_portal=total_portal, total_portal_chg=total_portal_chg),
                          style={'background-color': 'e1e7e3', 'border': border_col_change(total_portal_chg),
                                 'border-radius': '5px', 'font-size': '12px', 'text-align': 'center', 'width': '300px',
                                 'color': color_choice_portal()}, width_policy='max')

html_pane4 = pn.pane.HTML("""
<h2>Sweep Balance</h2>

<code>
   <h1>${total_sweep:,.2f}B | {total_sweep_chg:.2f}%</h1>

""".format(total_sweep=total_sweep, total_sweep_chg=total_sweep_chg),
                          style={'background-color': 'e1e7e3', 'border': border_col_change(total_sweep_chg),
                                 'border-radius': '5px', 'font-size': '12px', 'text-align': 'center', 'width': '300px',
                                 'color': color_choice_sweep()})

html_pane5 = pn.pane.HTML("""
<h2>MTD Est. Revenue</h2>

<code>
   <h1>${current_mtd_sum:.2f}M</h1>

""".format(current_mtd_sum=current_mtd_sum), style={'background-color': 'e1e7e3', 'border': '2px solid black',
                                                    'border-radius': '5px', 'font-size': '12px', 'text-align': 'center',
                                                    'width': '300px'})

html_pane6 = pn.pane.HTML("""
<h2>Prior Month Est. Revenue</h2>

<code>
   <h1>${prior_month_revenue:.2f}M</h1>

""".format(prior_month_revenue=prior_month_revenue), style={'background-color': 'e1e7e3', 'border': '2px solid black',
                                                            'border-radius': '5px', 'font-size': '12px',
                                                            'text-align': 'center', 'width': '300px'})



html_pane8 = pn.pane.HTML("""
<h2>White Label Balance</h2>

<code>
   <h1>${total_whitelabel:,.2f}B | {total_whitelabel_chg:.2f}%</h1>

""".format(total_whitelabel=total_whitelabel, total_whitelabel_chg=total_whitelabel_chg),
                          style={'background-color': 'e1e7e3', 'border': border_col_change(total_whitelabel_chg),
                                 'border-radius': '5px', 'font-size': '12px', 'text-align': 'center', 'width': '300px',
                                 'color': color_choice_whitelabel()})

html_pane9 = pn.pane.HTML("""
<h2>Net Activity</h2>

<code>
   <h1>${net_activity:,.2f}M</h1>

""".format(net_activity=net_activity), style={'background-color': 'e1e7e3', 'border': '2px solid black',
                                              'border-radius': '5px', 'font-size': '12px', 'text-align': 'center',
                                              'width': '175px'})

html_pane10 = pn.pane.HTML("""
<h2>Total Activity</h2>

<code>
   <h1>${total_activity:,.2f}B</h1>

""".format(total_activity=total_activity), style={'background-color': 'e1e7e3', 'border': '2px solid black',
                                                  'border-radius': '5px', 'font-size': '12px', 'text-align': 'center',
                                                  'width': '175px'})

html_pane11 = pn.pane.HTML("""
<h2>Projected Balance</h2>

<code>
   <h1>${projected_balance:,.2f}B | {projected_balance_chg:.2f}%</h1>

""".format(projected_balance=projected_balance, projected_balance_chg=projected_balance_chg),
                           style={'background-color': 'e1e7e3', 'border': '2px solid black',
                                  'border-radius': '5px', 'font-size': '12px', 'text-align': 'center',
                                  'width': '250px'})

html_paneh = pn.pane.HTML("""

<figure>
<img src="https://www.fundconnectportal.com/assets/logo-light.svg" class="logo_light">
<h3>Volume Dashboard</h3>
<p>All values are expressed in USD. Data is as of : {latest_dt_str}. Activity data is refreshed every 40 minutes and reconciled at end of day. A superscript "WL" indicates a White Label client.</p>
</figure>



""".format(latest_dt_str=latest_dt_str),
                          style={'background-color': '#0a2f5d', 'color': 'white', 'border': '2px solid black',
                                 'border-radius': '5px', 'font-size': '16px', 'height': '265px'}, width_policy='max')

html_paneh2 = pn.pane.HTML("""

<figure>
<img src="https://www.fundconnectportal.com/assets/logo-light.svg" class="logo_light">
<h3>Volume Dashboard</h3>
<p>Data is as of : {latest_dt_str}. Click on drop down menus to slice data as desired. To sort by column value, click on column name to toggle ascending/ descending. Category and Instituion Selection menus are prioritized over Provider Selection.</p>
</figure>



""".format(latest_dt_str=latest_dt_str),
                           style={'background-color': '#0a2f5d', 'color': 'white', 'border': '2px solid black',
                                  'border-radius': '5px', 'font-size': '16px', 'height': '285px'}, width_policy='max')

html_paneh3 = pn.pane.HTML("""

<figure>
<img src="https://www.fundconnectportal.com/assets/logo-light.svg" class="logo_light">
<h3>Volume Dashboard</h3>
<p>Data is as of : {latest_dt_str}. Below, please find the appendix including assorted useful tables. </p>
</figure>



""".format(latest_dt_str=latest_dt_str),
                           style={'background-color': '#0a2f5d', 'color': 'white', 'border': '2px solid black',
                                  'border-radius': '5px', 'font-size': '16px', 'height': '265px'}, width_policy='max')

html_paneh4 = pn.pane.HTML("""

<figure>
<img src="https://www.fundconnectportal.com/assets/logo-light.svg" class="logo_light">
<h3>Volume Dashboard</h3>
<p>Data is as of : {latest_dt_str}. Below, please find view White Label performance. </p>
</figure>



""".format(latest_dt_str=latest_dt_str),
                           style={'background-color': '#0a2f5d', 'color': 'white', 'border': '2px solid black',
                                  'border-radius': '5px', 'font-size': '16px', 'height': '265px'}, width_policy='max')

html_panef = pn.pane.HTML("""

<figure>
<img src="https://www.fundconnectportal.com/assets/logo-light.svg" class="logo_light">
<h3>Volume Dashboard</h3>
</figure>


""".format(latest_dt_str=latest_dt_str),
                          style={'background-color': '#0a2f5d', 'color': 'white', 'border': '2px solid black',
                                 'border-radius': '5px', 'font-size': '16px', 'height': '200px'}, width_policy='max')




# titles
PRC = "Performance by Report Category"
PD = "Performance by Domicile"
PBC = "Performance by Business Line"
PCCY = "Performance by Currency"
PC = "Performance by Fund Provider"
spc = " "
g2pbc = "Provider Performance by Business Line"
g2pbi = "Provider Performance by Institution"

top5text = "Top 5 Gains"
b5text = "Top 5 Losses"
#


provider_col = pn.Column(provider_choice, client_rc_callback, margin=(5, 5, 5, 5))
ins_col = pn.Column(prov_ins_choice, client_ins_callback, margin=(5, 5, 5, 5))




gspec = pn.GridSpec(sizing_mode='stretch_both')
gspec[0, :4] = pn.Row(html_paneh, margin=5, max_height=175)
gspec[1, :4] = pn.Row(html_pane, html_pane3, html_pane4, html_pane8, html_pane9, html_pane10, html_pane11,
                      margin=(5, 5, 5, 5), max_height=120)
gspec[2, 0:2] = pn.Row(pn.Column(PBC, billing_cat_multiindex, p3), margin=5, max_width=1200, css_classes=['panel-df'])
gspec[2, 2:4] = pn.Row(pn.Column(function_for_plot, function_for_plot_balance), margin=5)
gspec[3, 0:2] = pn.Row(top10institutions, sizing_mode='stretch_width', margin=5, css_classes=['panel-df'])
gspec[3, 2:4] = pn.Row(top10providers, margin=5, css_classes=['panel-df'])
gspec[4, :4] = pn.Row(html_panef, max_height=100, margin=(75, 5, 5, 5))

gspec2 = pn.GridSpec(sizing_mode='stretch_both')
gspec2[0, :4] = pn.Row(html_paneh2, margin=5, max_height=175)

gspec2[1, :4] = pn.Row(prov_bc_choice, sizing_mode='stretch_width', max_height=75, margin=5)
gspec2[2, 0:2] = pn.Row(provider_col, sizing_mode='stretch_width', margin=(5, 5, 25, 5))
gspec2[2, 2:4] = pn.Row(ins_col, sizing_mode='stretch_width', margin=(5, 5, 25, 5))
gspec2[3, 0:2] = pn.Row(client_prov_plot_callback, margin=5)
gspec2[3, 2:4] = pn.Row(client_inst_callback, margin=5)
gspec2[4, :4] = pn.Row(html_panef, max_height=100, margin=5)

gspec3 = pn.GridSpec(sizing_mode='stretch_both')
gspec3[0, :4] = pn.Row(html_paneh3, margin=5, max_height=175)
gspec3[1, 0:2] = pn.Row(pn.Column(top5providers, bot5providers), sizing_mode='stretch_width', margin=5,
                        css_classes=['panel-df'])
gspec3[1, 2:3] = pn.Row(provider_netActivity_pvt_head, margin=5, css_classes=['panel-df'])
gspec3[1, 3:4] = pn.Row(provider_netActivity_pvt_tail, margin=5, css_classes=['panel-df'])
gspec3[2, :4] = pn.Row(html_panef, max_height=100, margin=5)

gspec4 = pn.GridSpec(sizing_mode='stretch_both')
gspec4[0, :4] = pn.Row(html_paneh4, margin=5, max_height=175)
gspec4[1, :4] = pn.Row(prov_bc_choice, sizing_mode='stretch_width', max_height=75, margin=5)
gspec4[2, 0:2] = pn.Row(pn.Column(whitelabel_choice, whitelabel_callback), sizing_mode='stretch_width',
                        margin=(5, 5, 25, 5))
gspec4[2, 2:4] = pn.Row(pn.Column(p4), sizing_mode='stretch_width', margin=5)
gspec4[3, :4] = pn.Row(pn.Column(client_cb_plot_callback), sizing_mode='stretch_width', margin=(5, 5, 25, 5))

gspec4[4, :4] = pn.Row(html_panef, max_height=100, margin=5)

tabs = pn.Tabs(
    ('Home', gspec),
    ('Providers and Institutions', gspec2),
    ('White Label', gspec4),
    ('Appendix', gspec3)
).servable()



tabs.save(app_home + 'FC_Dashboard.html', resources=INLINE, embed=True)  ## how to save as an html file

parser = argparse.ArgumentParser()

parser.add_argument('-e', '--email', action='store_true',
                    help="send email")

args = parser.parse_args()

if args.email:
    me = "fundconnect@globallink.com"  # testing set
    to = ""
    body = ("Attached is the Fund Connect Performance Dashboard as of " + latest_dt.strftime("%m-%d-%y") + ":" + "\n"
                                                                                                                 "$" + filtered_total_bal_usde_str + "B is the current balance and this is a change of " + filtered_total_bal_chg_str + "%" + " and $" + filtered_total_bal_chg_usd_str + "B." + "\n" + "\n"
                                                                                                                                                                                                                                                                                                        "All values are expressed in USD. (Please open with Chrome for optimal performance.) \n" +
            "Web: http://btreves01-vdi.pc.ny2.eexchange.com:5006/panel_process")

    rcpt = to.split(",")
    msg = MIMEMultipart()
    msg['Subject'] = "Fund Connect Volume Dashboard " + latest_dt.strftime("%m-%d-%y")
    msg['From'] = me
    msg['To'] = to

    body = MIMEText(body)  # convert the body to a MIME compatible string`
    msg.attach(body)  # attach it to your main message
    part = MIMEBase('application', "octet-stream")
    part.set_payload(open(app_home + 'FC_Dashboard.html', "rb").read())
    encoders.encode_base64(part)
    part.add_header('Content-Disposition', 'attachment; filename=' + 'Volume_Dashboard.html')
    msg.attach(part)
    server = smtplib.SMTP("mail.ny2.eexchange.com")
    server.sendmail(me, rcpt, msg.as_string())
    print("Completed")


