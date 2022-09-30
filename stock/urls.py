"""stock URL Configuration

The `urlpatterns` list routes URLs to views. For more information please see:
    https://docs.djangoproject.com/en/4.0/topics/http/urls/
Examples:
Function views
    1. Add an import:  from my_app import views
    2. Add a URL to urlpatterns:  path('', views.home, name='home')
Class-based views
    1. Add an import:  from other_app.views import Home
    2. Add a URL to urlpatterns:  path('', Home.as_view(), name='home')
Including another URLconf
    1. Import the include() function: from django.urls import include, path
    2. Add a URL to urlpatterns:  path('blog/', include('blog.urls'))
"""
from django.contrib import admin
from django.urls import path
from stock_ledger_models.views_err import err_trn,del_err_trn_data,err_trn_data_table
from stock_ledger_models.views_daily import count_pndg_dly_rollup,daily_sku_table,daily_rollup_table,daily_rec_table,Daily_view
from stock_ledger_models.views_stage import count_stg_trn_data,stg_trn,retrieve_stg,retrieve_err_stg,stg_fin
from stock_ledger_models.views_global import cancel_transaction,system_conf,location_valid,currency_valid,item_location_valid,get_cost_item_location,cost_update_stg,lov_item_dtl,system_config_table,fetch_item_location,sub_cost,system_config_creation
from stock_ledger_models.views_tran import count_trn_data,trn_data_table,trn_data_history_table,trn_data_rev_table,trn_data_rev_1_table,trn_type_dtl_table,trn_type_dtl_list
from stock_ledger_models.views import sample,GL_ACCOUNT_table,GL_ACCOUNT_update,GL_ACCOUNT_INSERT,item_valid,currency_gl,primary_gl

urlpatterns = [
    path('admin/', admin.site.urls),
    path('err_trn_data/',err_trn),                                  #Fetching all the column values from ERR_TRN_DATA table.
    path('delete_err_trn/',del_err_trn_data),                       #Deleting the records from ERR_TRN_DATA table and updating in STG_TRN_DATA table.
    path('err_trn_data_tab/',err_trn_data_table),                   #Fetching the data from ERR_TRN_DATA table based on the input parameters.
    path('count_pending/', count_pndg_dly_rollup),                  #count of different indicators in PNDG_DLY_ROLLUP table.
    path('daily_sku/',daily_sku_table),                             #Fetching the data from DAILY SKU based on the input parameters.
    path('daily_rollup/',daily_rollup_table),                       #Fetching the data from DAILY ROLLUP based on the input parameters. 
    path('count_stg_trn/', count_stg_trn_data),                     #count of different indicators in STG_TRN_DATA table.
    path('stg_trn_data/',stg_trn),                                  #Inserting random TRAN_SEQ_NO in the STG_TRN_DATA table.
    path('retrieve_stg_trn_data/',retrieve_stg),                    #Retrieve filtered data STG_TRN_DATA table using input parameters user and date.
    path('count_trn/', count_trn_data),                             #count of different indicators in TRN_DATA table.
    path('trn_data/',trn_data_table),                               #Fetching the data from TRN_DATA based on the input parameters.
    path('trn_data_history/',trn_data_history_table),               #Fetching the data from TRN DATA HISTORY based on the input parameters.
    path('trn_data_rev/',trn_data_rev_table),                       #Transaction reversal(Fetch the record from TRN_DATA_HISTORY table, insert the original data in TRN_DATA_REV, insert the updated record values in STG_TRN_DATA table & insert the QTY*(-1) into record in STG_TRN_DATA table with new TRAN_SEQ_NO)
    path('cancel/',cancel_transaction),                             #Transaction reversal(Fetch the record from TRN_DATA_HISTORY table, insert the QTY*(-1) into record in STG_TRN_DATA table with new TRAN_SEQ_NO)
    path('trn_data_rev_1/',trn_data_rev_1_table),                   #Transaction reversal(Fetch the record from TRN_DATA_HISTORY table, insert the original data in TRN_DATA_REV, insert the updated record values in STG_TRN_DATA table with new TRAN_SEQ_NO & call the another webservice(cancel_transaction))
    path('location_validation/',location_valid),                    #location validation from LOCATION table.
    path('currency_validation/',currency_valid),                    #currency validation from CURRENCY table.
    path('item_location_validation/',item_location_valid),          #item and location validation from ITEM_LOCATION table.
    path('get_cost_item_location_valid/',get_cost_item_location),   #Retrieve UNIT_COST from ITEM_LOCATION with input parameters item and location.
    path('cost_update_stg_trn/',cost_update_stg),                   #Update and Retrieve UNIT_COST from ITEM_LOCATION with input parameters item and location new_cost, also update STG_TRN_DATA.
    path('retrieve_stg_trn_data/',retrieve_stg),                    #Retrieve filtered data STG_TRN_DATA table using input parameters user and date.
    path('sys_conf/',system_conf),                                  #Fetching the data from SYSTEM CONFIG table based on the TRN_TYPE and updated the record with new values.   
    path('lov_item_dtl/',lov_item_dtl),                             # "ITEM","ITEM_DESC","CLASS","DEPT","SUBCLASS" validation from lov_item_dtl . 
    path('system_config_tab/',system_config_table),                 #Fetching the data from SYSTEM_CONFIG based on the input parameters:
    path('gl_account_tab/',GL_ACCOUNT_table),                       #Fetching the data from GL_ACCOUNT based on the input parameters:
    path('item_loc_data/',fetch_item_location),                     #Fetch data from ITEM_LOCATION tables
    path('daily_rec/',daily_rec_table),                             #Fetching the data from DAILY SKU based on the input parameters.
    path('retrieve_err_stg_data/',retrieve_err_stg),                #Retrieve filtered data from ERR_TRN_DATA and STG_TRN_DATA table using input parameters user and date.
    path('GL_ACCOUNT_update/',GL_ACCOUNT_update),                   #UPDATING - GL_ACCOUNT based on the input 
    path('GL_ACCOUNT_create/',GL_ACCOUNT_INSERT),                   #Insert the input data to GL account.
    path('item_validation/',item_valid),
    path('currency_gl/',currency_gl),
    path('Retrieve_stg_fin/',stg_fin),
    path('trn_type_dtl/',trn_type_dtl_table),
    path('trn_type_dtl_list/',trn_type_dtl_list),
    path('Daily_view/',Daily_view),
    path('subledger_cost/',sub_cost),
    path('system_config_creation/',system_config_creation),
    path('primary_gl/',primary_gl),
    path('test/',sample)
]
