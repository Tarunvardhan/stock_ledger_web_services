from django.shortcuts import render

# Create your views here.
import json
import csv
import pandas as pd
from django.db import IntegrityError
#from .models import LOCATION, STG_TRN_DATA,TRN_DATA,PNDG_DLY_ROLLUP,STG_TRN_DATA_DEL_RECORDS,SYSTEM_CONFIG,ERR_TRN_DATA,DAILY_SKU,DAILY_ROLLUP,TRN_DATA_HISTORY,TRN_DATA_REV,CURRENCY,ITEM_LOCATION
from django.http import JsonResponse,HttpResponse,StreamingHttpResponse
from django.core import serializers
from datetime import datetime,date
from django.views.decorators.csrf import csrf_exempt
from django.utils.crypto import get_random_string
from django.shortcuts import render
from django.db.models import Q
import datetime
import decimal
from decimal import Decimal
from decimal import *
from django.core.serializers.python import Serializer
import numpy as np
from django.db import connection
from datetime import date, timedelta

class MySerialiser(Serializer):
    def end_object( self, obj ):
        self._current['id'] = obj._get_pk_val()
        self.objects.append( self._current )


def sample(request):
    if request.method == 'GET':
        tran_seq_no="12311"
        result=pd.read_sql("select * from err_trn_data where tran_seq_no='"+tran_seq_no+"'",connection)
        print(result)
        
        mycursor=connection.cursor()
        mycursor.execute("select * from err_trn_data where tran_seq_no='"+tran_seq_no+"'")
        result1=mycursor.fetchall()
        print(result1)
        print(date.today().replace(day=1))
        last_day_of_prev_month = date.today().replace(day=1) - timedelta(days=1)

        start_day_of_prev_month = date.today().replace(day=1)- timedelta(days=last_day_of_prev_month.day)

        # For printing results
        print("First day of prev month:", start_day_of_prev_month)
        print("Last day of prev month:", last_day_of_prev_month)


#Fetching the data from GL_ACCOUNT based on the input parameters:
@csrf_exempt 
def GL_ACCOUNT_table(request):
    if request.method == 'POST':
        try:
            json_object = json.loads(request.body)
            json_object=json_object[0]
            keys=[]
            mycursor=connection.cursor()
            for key1 in json_object:
                if (len(str(json_object[key1])))==0:
                    json_object[key1]="NULL"
            for key in json_object:
                if json_object[key]=="NULL" or json_object[key]=="":
                    json_object[key]=None
                    keys.append(key)
            for k in keys:
                json_object.pop(k)
            #fetch DECIMAL type columns
            mycursor.execute("desc gl_account")
            d_type=mycursor.fetchall()
            list_type=[]
            for col2 in d_type:
                if "decimal" in col2[1]:
                    if "PRIMARY_ACCOUNT" in col2[0] or "SET_OF_BOOKS_ID" in col2[0]:
                        list_type.append(col2[0])
            #checking the inputs are mutliple or not
            count=0
            for keys_2 in json_object:
                if isinstance(json_object[keys_2], list):
                    count=1
            if count==1:
                for keys1 in json_object:
                    if isinstance(json_object[keys1], list):
                        if len(json_object[keys1])>1:
                            json_object[keys1]=str(tuple(json_object[keys1]))
                    else:
                        json_object[keys1]=("('"+str(json_object[keys1])+"')")
                query="SELECT * FROM GL_ACCOUNT GL WHERE {}".format(' '.join('GL.{} IN ({}) AND'.format(k,str(json_object[k])[1:-1]) for k in json_object))
            else:
                query="SELECT * FROM GL_ACCOUNT GL WHERE {}".format(' '.join('GL.{} LIKE "%{}%" AND'.format(k,json_object[k]) for k in json_object))
            if len(json_object)==0:
                query=query[:-6]+';'
                results55=pd.read_sql(query,connection)
            else:
                query=query[:-4]+';'
                results55=pd.read_sql(query,connection)
            res_list=[]
            rec={}
            for val2 in results55.values:
                count=0
                for col4 in results55.columns:
                    rec[col4]=val2[count]
                    count=count+1
                for col5 in list_type:
                    if col5 in rec:
                        rec[col5]=int(rec[col5])
                res_list.append(rec.copy())
            if len(res_list)==0:
                return JsonResponse({"status": 500, "message": "NO DATA FOUND"})
            else:
                return JsonResponse(res_list, content_type="application/json",safe=False)
        except Exception as error:
            return JsonResponse({"status": 500, "message": str(error)})
        except ValueError:
            return JsonResponse({"status": 500, "message": "error"})
        finally:
             connection.close()
