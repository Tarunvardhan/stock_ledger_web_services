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
             
             
             
#UPDATING - GL_ACCOUNT based on the input 
@csrf_exempt
def GL_ACCOUNT_update(request):
    if request.method == 'POST':
        try:
            json_object_list=json.loads(request.body)
            mycursor=connection.cursor()
            u_count=0
            for json_object in json_object_list:
                key_list=[]
                for key in json_object:
                  if json_object[key]=="" or json_object[key]=="NULL":
                      key_list.append(key)
                for key in key_list:
                   json_object.pop(key)
                s_query="SELECT * FROM GL_ACCOUNT WHERE PRIMARY_ACCOUNT= "+str(json_object["PRIMARY_ACCOUNT"])+";"
                result=pd.read_sql(s_query,connection)
                for val in result.values:
                    count=0
                    l_dict={}
                    for col in result.columns:
                        l_dict[col]=val[count]
                        count=count+1
                l_dict["CREATE_DATETIME"]=str(l_dict["CREATE_DATETIME"])
                u_dict={}
                if len(l_dict)>0:
                    for col in json_object:
                        if col=="PRIMARY_ACCOUNT" or col=="SET_OF_BOOKS_ID":
                            if Decimal(json_object[col])!=l_dict[col]:
                                 u_dict[col]=json_object[col]
                        if json_object[col]!=l_dict[col]:
                            u_dict[col]=json_object[col]
                    u_query="UPDATE GL_ACCOUNT SET "
                    for col in u_dict:
                        if col=="PRIMARY_ACCOUNT" or col=="SET_OF_BOOKS_ID":
                            u_query=u_query+str(col)+"="+str(u_dict[col])+","
                        else:
                            u_query=u_query+str(col)+"="+"'"+str(u_dict[col])+"'"+","
                    print(u_query)
                    u_query=u_query[:-1]+" WHERE PRIMARY_ACCOUNT="+str(json_object["PRIMARY_ACCOUNT"])+";"
                    print(u_query)
                    mycursor.execute(u_query)
                    if mycursor.rowcount >0:
                        u_count=u_count+1
                return JsonResponse({"status": 200,"message": f"Records updated: {u_count} "})
        except Exception as error:
            return JsonResponse({"status": 500, "message": str(error)})
        finally:
            mycursor.close()
            connection.close()
