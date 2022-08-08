import json
import csv
import pandas as pd
from django.db import IntegrityError
#from .models import LOCATION, STG_TRN_DATA,TRN_DATA,PNDG_DLY_ROLLUP,STG_TRN_DATA_DEL_RECORDS,SYSTEM_CONFIG,ERR_TRN_DATA,DAILY_SKU,DAILY_ROLLUP,TRN_DATA_HISTORY,TRN_DATA_REV,CURRENCY,ITEM_LOCATION,ITEM_DTL,HIER1,HIER2,HIER3
from django.http import JsonResponse,HttpResponse,StreamingHttpResponse
from django.core import serializers
from datetime import datetime,date
from django.views.decorators.csrf import csrf_exempt
from django.utils.crypto import get_random_string
from django.shortcuts import render
from django.db.models import Q
import time
import decimal
from decimal import Decimal
from decimal import *
from django.core.serializers.python import Serializer
import numpy as np
from django.db import connection


class MySerialiser(Serializer):
    def end_object( self, obj ):
        self._current['id'] = obj._get_pk_val()
        self.objects.append( self._current )

#count of different indicators in PNDG_DLY_ROLLUP table:        
def count_pndg_dly_rollup(request):
    if request.method == 'GET':
        count1 = pd.read_sql("SELECT COUNT(PROCESS_IND) FROM pndg_dly_rollup WHERE PROCESS_IND='N';",connection)
        count2 = pd.read_sql("SELECT COUNT(PROCESS_IND) FROM pndg_dly_rollup WHERE PROCESS_IND='I';",connection)
        count3 = pd.read_sql("SELECT COUNT(PROCESS_IND) FROM pndg_dly_rollup WHERE PROCESS_IND='E';",connection)
        count4 = pd.read_sql("SELECT COUNT(PROCESS_IND) FROM pndg_dly_rollup WHERE PROCESS_IND='Y';",connection)
        count5 = pd.read_sql("SELECT COUNT(PROCESS_IND) FROM pndg_dly_rollup WHERE PROCESS_IND='W';",connection)
        count6 = pd.read_sql("SELECT COUNT(PROCESS_IND) FROM pndg_dly_rollup WHERE PROCESS_IND='C';",connection)
        count7 = pd.read_sql("SELECT RECORDS_CLEANED FROM stg_trn_data_del_records WHERE DATE=curdate() AND PROCESS='PNDG_DLY_ROLLUP'",connection)
        count1=(count1.values)[0][0]
        count2=(count2.values)[0][0]
        count3=(count3.values)[0][0]
        count4=(count4.values)[0][0]
        count5=(count5.values)[0][0]
        count6=(count6.values)[0][0]
        if len(count7.values)==0:
            count8=(count4+count5+count6)
        else:
            count7=int((count7.values)[0][0])
            count8=(count4+count5+count6+count7)
        return JsonResponse(
            {
                "Ready to process": f"{count1}",
                "In process": f"{count2}",
                "Error records": f"{count3}",
                "Processed records": f"{count8}"
            }
        )

#Fetching the data from DAILY ROLLUP based on the input parameters:   
@csrf_exempt          
def daily_rollup_table(request):
    if request.method == 'POST':
        try:
            json_object = json.loads(request.body)
            json_object=json_object[0]
            keys=[]
            mycursor=connection.cursor()
            for key1 in json_object:
                if isinstance(json_object[key1], list):
                    if (len(json_object[key1]))==0:
                        json_object[key1]="NULL"
            for key in json_object:
                if json_object[key]=="NULL" or json_object[key]=="":
                    json_object[key]=None
                    keys.append(key)
            for k in keys:
                json_object.pop(k)
            #fetch DECIMAL type columns
            mycursor.execute("desc daily_rollup")
            d_type=mycursor.fetchall()
            list_type=[]
            for col2 in d_type:
                if "decimal" in col2[1]:
                    if "LOCATION" in col2[0] or "SET_OF_BOOKS_ID" in col2[0] or "CURR_MONTH" in col2[0] or "CURR_WEEK" in col2[0]:
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
                query="SELECT DR.*,LOC.LOCATION_NAME,TTD.TRN_NAME,DT.HIER1_DESC,CL.HIER2_DESC,SCL.HIER3_DESC FROM daily_rollup DR,location LOC,trn_type_dtl TTD,hier1 DT,hier2 CL,hier3 SCL WHERE LOC.LOCATION=DR.LOCATION AND DR.HIER1=DT.HIER1 AND DR.TRN_TYPE=TTD.TRN_TYPE AND CL.HIER2=DR.HIER2 AND SCL.HIER3=DR.HIER3 AND IFNULL(DR.AREF,0)=IFNULL(TTD.AREF,0) AND {}".format(' '.join('DR.{} IN ({}) AND'.format(k,str(json_object[k])[1:-1]) for k in json_object))
            else:
                query="SELECT DR.*,LOC.LOCATION_NAME,TTD.TRN_NAME,DT.HIER1_DESC,CL.HIER2_DESC,SCL.HIER3_DESC FROM daily_rollup DR,location LOC,trn_type_dtl TTD,hier1 DT,hier2 CL,hier3 SCL WHERE LOC.LOCATION=DR.LOCATION AND DR.TRN_TYPE=TTD.TRN_TYPE AND DR.HIER1=DT.HIER1 AND CL.HIER2=DR.HIER2 AND SCL.HIER3=DR.HIER3 AND IFNULL(DR.AREF,0)=IFNULL(TTD.AREF,0) AND {}".format(' '.join('DR.{} LIKE "%{}%" AND'.format(k,json_object[k]) for k in json_object))
            if len(json_object)==0:
                query=query[:-4]+';'
                results55=pd.read_sql(query,connection)
            else:
                query=query[:-4]+';'
                results55=pd.read_sql(query,connection)
            res_list=[]
            rec={}
            print(query)
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
             #mycursor.close()
            
            
#Fetching the data from DAILY SKU based on the input parameters:  
@csrf_exempt           
def daily_sku_table(request):
    if request.method == 'POST':
        try:
            json_object = json.loads(request.body)
            json_object=json_object[0]
            keys=[]
            mycursor=connection.cursor()
            for key1 in json_object:
                if isinstance(json_object[key1], list):
                    if (len(json_object[key1]))==0:
                        json_object[key1]="NULL"
            for key in json_object:
                if json_object[key]=="NULL" or json_object[key]=="":
                    json_object[key]=None
                    keys.append(key)
            for k in keys:
                json_object.pop(k)
            #fetch DECIMAL type columns
            mycursor.execute("desc daily_sku")
            d_type=mycursor.fetchall()
            list_type=[]
            for col2 in d_type:
                if "decimal" in col2[1]:
                    if "LOCATION" in col2[0] or "SET_OF_BOOKS_ID" in col2[0] or "CURR_MONTH" in col2[0] or "CURR_WEEK" in col2[0]:
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
                query="SELECT DS.*,ITD.ITEM_DESC,LOC.LOCATION_NAME,TTD.TRN_NAME,DT.HIER1_DESC,CL.HIER2_DESC,SCL.HIER3_DESC FROM daily_sku DS,item_dtl ITD,location LOC,trn_type_dtl TTD,hier1 DT,hier2 CL,hier3 SCL WHERE ITD.ITEM=DS.ITEM AND LOC.LOCATION=DS.LOCATION AND DS.HIER1=DT.HIER1 AND DS.TRN_TYPE=TTD.TRN_TYPE AND CL.HIER2=DS.HIER2 AND SCL.HIER3=DS.HIER3 AND IFNULL(DS.AREF,0)=IFNULL(TTD.AREF,0) AND {}".format(' '.join('DS.{} IN ({}) AND'.format(k,str(json_object[k])[1:-1]) for k in json_object))
            else:
                query="SELECT DS.*,ITD.ITEM_DESC,LOC.LOCATION_NAME,TTD.TRN_NAME,DT.HIER1_DESC,CL.HIER2_DESC,SCL.HIER3_DESC FROM daily_sku DS,item_dtl ITD,location LOC,trn_type_dtl TTD,hier1 DT,hier2 CL,hier3 SCL WHERE ITD.ITEM=DS.ITEM AND LOC.LOCATION=DS.LOCATION AND DS.TRN_TYPE=TTD.TRN_TYPE AND DS.HIER1=DT.HIER1 AND CL.HIER2=DS.HIER2 AND SCL.HIER3=DS.HIER3 AND IFNULL(DS.AREF,0)=IFNULL(TTD.AREF,0) AND {}".format(' '.join('DS.{} LIKE "%{}%" AND'.format(k,json_object[k]) for k in json_object))
            if len(json_object)==0:
                query=query[:-4]+';'
                results55=pd.read_sql(query,connection)
            else:
                query=query[:-4]+';'
                results55=pd.read_sql(query,connection)
            res_list=[]
            rec={}
            print(query)
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



#Fetching the data from DAILY REC based on the input parameters:  
@csrf_exempt           
def daily_rec_table(request):
    if request.method == 'POST':
        try:
            json_object = json.loads(request.body)
            json_object=json_object[0]
            keys=[]
            mycursor=connection.cursor()
            for key1 in json_object:
                if isinstance(json_object[key1], list):
                    if (len(json_object[key1]))==0:
                        json_object[key1]="NULL"
            for key in json_object:
                if json_object[key]=="NULL" or json_object[key]=="":
                    json_object[key]=None
                    keys.append(key)
            for k in keys:
                json_object.pop(k)
            #fetch DECIMAL type columns
            mycursor.execute("desc daily_rec")
            d_type=mycursor.fetchall()
            list_type=[]
            for col2 in d_type:
                if "decimal" in col2[1]:
                    if "LOCATION" in col2[0]:
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
                query="SELECT DR.*,D.HIER1_DESC,LOC.LOCATION_NAME,TTD.TRN_NAME FROM daily_rec DR,hier1 D,location LOC,trn_type_dtl TTD WHERE DR.HIER1=D.HIER1 AND DR.LOCATION=LOC.LOCATION AND DR.TRN_TYPE=TTD.TRN_TYPE AND IFNULL(DR.AREF,0)=IFNULL(TTD.AREF,0) AND {}".format(' '.join('DR.{} IN ({}) AND'.format(k,str(json_object[k])[1:-1]) for k in json_object))
            else:
                query="SELECT DR.*,D.HIER1_DESC,LOC.LOCATION_NAME,TTD.TRN_NAME FROM daily_rec DR,hier1 D,location LOC,trn_type_dtl TTD WHERE DR.HIER1=D.HIER1 AND DR.LOCATION=LOC.LOCATION AND DR.TRN_TYPE=TTD.TRN_TYPE AND IFNULL(DR.AREF,0)=IFNULL(TTD.AREF,0) AND {}".format(' '.join('DR.{} LIKE "%{}%" AND'.format(k,json_object[k]) for k in json_object))
            if len(json_object)==0:
                query=query[:-4]+';'
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