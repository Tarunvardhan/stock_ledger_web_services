import json
import csv
from multiprocessing.connection import Connection
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


#Fetching all the column values from ERR_TRN_DATA table:
def err_trn(request):
    if request.method == 'GET':
        try:
            mycursor=connection.cursor()
            #fetch DECIMAL type columns
            mycursor.execute("desc err_trn_data")
            d_type=mycursor.fetchall()
            list_type=[]
            for col in d_type:
                if "decimal" in col[1]:
                    if "LOCATION" in col[0] or "REV_NO" in col[0] or "ERR_SEQ_NO" in col[0]:
                        list_type.append(col[0])
            
            query="SELECT ETD.*,ITD.ITEM_DESC,LOC.LOCATION_NAME FROM ERR_TRN_DATA ETD,ITEM_DTL ITD,LOCATION LOC WHERE ETD.ITEM=ITD.ITEM AND LOC.LOCATION=ETD.LOCATION "
            results=pd.read_sql(query,connection)
            res_list=[]
            for val1 in results.values:
                count=0
                rec={}
                for col in results.columns:
                    rec[col]=val1[count]
                    count=count+1
                #converting LOCATION ,REV_NO AND ERR_SEQ_NO to INTEGER  if DECIMAL
                for col in list_type:
                    if col in rec:
                        rec[col]=int(rec[col])
                #Appending each row to list
                res_list.append(rec)
                
            if len(res_list)==0:
                return JsonResponse({"status": 500, "message":"NO DATA FOUND"})
            else:
                return JsonResponse(res_list,content_type="application/json",safe=False)
        except Exception as error:
            return JsonResponse({"status": 500, "message":str(error)})
        except ValueError:
            return JsonResponse({"status": 500, "message": "error"})



#Deleting the records from ERR_TRN_DATA table and updating in STG_TRN_DATA table:  
@csrf_exempt    
def del_err_trn_data(request):
    if request.method == 'POST':
        try:
            data_list = json.loads(request.body)
            l_counter=0   
            mycursor=connection.cursor()
            for data in data_list: 
                TRAN_SEQ_NO=data['TRAN_SEQ_NO']
                data.pop('TRAN_SEQ_NO')
                data.pop('ERR_MSG')
                data.pop('ERR_SEQ_NO')
                r_list=[]
                for key in data:
                    if data[key]=="" or data[key]=="NULL":
                        r_list.append(key)
                for col in r_list:
                    data.pop(col)
                mycursor.execute("desc err_trn_data")
                type=mycursor.fetchall()
                list_type=[]
                for col in type:
                    if "decimal" in col[1]:
                        list_type.append(col[0])
                if len(data)>0:
                    D_keys=[]
                    mycursor.execute("select TRAN_SEQ_NO from ERR_TRN_DATA WHERE TRAN_SEQ_NO={}".format(TRAN_SEQ_NO))
                    if mycursor.rowcount>0:
                        my_data = pd.read_sql("SELECT * FROM ERR_TRN_DATA WHERE TRAN_SEQ_NO={};".format(TRAN_SEQ_NO),connection)
                        for val in my_data.values:
                            count=0
                            rec={}
                            for col in my_data.columns:
                                rec[col]=val[count]
                                count=count+1                        
                        #DELETING WHEN BOTH DATA ARE SAME 
                        remove=[]
                        rec["TRN_DATE"]=str(rec["TRN_DATE"])
                        for key in data:
                            if key in list_type:
                                if Decimal(data[key])==rec[key]:
                                    remove.append(key)   
                            else:
                                if data[key]==rec[key]:
                                    remove.append(key)  
                        for p in remove: 
                            data.pop(p)  
                        if len(data)>0:
                            d= str(datetime.now()).replace('-',"").replace(':',"").replace(' ',"").replace('.',"")
                            l_counter=l_counter+1
                            unique_id=d+str(l_counter)+'EPS'
                            current_user = request.user
                            
                            #Retrieving the record and inserting into STG_TRN_DATA before deleting
                            rec.pop('ERR_MSG')
                            rec.pop('ERR_SEQ_NO')
                            rec.pop('HIER1')
                            rec.pop('HIER2')
                            rec.pop('HIER3')
                    
                            for key in data:
                                if key in rec:
                                    rec[key]=data[key]
                            #updating New values
                            rec['REV_TRN_NO']=(TRAN_SEQ_NO)
                            rec['PROCESS_IND']='N'
                            rec['TRAN_SEQ_NO']=unique_id
                            rec['CREATE_DATETIME']= (datetime.now())                    
                            rec['CREATE_ID']=str(current_user)
                            rec['REV_NO']=rec['REV_NO']+1
                            for key in data:
                                rec[key]=data[key]
                            for key in rec:
                                if rec[key]=="" or rec[key]=="NULL" or rec[key]==None:
                                    D_keys.append(key)
                            for key in D_keys:
                               rec.pop(key)
                            D_keys.clear()                           
                            #inserting into STG_TRN_DATA
                            cols=",".join(map(str, rec.keys()))                    
                            v_list=[]
                            val=') VALUES('
                            for v in rec.values():
                                if v== None:
                                    val=val+'NULL,'
                                else:
                                    v_list.append(v)
                                    val=val+""
                                    val=val+'%s,'                                
                            val=val[:-1]+')'
                            i_query="INSERT INTO STG_TRN_DATA ("+cols+val
                            mycursor.execute(i_query,v_list)                            
                            #Delete the Record                 
                            mycursor.execute("DELETE FROM ERR_TRN_DATA WHERE TRAN_SEQ_NO='{}'".format(TRAN_SEQ_NO))
                            connection.commit()
                    else:
                        return JsonResponse({"status": 500,"message" :f"{TRAN_SEQ_NO} does not exist"})
            return JsonResponse({"status": 200,"message": f"Records deleted: {l_counter} "})    
        except Exception as error:
            return JsonResponse({"status": 500, "message":str(error)})
        except ValueError:
            return JsonResponse({"status": 500, "message": "error"})
        finally:
            mycursor.close()
            connection.close()


#Fetching the data from ERR_TRN_DATA table based on the input parameters:
@csrf_exempt   
def err_trn_data_table(request):
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
            mycursor.execute("desc err_trn_data")
            d_type=mycursor.fetchall()
            list_type=[]
            for col2 in d_type:
                if "decimal" in col2[1]:
                    if "LOCATION" in col2[0] or "REV_NO" in col2[0] or "ERR_SEQ_NO" in col2[0]:
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
                query="SELECT ETD.*,ITD.ITEM_DESC,LOC.LOCATION_NAME,TTD.TRN_NAME,DT.HIER1_DESC,CL.HIER2_DESC,SCL.HIER3_DESC FROM ERR_TRN_DATA ETD,ITEM_DTL ITD,LOCATION LOC,TRN_TYPE_DTL TTD,HIER1 DT,HIER2 CL,HIER3 SCL WHERE ETD.ITEM=ITD.ITEM AND LOC.LOCATION=ETD.LOCATION AND ETD.HIER1=DT.HIER1 AND ETD.TRN_TYPE=TTD.TRN_TYPE AND CL.HIER2=ETD.HIER2 AND SCL.HIER3=ETD.HIER3 AND IFNULL(ETD.AREF,0)=IFNULL(TTD.AREF,0) AND {}".format(' '.join('ETD.{} IN ({}) AND'.format(k,str(json_object[k])[1:-1]) for k in json_object))
            else:
                query="SELECT ETD.*,ITD.ITEM_DESC,LOC.LOCATION_NAME,TTD.TRN_NAME,DT.HIER1_DESC,CL.HIER2_DESC,SCL.HIER3_DESC FROM ERR_TRN_DATA ETD,ITEM_DTL ITD,LOCATION LOC,TRN_TYPE_DTL TTD,HIER1 DT,HIER2 CL,HIER3 SCL WHERE ETD.ITEM=ITD.ITEM AND LOC.LOCATION=ETD.LOCATION AND ETD.TRN_TYPE=TTD.TRN_TYPE AND ETD.HIER1=DT.HIER1 AND CL.HIER2=ETD.HIER2 AND SCL.HIER3=ETD.HIER3 AND IFNULL(ETD.AREF,0)=IFNULL(TTD.AREF,0) AND {}".format(' '.join('ETD.{} LIKE "%{}%" AND'.format(k,json_object[k]) for k in json_object))
                
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



