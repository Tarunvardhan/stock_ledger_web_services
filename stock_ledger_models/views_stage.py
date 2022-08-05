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


#count of different indicators in STG_TRN_DATA table:
def count_stg_trn_data(request):
    if request.method == 'GET':
        count1 = pd.read_sql("SELECT COUNT(PROCESS_IND) FROM STG_TRN_DATA WHERE PROCESS_IND='N';",connection)
        count2 = pd.read_sql("SELECT COUNT(PROCESS_IND) FROM STG_TRN_DATA WHERE PROCESS_IND='I';",connection)
        count3 = pd.read_sql("SELECT COUNT(PROCESS_IND) FROM STG_TRN_DATA WHERE PROCESS_IND='E';",connection)
        count4 = pd.read_sql("SELECT COUNT(PROCESS_IND) FROM STG_TRN_DATA WHERE PROCESS_IND='Y';",connection)
        count5 = pd.read_sql("SELECT RECORDS_CLEANED FROM STG_TRN_DATA_DEL_RECORDS WHERE DATE=curdate() AND PROCESS='STG_TRN_DATA'",connection)
        count1=(count1.values)[0][0]
        count2=(count2.values)[0][0]
        count3=(count3.values)[0][0]
        count4=(count4.values)[0][0]
        if len(count5.values)==0:
            count6=(count4)
        else:
            count5=int((count5.values)[0][0])
            count6=(count4+count5)
        return JsonResponse(
            {
                "Ready to process": f"{count1}",
                "In process": f"{count2}",
                "Error records": f"{count3}",
                "Processed records": f"{count6}"
            }
        )


#Inserting random TRAN_SEQ_NO in the STG_TRN_DATA table:
@csrf_exempt 
def stg_trn(request):
    if request.method == 'POST':
        try: 
            json_object = json.loads(request.body)
            current_user = request.user
            D_keys=[]
            P_keys=[]
            R_keys=[]
            l_counter=0
            mycursor = connection.cursor()
            for row in json_object:
                for key in row:    
                    if row[key]=="" or row[key]=="NULL" or key=="SR_NO":
                        D_keys.append(key) 
                    if key=="LOC":
                        P_keys.append(key)
                    if key=="LOC_TYPE":
                        R_keys.append(key)
                for key in D_keys:
                    row.pop(key)
                D_keys.clear()
                for key in P_keys:
                    row["LOCATION"]=row.pop(key)
                P_keys.clear()
                for key in R_keys:
                    row["LOCATION_TYPE"]=row.pop(key)
                R_keys.clear()
                l_counter=l_counter+1
                d= str(datetime.now()).replace('-',"").replace(':',"").replace(' ',"").replace('.',"")
                unique_id=d+str(l_counter)+'STG'
                row["TRAN_SEQ_NO"]=unique_id
                row["PROCESS_IND"]='N'
                row["CREATE_DATETIME"]=str(datetime.now())
                row["CREATE_ID"]=str(current_user)
                row["REV_NO"]=1
                mycursor.execute("SELECT TRN_TYPE FROM TRN_TYPE_DTL WHERE TRN_NAME="+'"'+(str(row["TRN_TYPE"]))+'"')
                #print(mycursor.fetchall()[0][0])
                res=mycursor.fetchall()
                res=res[0][0]
                row["TRN_TYPE"]=str(res)
                cols=",".join(map(str, row.keys()))
                v_list=[]
                val=') VALUES('
                for v in row.values():
                    if v== None:
                        val=val+'NULL,'
                    else:
                        v_list.append(v)
                        val=val+'%s,'
                val=val[:-1]+')'
                query="insert into stg_trn_data(" +cols + val
                mycursor.execute(query,v_list)
                connection.commit()
            return JsonResponse({"status": 201, "message": "Data Inserted"})
        except IntegrityError:
            return JsonResponse({"status": 500, "message": "TRAN_SEQ_NO must be unique"})
        except ValueError:
            return JsonResponse({"status": 500, "message": "error"})
        except Exception as error:
            return JsonResponse({"status": 500, "message": str(error)})
        finally:
            mycursor.close()
            connection.close()


#Retrieve filtered data STG_TRN_DATA table using input parameters user and date.
@csrf_exempt             
def retrieve_stg(request):
    if request.method == 'POST':
        try:
            data = json.loads(request.body)
            data=data[0]
            if "DATE" not in data:
                data["DATE"]='NULL'
            query="SELECT * FROM STG_TRN_DATA WHERE PROCESS_IND IN ('E','N') AND"
            #coverting the date format
            if  data["DATE"]=="NULL" or data["DATE"]=="" or data["DATE"]==[] :
                data.pop("DATE")
            else:
                start_date =datetime.strptime(data["DATE"],"%Y-%m-%d") 
                end_date=datetime.combine(start_date, datetime.max.time())
                query=query+" CREATE_DATETIME BETWEEN '"+ str(start_date)+ "' AND '"+ str(end_date)+"' AND"
            #user validation
            if  data["USER"]=="NULL" or data["USER"]=="" or data["USER"]==[]:                
                data.pop("USER")
            else:
                if len(data['USER'])==1:
                    query=query+" CREATE_ID='"+ (data['USER'])[0]+"' AND"
                else:
                    query=query+" CREATE_ID"+" in "+str(tuple(data["USER"]))+" AND"
            query=query[:-4]+";"
            result=pd.read_sql(query,connection)
            res_list=[]                
            for val in result.values:
                count=0
                l_dict={}
                for col in result.columns:
                    l_dict[col]=val[count]
                    count=count+1
                #converting LOCATION ,REV_NO  to INTEGER
                l_dict["LOCATION"]=int(l_dict["LOCATION"])
                l_dict["REV_NO"]=int(l_dict["REV_NO"])
                #Appending each row
                res_list.append(l_dict)
            if len(res_list)==0:
                return JsonResponse({"status": 500,"message ":"No Data Found"})
            else:
                return JsonResponse(res_list, content_type="application/json",safe=False)
        except Exception as error:
            return JsonResponse({"status": 500, "message":str(error)})
        except ValueError:
            return JsonResponse({"status": 500, "message": "error"})



#Retrieve filtered data from ERR_TRN_DATA and STG_TRN_DATA table using input parameters user and date.
@csrf_exempt             
def retrieve_err_stg(request):
    if request.method == 'POST':
        try:
            data = json.loads(request.body)
            data=data[0]
            if "DATE" not in data:
                data["DATE"]='NULL'
            query="select * from ((select TRAN_SEQ_NO,PROCESS_IND,ITEM,REF_ITEM,REF_ITEM_TYPE,LOCATION_TYPE,LOCATION,TRN_TYPE,QTY,PACK_QTY,PACK_COST,PACK_RETAIL,UNIT_COST,UNIT_RETAIL,TOTAL_COST,TOTAL_RETAIL,REF_NO2,REF_NO3,REF_NO4,AREF,CURRENCY,CREATE_DATETIME,CREATE_ID,REV_NO,NULL AS ERR_MSG,NULL AS ERR_SEQ_NO,NULL AS HIER2,NULL AS HIER1,NULL AS HIER3,rev_trn_no from stg_trn_data) UNION (select TRAN_SEQ_NO,PROCESS_IND,ITEM,REF_ITEM,REF_ITEM_TYPE,LOCATION_TYPE,LOCATION,TRN_TYPE,QTY,PACK_QTY,PACK_COST,PACK_RETAIL,UNIT_COST,UNIT_RETAIL,TOTAL_COST,TOTAL_RETAIL,REF_NO2,REF_NO3,REF_NO4,AREF,CURRENCY,CREATE_DATETIME,CREATE_ID,REV_NO,ERR_MSG,ERR_SEQ_NO,HIER2,HIER1,HIER3,rev_trn_no from err_trn_data)) ERR_STG where "
            #coverting the date format
            if  data["DATE"]=="NULL" or data["DATE"]=="" or data["DATE"]==[] :
                data.pop("DATE")
            else:
                start_date =datetime.strptime(data["DATE"],"%Y-%m-%d") 
                end_date=datetime.combine(start_date, datetime.max.time())
                query=query+" CREATE_DATETIME BETWEEN '"+ str(start_date)+ "' AND '"+ str(end_date)+"' AND"
            #user validation
            if  data["USER"]=="NULL" or data["USER"]=="" or data["USER"]==[]:                
                data.pop("USER")
            else:
                if len(data['USER'])==1:
                    query=query+" CREATE_ID='"+ (data['USER'])[0]+"' AND"
                else:
                    query=query+" CREATE_ID"+" in "+str(tuple(data["USER"]))+" AND"
            if len(data)==0:
                query=query[:-6]+";"
            else:
                query=query[:-4]+";"
            result=pd.read_sql(query,connection)
            result =  result.replace(np.NaN, "NULL", regex=True)
            res_list=[]                
            for val in result.values:
                count=0
                l_dict={}
                for col in result.columns:
                    l_dict[col]=val[count]
                    count=count+1
                #converting LOCATION ,REV_NO  to INTEGER
                if l_dict["LOCATION"] !="NULL":
                    l_dict["LOCATION"]=int(l_dict["LOCATION"])
                if  l_dict["REV_NO"] !="NULL":
                    l_dict["REV_NO"]=int(l_dict["REV_NO"])
                #Appending each row
                res_list.append(l_dict)
            if len(res_list)==0:
                return JsonResponse({"status": 500,"message ":"No Data Found"})
            else:
                return JsonResponse(res_list, content_type="application/json",safe=False)
        except Exception as error:
            return JsonResponse({"status": 500, "message":str(error)})
        except ValueError:
            return JsonResponse({"status": 500, "message": "error"})
