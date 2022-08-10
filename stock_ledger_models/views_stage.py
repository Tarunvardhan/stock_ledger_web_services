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
        count5 = pd.read_sql("SELECT RECORDS_CLEANED FROM stg_trn_data_del_records WHERE DATE=curdate() AND PROCESS='STG_TRN_DATA'",connection)
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
                for keys1 in row:
                    if keys1=="TRN_TYPE":
                        TRN_TYPE=row["TRN_TYPE"]
                        query1=pd.read_sql("SELECT TRN_TYPE,AREF FROM trn_type_dtl where TRN_NAME='{}'".format(TRN_TYPE),connection)
                        for val in query1.values:
                            count=0
                            l_dict={}
                            for col in query1.columns:
                                l_dict[col]=val[count]
                                count=count+1
                l_counter=l_counter+1
                d= str(datetime.now()).replace('-',"").replace(':',"").replace(' ',"").replace('.',"")
                unique_id=d+str(l_counter)+'STG'
                row["TRAN_SEQ_NO"]=unique_id
                row["TRN_TYPE"]=l_dict["TRN_TYPE"]
                row["AREF"]=l_dict["AREF"]
                row["PROCESS_IND"]='N'
                row["CREATE_DATETIME"]=str(datetime.now())
                row["CREATE_ID"]=str(current_user)
                row["REV_NO"]=1
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
                query="insert into STG_TRN_DATA(" +cols + val
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
            key_list=[]
            count=0
            data["CREATE_ID"]=data.pop("USER")
            for key in data:
              if isinstance(data[key], list):
                  if len(data[key])==0:
                      key_list.append(key)
              if data[key]=="" or data[key]=="NULL":
                  key_list.append(key)
            for key in key_list:
               data.pop(key)
            
            if "DATE" not in data:
                data["DATE"]='NULL'  
            #query="SELECT * FROM ((SELECT TRAN_SEQ_NO,STG.PROCESS_IND,STG.ITEM,NULL AS ITEM_DESC,STG.REF_ITEM,STG.REF_ITEM_TYPE,STG.LOCATION_TYPE,STG.LOCATION,LOC.LOCATION_NAME,STG.TRN_TYPE,TRN.TRN_NAME,STG.QTY,STG.PACK_QTY,STG.PACK_COST,STG.PACK_RETAIL,STG.UNIT_COST,STG.UNIT_RETAIL,STG.TOTAL_COST,STG.TOTAL_RETAIL,STG.REF_NO1,STG.REF_NO2,STG.REF_NO3,STG.REF_NO4,STG.AREF,STG.CURRENCY,STG.CREATE_DATETIME,STG.CREATE_ID,STG.REV_NO,NULL AS ERR_MSG,NULL AS ERR_SEQ_NO,NULL AS HIER2,NULL AS HIER2_DESC,NULL AS HIER1,NULL AS HIER1_DESC,NULL AS HIER3,NULL AS HIER3_DESC,STG.REV_TRN_NO FROM STG_TRN_DATA STG,trn_type_dtl TRN, location LOC WHERE STG.LOCATION=LOC.LOCATION AND STG.TRN_TYPE=TRN.TRN_TYPE) UNION (SELECT ERR.TRAN_SEQ_NO,ERR.PROCESS_IND,ERR.ITEM,ID.ITEM_DESC,ERR.REF_ITEM,ERR.REF_ITEM_TYPE,ERR.LOCATION_TYPE,ERR.LOCATION,LOC.LOCATION_NAME,ERR.TRN_TYPE,TRN.TRN_NAME,ERR.QTY,ERR.PACK_QTY,ERR.PACK_COST,ERR.PACK_RETAIL,ERR.UNIT_COST,ERR.UNIT_RETAIL,ERR.TOTAL_COST,ERR.TOTAL_RETAIL,ERR.REF_NO1,ERR.REF_NO2,ERR.REF_NO3,ERR.REF_NO4,ERR.AREF,ERR.CURRENCY,ERR.CREATE_DATETIME,ERR.CREATE_ID,ERR.REV_NO,ERR.ERR_MSG,ERR.ERR_SEQ_NO,ERR.HIER2,CL.HIER2_DESC,ERR.HIER1,DT.HIER1_DESC,ERR.HIER3,SCL.HIER3_DESC,ERR.REV_TRN_NO FROM err_trn_data ERR,trn_type_dtl TRN,item_dtl ID,hier1 DT,hier2 CL,hier3 SCL,location LOC WHERE ERR.TRN_TYPE=TRN.TRN_TYPE AND ERR.HIER1=DT.HIER1 AND ERR.HIER2=CL.HIER2 AND ERR.HIER3=SCL.HIER3 AND ERR.LOCATION=LOC.LOCATION)) ESTG WHERE "
            query="SELECT * FROM ((( SELECT ERR.TRAN_SEQ_NO,ERR.PROCESS_IND,ERR.ITEM,ID.ITEM_DESC,ERR.REF_ITEM,ERR.REF_ITEM_TYPE,ERR.LOCATION_TYPE,ERR.LOCATION,LOC.LOCATION_NAME,ERR.TRN_POST_DATE,ERR.TRN_DATE,ERR.TRN_TYPE,TRN.TRN_NAME,ERR.QTY,ERR.SELLING_UOM,ERR.PACK_QTY,ERR.PACK_COST,ERR.PACK_RETAIL,ERR.UNIT_COST,ERR.UNIT_RETAIL,ERR.TOTAL_COST,ERR.TOTAL_RETAIL,ERR.REF_NO1,ERR.REF_NO2,ERR.REF_NO3,ERR.REF_NO4,ERR.CURRENCY,ERR.CREATE_DATETIME,ERR.CREATE_ID,ERR.REV_NO,ERR.ERR_MSG,ERR.ERR_SEQ_NO,ERR.HIER1,DT.HIER1_DESC,ERR.HIER2,CL.HIER2_DESC,ERR.HIER3,SCL.HIER3_DESC,ERR.REV_TRN_NO ,ERR.AREF,NULL AS ARCHIEVE_DATETIME FROM err_trn_data ERR,trn_type_dtl TRN,item_dtl ID,hier1 DT,hier2 CL,hier3 SCL,location LOC WHERE ERR.TRN_TYPE=TRN.TRN_TYPE AND ERR.AREF=TRN.AREF AND ERR.HIER1=DT.HIER1 AND ERR.HIER2=CL.HIER2 AND ERR.HIER3=SCL.HIER3 AND ERR.LOCATION=LOC.LOCATION AND ERR.ITEM=ID.ITEM AND PROCESS_IND IN ('E') ) UNION   (SELECT TRAN_SEQ_NO,STG.PROCESS_IND,STG.ITEM,ID.ITEM_DESC,STG.REF_ITEM,STG.REF_ITEM_TYPE,STG.LOCATION_TYPE,STG.LOCATION,LOC.LOCATION_NAME,STG.TRN_TYPE,TRN.TRN_NAME, STG.QTY,STG.PACK_QTY,STG.PACK_COST,STG.PACK_RETAIL,STG.UNIT_COST,STG.UNIT_RETAIL,STG.TOTAL_COST,STG.TOTAL_RETAIL,STG.REF_NO1,STG.REF_NO2,STG.REF_NO3,STG.REF_NO4,STG.AREF, STG.CURRENCY,STG.CREATE_DATETIME,STG.CREATE_ID,STG.REV_NO,NULL AS TRN_POST_DATE,NULL AS TRN_DATE,NULL AS ERR_MSG,NULL AS ERR_SEQ_NO,NULL AS HIER2,NULL AS HIER2_DESC, NULL AS HIER1,NULL AS HIER1_DESC,NULL AS HIER3,NULL AS HIER3_DESC,STG.REV_TRN_NO,NULL AS SELLING_UOM,NULL AS ARCHIEVE_DATETIME FROM stg_trn_data STG,trn_type_dtl TRN,item_dtl ID,  location LOC WHERE STG.LOCATION=LOC.LOCATION AND STG.TRN_TYPE=TRN.TRN_TYPE AND  STG.AREF=TRN.AREF  AND STG.ITEM=ID.ITEM AND PROCESS_IND IN ('N','I') ))  UNION (SELECT TDH.TRAN_SEQ_NO, TDH.PROCESS_IND, TDH.ITEM, ID.ITEM_DESC,  TDH.REF_ITEM, TDH.REF_ITEM_TYPE, TDH.LOCATION_TYPE, TDH.LOCATION,LOC.LOCATION_NAME, TDH.TRN_POST_DATE, TDH.TRN_DATE,TDH.TRN_TYPE, TRN.TRN_NAME, TDH.QTY, TDH.SELLING_UOM, TDH.PACK_QTY, TDH.PACK_COST, TDH.PACK_RETAIL, TDH.UNIT_COST, TDH.UNIT_RETAIL, TDH.TOTAL_COST, TDH.TOTAL_RETAIL, TDH.REF_NO1, TDH.REF_NO2, TDH.REF_NO3, TDH.REF_NO4, TDH.CURRENCY, TDH.REV_NO,NULL AS ERR_MSG,NULL AS ERR_SEQ_NO ,TDH.HIER1, H1.HIER1_DESC, TDH.HIER2, H2.HIER2_DESC, TDH.HIER3, H3.HIER3_DESC, TDH.REV_TRN_NO, TDH.CREATE_ID,  TDH.AREF, TDH.ARCHIEVE_DATETIME,NULL AS CREATE_DATETIME FROM trn_data_history TDH,  item_dtl ID, hier1 H1, hier2 H2,  hier3 H3,location LOC,trn_type_dtl TRN   WHERE TDH.ITEM=ID.ITEM AND TDH.HIER1=H1.HIER1 AND TDH.HIER2=H2.HIER2 AND TDH.HIER3=H3.HIER3 AND TDH.LOCATION=LOC.LOCATION AND TDH.TRN_TYPE=TRN.TRN_TYPE AND TDH.AREF=TRN.AREF)) ESTG where "
            #coverting the date format
            if  data["DATE"]=="NULL" or data["DATE"]=="":
                data.pop("DATE")
            else:
                start_date =datetime.strptime(data["DATE"],"%Y-%m-%d")
                end_date=datetime.combine(start_date, datetime.max.time())
                count=1
                query=query+" CREATE_DATETIME BETWEEN '"+ str(start_date)+ "' AND '"+ str(end_date)+"' AND "
                data.pop("DATE")
            for key in data:
                if isinstance(data[key], list):
                    if len(data[key])==1:
                        data[key]=(data[key])[0]
                        query=query+key+" in ('"+str(data[key])+"') AND "
                    else:
                        query=query+key+" in "+str(tuple(data[key]))+" AND "
                else:
                    query=query+key+"='"+str(data[key])+"' AND "
            if count==1:
                data["DATE"]=''
            if len(data)==0:
                query=query[:-6]+";"
            else:
                query=query[:-4]+";"
            result=pd.read_sql(query,connection)
            result =  result.replace(np.NaN, None, regex=True)
            res_list=[]                
            for val in result.values:
                count=0
                l_dict={}
                for col in result.columns:
                    l_dict[col]=val[count]
                    count=count+1
                #converting LOCATION ,REV_NO  to INTEGER
                if l_dict["LOCATION"] !=None:
                    l_dict["LOCATION"]=int(l_dict["LOCATION"])
                if  l_dict["REV_NO"] !=None:
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



#Fetching all the column values from ERR_TRN_DATA table:
def stg_fin(request):
    if request.method == 'GET':
        try:
            query="select * from stg_fin_data"
            result=pd.read_sql(query,connection)
            result = result.replace(np.NaN, None, regex=True)
            res_list=[]
            for val1 in result.values:
                count=0
                rec={}
                for col in result.columns:
                    rec[col]=val1[count]
                    count=count+1
                res_list.append(rec)
                
            if len(res_list)==0:
                return JsonResponse({"status": 500, "message":"NO DATA FOUND"})
            else:
                return JsonResponse(res_list,content_type="application/json",safe=False)
        except Exception as error:
            return JsonResponse({"status": 500, "message":str(error)})
        except ValueError:
            return JsonResponse({"status": 500, "message": "error"})

