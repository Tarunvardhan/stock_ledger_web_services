import json
import pandas as pd
from django.db import IntegrityError
#from .models import LOCATION, STG_TRN_DATA,TRN_DATA,PNDG_DLY_ROLLUP,STG_TRN_DATA_DEL_RECORDS,SYSTEM_CONFIG,ERR_TRN_DATA,DAILY_SKU,DAILY_ROLLUP,TRN_DATA_HISTORY,TRN_DATA_REV,CURRENCY,ITEM_LOCATION,ITEM_DTL,DEPT,CLASS,SUBCLASS
from django.http import JsonResponse
from datetime import datetime,date
from django.views.decorators.csrf import csrf_exempt
import time
import decimal
from decimal import Decimal
from decimal import *
import numpy as np
from django.db import connection
from stock_ledger_models.views_global import cancel_transaction


#count of different indicators in TRN_DATA table:
def count_trn_data(request):
    if request.method == 'GET':
        count1 = pd.read_sql("SELECT COUNT(PROCESS_IND) FROM TRN_DATA WHERE PROCESS_IND='N';",connection)
        count2 = pd.read_sql("SELECT COUNT(PROCESS_IND) FROM TRN_DATA WHERE PROCESS_IND='I';",connection)
        count3 = pd.read_sql("SELECT COUNT(PROCESS_IND) FROM TRN_DATA WHERE PROCESS_IND='E';",connection)
        count4 = pd.read_sql("SELECT COUNT(PROCESS_IND) FROM TRN_DATA WHERE PROCESS_IND='Y';",connection)
        count5 = pd.read_sql("SELECT COUNT(PROCESS_IND) FROM TRN_DATA WHERE PROCESS_IND='W';",connection)
        count6 = pd.read_sql("SELECT COUNT(PROCESS_IND) FROM TRN_DATA WHERE PROCESS_IND='C';",connection)
        count7 = pd.read_sql("SELECT RECORDS_CLEANED FROM STG_TRN_DATA_DEL_RECORDS WHERE DATE=curdate() AND PROCESS='TRN_DATA'",connection)
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


#Fetching the data from TRN_DATA based on the input parameters: 
@csrf_exempt            
def trn_data_table(request):
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
            mycursor.execute("desc trn_data")
            d_type=mycursor.fetchall()
            list_type=[]
            for col2 in d_type:
                if "decimal" in col2[1]:
                    if "LOCATION" in col2[0] or "REV_NO" in col2[0]:
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
                query="SELECT TD.*,ITD.ITEM_DESC,LOC.LOCATION_NAME,TTD.TRN_NAME,DT.DEPT_DESC,CL.CLASS_DESC,SCL.SUBCLASS_DESC FROM TRN_DATA TD,ITEM_DTL ITD,LOCATION LOC,TRN_TYPE_DTL TTD,DEPT DT,CLASS CL,SUBCLASS SCL WHERE TD.ITEM=ITD.ITEM AND LOC.LOCATION=TD.LOCATION AND TD.DEPT=DT.DEPT AND TD.TRN_TYPE=TTD.TRN_TYPE AND CL.CLASS=TD.CLASS AND SCL.SUBCLASS=TD.SUBCLASS AND IFNULL(TD.AREF,0)=IFNULL(TTD.AREF,0) AND {}".format(' '.join('TD.{} IN ({}) AND'.format(k,str(json_object[k])[1:-1]) for k in json_object))
            else:
                query="SELECT TD.*,ITD.ITEM_DESC,LOC.LOCATION_NAME,TTD.TRN_NAME,DT.DEPT_DESC,CL.CLASS_DESC,SCL.SUBCLASS_DESC FROM TRN_DATA TD,ITEM_DTL ITD,LOCATION LOC,TRN_TYPE_DTL TTD,DEPT DT,CLASS CL,SUBCLASS SCL WHERE TD.ITEM=ITD.ITEM AND LOC.LOCATION=TD.LOCATION AND TD.TRN_TYPE=TTD.TRN_TYPE AND TD.DEPT=DT.DEPT AND CL.CLASS=TD.CLASS AND SCL.SUBCLASS=TD.SUBCLASS AND IFNULL(TD.AREF,0)=IFNULL(TTD.AREF,0) AND {}".format(' '.join('TD.{} LIKE "%{}%" AND'.format(k,json_object[k]) for k in json_object))
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


#Fetching the data from TRN_DATA_HISTORY based on the input parameters:
@csrf_exempt            
def trn_data_history_table(request):
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
            mycursor.execute("desc trn_data_history")
            d_type=mycursor.fetchall()
            list_type=[]
            for col2 in d_type:
                if "decimal" in col2[1]:
                    if "LOCATION" in col2[0] or "REV_NO" in col2[0]:
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
                query="SELECT TD.*,ITD.ITEM_DESC,LOC.LOCATION_NAME,TTD.TRN_NAME,DT.DEPT_DESC,CL.CLASS_DESC,SCL.SUBCLASS_DESC FROM TRN_DATA_HISTORY TD,ITEM_DTL ITD,LOCATION LOC,TRN_TYPE_DTL TTD,DEPT DT,CLASS CL,SUBCLASS SCL WHERE TD.ITEM=ITD.ITEM AND LOC.LOCATION=TD.LOCATION AND TD.DEPT=DT.DEPT AND TD.TRN_TYPE=TTD.TRN_TYPE AND CL.CLASS=TD.CLASS AND SCL.SUBCLASS=TD.SUBCLASS AND IFNULL(TD.AREF,0)=IFNULL(TTD.AREF,0) AND {}".format(' '.join('TD.{} IN ({}) AND'.format(k,str(json_object[k])[1:-1]) for k in json_object))
            else:
                query="SELECT TD.*,ITD.ITEM_DESC,LOC.LOCATION_NAME,TTD.TRN_NAME,DT.DEPT_DESC,CL.CLASS_DESC,SCL.SUBCLASS_DESC FROM TRN_DATA_HISTORY TD,ITEM_DTL ITD,LOCATION LOC,TRN_TYPE_DTL TTD,DEPT DT,CLASS CL,SUBCLASS SCL WHERE TD.ITEM=ITD.ITEM AND LOC.LOCATION=TD.LOCATION AND TD.TRN_TYPE=TTD.TRN_TYPE AND TD.DEPT=DT.DEPT AND CL.CLASS=TD.CLASS AND SCL.SUBCLASS=TD.SUBCLASS AND IFNULL(TD.AREF,0)=IFNULL(TTD.AREF,0) AND {}".format(' '.join('TD.{} LIKE "%{}%" AND'.format(k,json_object[k]) for k in json_object))
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


#Transaction reversal(Fetch the record from TRN_DATA_HISTORY table, insert the original data in TRN_DATA_REV, insert the updated record values in STG_TRN_DATA table & insert the QTY*(-1) into record in STG_TRN_DATA table with new TRAN_SEQ_NO)
@csrf_exempt             
def trn_data_rev_table(request):
    if request.method == 'POST':
        try:
            json_object_list = json.loads(request.body)
            #json_object=json_object[0]
            current_user = request.user
            list1=[]
            list2=[]
            list3=[]
            count1=0
            mycursor = connection.cursor()
            for json_object in json_object_list:
                keys=[]
                #Removing NULL and Empty columns.
                for key in json_object:
                    if json_object[key]=="NULL" or json_object[key]=="":
                        json_object[key]=None
                        keys.append(key)
                    if key=="ITEM_DESC" or key=="DEPT_DESC" or key=="CLASS_DESC" or key=="SUBCLASS_DESC" or key=="LOCATION_NAME" or key=="TRN_NAME" or key=="ARCHIVE_DATETIME":
                        keys.append(key)
                for k in keys:
                    json_object.pop(k)
                list1.append(json_object)
            
            for json_object in list1:
                #Storing the value of TRAN_SEQ_NO column
                TRANS_NO=json_object.get("TRAN_SEQ_NO", None)
                if len(json_object)>0:
                    #Taking a record from TRN_DATA_HISTORY table based on TRAN_SEQ_NO.
                    my_data = pd.read_sql("SELECT * FROM TRN_DATA_HISTORY WHERE TRAN_SEQ_NO={};".format(TRANS_NO),connection)
                    res_list=[]
                    #Converting the Queryset into the json format
                    for val in my_data.values:
                        count=0
                        l_dict={}
                        for col in my_data.columns:
                            l_dict[col]=val[count]
                            count=count+1
                        res_list.append(l_dict)
                    rec1=res_list[0]
                    mycursor.execute("desc TRN_DATA_HISTORY")
                    d_type=mycursor.fetchall()
                    list_type=[]
                    list_type1=[]
                    for col2 in d_type:
                        if "decimal" in col2[1]:
                            if col2=="LOCATION"or col2=="REV_NO":
                                list_type.append(col2[0])
                    for key1 in rec1:
                        if rec1[key1]==None:
                            list_type1.append(key1)
                    for k5 in list_type1:
                        rec1.pop(k5)
                    #converting LOCATION ,REV_NO AND ERR_SEQ_NO to INTEGER  if DECIMAL
                    for col3 in list_type:
                        if col3 in rec1:
                            rec1[col3]=int(rec1[col3])
                    #converting LOCATION ,REV_NO AND ERR_SEQ_NO to INTEGER  if DECIMAL
                    for col3 in list_type:
                        if col3 in json_object:
                            json_object[col3]=int(json_object[col3])
                    #Removing the same values from input to TRN_DATA_HISTORY table.
                    remove=[]
                    rec1["TRN_DATE"]=str(rec1["TRN_DATE"])
                    for key in json_object:
                        if key in list_type:
                            if Decimal(json_object[key])==rec1[key]:
                                remove.append(key)   
                        else:
                            if json_object[key]==rec1[key]:
                                remove.append(key)  
                    for p in remove:
                        json_object.pop(p)
                json_object["TRAN_SEQ_NO"]=TRANS_NO
                list2.append(json_object)
            for json_object in list2:
                #Storing the value of TRAN_SEQ_NO column
                TRANS_NO=json_object.get("TRAN_SEQ_NO", None)
                json_object.pop("TRAN_SEQ_NO")
                if len(json_object)>0:
                    D_keys=[]
                    for row in res_list:
                        #Removing NULL and Empty columns.
                        for k1 in row:
                            if row[k1]=="" or (row[k1])=="NULL":
                                D_keys.append(k1) 
                        for k1 in D_keys:
                            row.pop(k1)
                        D_keys.clear()
                        json_object["TRAN_SEQ_NO"]=TRANS_NO
                        #inserting the data.
                        row["CREATE_ID"]=str(current_user)
                        row["TRAN_SEQ_NO"]=TRANS_NO
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
                        query="insert into TRN_DATA_REV(" +cols + val
                        mycursor.execute(query,v_list)
                    list3.append(json_object)
            for json_object in list3:
                count1=count1+1
                #Taking a record from STG_TRN_DATA of 1 record.
                TRANS_NO=json_object.get("TRAN_SEQ_NO", None)
                my_data = pd.read_sql("SELECT * FROM TRN_DATA_HISTORY WHERE TRAN_SEQ_NO={};".format(TRANS_NO),connection)
                res_list=[]
                #Converting the Queryset into the json format
                for val in my_data.values:
                    count=0
                    l_dict={}
                    for col in my_data.columns:
                        l_dict[col]=val[count]
                        count=count+1
                    res_list.append(l_dict)
                if mycursor.rowcount>0 and connection:
                    my_data = pd.read_sql("SELECT * FROM STG_TRN_DATA LIMIT 1;",connection)
                    result5=[]
                    #Converting the Queryset into the json format
                    for val in my_data.values:
                        count=0
                        l_dict={}
                        for col in my_data.columns:
                            l_dict[col]=val[count]
                            count=count+1
                        result5.append(l_dict)
                    rec2=result5[0] 
                    rec3=res_list[0]
                    D_keys1=[]
                    f_keys1=[]
                    R_keys1=[]
                    #Removing extra columns from STG_TRN_DATA table when compared with TRN_DATA_HISTORY table
                    for f1 in rec2:
                        if f1 not in rec3:
                            f_keys1.append(f1)
                    for f2 in f_keys1:
                        rec2.pop(f2)
                    l_counter=0 
                    for item in res_list:
                        #Removing extra columns from TRN_DATA_HISTORY table when compared with STG_TRN_DATA table
                        for f3 in item:
                            if f3 not in rec2:
                                R_keys1.append(f3)
                        for f5 in R_keys1:
                            item.pop(f5)
                        R_keys1.clear()
                        #Removing NULL and Empty columns.
                        for k2 in item:
                            if item[k2]=="" or (item[k2])=="NULL":
                                D_keys1.append(k2)        
                        for k2 in D_keys1:
                            item.pop(k2)
                        D_keys1.clear()
                        for key in json_object:
                            item[key]=json_object[key]
                        #Creating a new TRAN_SEQ_NO number
                        l_counter=l_counter+1
                        d= str(datetime.now()).replace('-',"").replace(':',"").replace(' ',"").replace('.',"")
                        unique_id=d+str(l_counter)+'TDR'
                        item["TRAN_SEQ_NO"]=unique_id
                        item["REV_NO"]=(int(item["REV_NO"])+1)
                        item["REV_TRN_NO"]=TRANS_NO
                        item["CREATE_DATETIME"]=str(datetime.now())
                        item["CREATE_ID"]=str(current_user)
                        #Values for inserting into the table
                        cols=",".join(map(str, item.keys()))
                        v_list=[]
                        val=') VALUES('
                        for v in item.values():
                            if v== None:
                                val=val+'NULL,'
                            else:
                                v_list.append(v)
                                val=val+'%s,'
                        val=val[:-1]+')'
                        query1="insert into stg_trn_data(" +cols + val
                        mycursor.execute(query1,v_list)
                    #Taking a record from TRN_DATA_HISTORY table based on TRAN_SEQ_NO.
                    if mycursor.rowcount>0 and connection:
                        my_data1 = pd.read_sql("SELECT * FROM TRN_DATA_HISTORY WHERE TRAN_SEQ_NO={};".format(TRANS_NO),connection)
                        val_list=[]
                        #Converting the Queryset into the json format
                        for val in my_data1.values:
                            count=0
                            l_dict={}
                            for col in my_data1.columns:
                                l_dict[col]=val[count]
                                count=count+1
                            val_list.append(l_dict)
                        D_keys2=[]
                        R_keys2=[]
                        l_counter=0
                        for item1 in val_list:
                            #Removing extra columns from TRN_DATA_HISTORY table when compared with STG_TRN_DATA table
                            for f6 in item1:
                                if f6 not in rec2:
                                    R_keys2.append(f6)       
                            for f7 in R_keys2:
                                item1.pop(f7)
                            R_keys2.clear()
                            #Removing NULL and Empty columns.
                            for k3 in item1:
                                if item1[k3]=="" or (item1[k3])=="NULL":
                                    D_keys2.append(k3)       
                            for k3 in D_keys2:
                                item1.pop(k3)
                            D_keys2.clear()
                            #Creating a new TRAN_SEQ_NO number
                            l_counter=l_counter+1
                            d= str(datetime.now()).replace('-',"").replace(':',"").replace(' ',"").replace('.',"")
                            unique_id=d+str(l_counter)+'TDC'
                            item1["TRAN_SEQ_NO"]=unique_id
                            item1["REV_TRN_NO"]=TRANS_NO
                            item1["QTY"]=item1["QTY"]*(-1)
                            item1["CREATE_DATETIME"]=str(datetime.now())
                            item1["CREATE_ID"]=str(current_user)
                            #Values for inserting into the table
                            cols=",".join(map(str, item1.keys()))
                            v_list=[]
                            val=') VALUES('
                            for v in item1.values():
                                if v== None:
                                    val=val+'NULL,'
                                else:
                                    v_list.append(v)
                                    val=val+'%s,'
                            val=val[:-1]+')'
                            query3="insert into stg_trn_data(" +cols + val
                            mycursor.execute(query3,v_list)
                            connection.commit()
                            mycursor.execute("DELETE FROM TRN_DATA_HISTORY WHERE TRAN_SEQ_NO='{}'".format(TRANS_NO))
                            connection.commit()
                    else:
                        connection.rollback()
                else:
                    connection.rollback()
            return JsonResponse({"status": 201, "message": "Data Inserted "f"{count1}"})
        except Exception as error:
            return JsonResponse({"status": 500, "message": str(error)})
        except ValueError:
            return JsonResponse({"status": 500, "message": "error"})
        except IntegrityError:
            return JsonResponse({"status": 500, "message": "TRAN_SEQ_NO must be unique"})
        finally:
             connection.close()


#Transaction reversal(Fetch the record from TRN_DATA_HISTORY table, insert the original data in TRN_DATA_REV, insert the updated record values in STG_TRN_DATA table with new TRAN_SEQ_NO & call the another webservice(cancel_transaction))           
@csrf_exempt             
def trn_data_rev_1_table(request):
    if request.method == 'POST':
        try:
            json_object_list = json.loads(request.body)
            #json_object=json_object[0]
            current_user = request.user
            list1=[]
            list2=[]
            list3=[]
            count1=0
            mycursor = connection.cursor()
            for json_object in json_object_list:
                keys=[]
                #Removing NULL and Empty columns.
                for key in json_object:
                    if json_object[key]=="NULL" or json_object[key]=="":
                        json_object[key]=None
                        keys.append(key)
                    if key=="ITEM_DESC" or key=="DEPT_DESC" or key=="CLASS_DESC" or key=="SUBCLASS_DESC" or key=="LOCATION_NAME" or key=="TRN_NAME" or key=="ARCHIVE_DATETIME":
                        keys.append(key)
                for k in keys:
                    json_object.pop(k)
                list1.append(json_object)
            
            for json_object in list1:
                #Storing the value of TRAN_SEQ_NO column
                TRANS_NO=json_object.get("TRAN_SEQ_NO", None)
                if len(json_object)>0:
                    #Taking a record from TRN_DATA_HISTORY table based on TRAN_SEQ_NO.
                    my_data = pd.read_sql("SELECT * FROM TRN_DATA_HISTORY WHERE TRAN_SEQ_NO={};".format(TRANS_NO),connection)
                    res_list=[]
                    #Converting the Queryset into the json format
                    for val in my_data.values:
                        count=0
                        l_dict={}
                        for col in my_data.columns:
                            l_dict[col]=val[count]
                            count=count+1
                        res_list.append(l_dict)
                    rec1=res_list[0]
                    mycursor.execute("desc TRN_DATA_HISTORY")
                    d_type=mycursor.fetchall()
                    list_type=[]
                    list_type1=[]
                    for col2 in d_type:
                        if "decimal" in col2[1]:
                            if col2=="LOCATION"or col2=="REV_NO":
                                list_type.append(col2[0])
                    for key1 in rec1:
                        if rec1[key1]==None:
                            list_type1.append(key1)
                    for k5 in list_type1:
                        rec1.pop(k5)
                    #converting LOCATION ,REV_NO AND ERR_SEQ_NO to INTEGER  if DECIMAL
                    for col3 in list_type:
                        if col3 in rec1:
                            rec1[col3]=int(rec1[col3])
                    #converting LOCATION ,REV_NO AND ERR_SEQ_NO to INTEGER  if DECIMAL
                    for col3 in list_type:
                        if col3 in json_object:
                            json_object[col3]=int(json_object[col3])
                    #Removing the same values from input to TRN_DATA_HISTORY table.
                    remove=[]
                    rec1["TRN_DATE"]=str(rec1["TRN_DATE"])
                    for key in json_object:
                        if key in list_type:
                            if Decimal(json_object[key])==rec1[key]:
                                remove.append(key)   
                        else:
                            if json_object[key]==rec1[key]:
                                remove.append(key)  
                    for p in remove:
                        json_object.pop(p)
                json_object["TRAN_SEQ_NO"]=TRANS_NO
                list2.append(json_object)
            for json_object in list2:
                #Storing the value of TRAN_SEQ_NO column
                TRANS_NO=json_object.get("TRAN_SEQ_NO", None)
                json_object.pop("TRAN_SEQ_NO")
                if len(json_object)>0:
                    D_keys=[]
                    for row in res_list:
                        #Removing NULL and Empty columns.
                        for k1 in row:
                            if row[k1]=="" or (row[k1])=="NULL":
                                D_keys.append(k1) 
                        for k1 in D_keys:
                            row.pop(k1)
                        D_keys.clear()
                        json_object["TRAN_SEQ_NO"]=TRANS_NO
                        #inserting the data.
                        row["CREATE_ID"]=str(current_user)
                        row["TRAN_SEQ_NO"]=TRANS_NO
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
                        query="insert into TRN_DATA_REV(" +cols + val
                        mycursor.execute(query,v_list)
                    list3.append(json_object)
            for json_object in list3:
                count1=count1+1
                #Taking a record from STG_TRN_DATA of 1 record.
                TRANS_NO=json_object.get("TRAN_SEQ_NO", None)
                my_data = pd.read_sql("SELECT * FROM TRN_DATA_HISTORY WHERE TRAN_SEQ_NO={};".format(TRANS_NO),connection)
                res_list=[]
                #Converting the Queryset into the json format
                for val in my_data.values:
                    count=0
                    l_dict={}
                    for col in my_data.columns:
                        l_dict[col]=val[count]
                        count=count+1
                    res_list.append(l_dict)
                if mycursor.rowcount>0 and connection:
                    my_data = pd.read_sql("SELECT * FROM STG_TRN_DATA LIMIT 1;",connection)
                    result5=[]
                    #Converting the Queryset into the json format
                    for val in my_data.values:
                        count=0
                        l_dict={}
                        for col in my_data.columns:
                            l_dict[col]=val[count]
                            count=count+1
                        result5.append(l_dict)
                    rec2=result5[0] 
                    rec3=res_list[0]
                    D_keys1=[]
                    f_keys1=[]
                    R_keys1=[]
                    #Removing extra columns from STG_TRN_DATA table when compared with TRN_DATA_HISTORY table
                    for f1 in rec2:
                        if f1 not in rec3:
                            f_keys1.append(f1)
                    for f2 in f_keys1:
                        rec2.pop(f2)
                    l_counter=0 
                    for item in res_list:
                        #Removing extra columns from TRN_DATA_HISTORY table when compared with STG_TRN_DATA table
                        for f3 in item:
                            if f3 not in rec2:
                                R_keys1.append(f3)
                        for f5 in R_keys1:
                            item.pop(f5)
                        R_keys1.clear()
                        #Removing NULL and Empty columns.
                        for k2 in item:
                            if item[k2]=="" or (item[k2])=="NULL":
                                D_keys1.append(k2)        
                        for k2 in D_keys1:
                            item.pop(k2)
                        D_keys1.clear()
                        for key in json_object:
                            item[key]=json_object[key]
                        #Creating a new TRAN_SEQ_NO number
                        l_counter=l_counter+1
                        d= str(datetime.now()).replace('-',"").replace(':',"").replace(' ',"").replace('.',"")
                        unique_id=d+str(l_counter)+'TDR'
                        item["TRAN_SEQ_NO"]=unique_id
                        item["REV_NO"]=(int(item["REV_NO"])+1)
                        item["REV_TRN_NO"]=TRANS_NO
                        item["CREATE_DATETIME"]=str(datetime.now())
                        item["CREATE_ID"]=str(current_user)
                        #Values for inserting into the table
                        cols=",".join(map(str, item.keys()))
                        v_list=[]
                        val=') VALUES('
                        for v in item.values():
                            if v== None:
                                val=val+'NULL,'
                            else:
                                v_list.append(v)
                                val=val+'%s,'
                        val=val[:-1]+')'
                        query1="insert into stg_trn_data(" +cols + val
                        mycursor.execute(query1,v_list)
                        connection.commit()
                        return cancel_transaction(request)
        except Exception as error:
            return JsonResponse({"status": 500, "message": str(error)})
        except ValueError:
            return JsonResponse({"status": 500, "message": "error"})
        except IntegrityError:
            return JsonResponse({"status": 500, "message": "TRAN_SEQ_NO must be unique"})
        finally:
             mycursor.close()
             connection.close()


