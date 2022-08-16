import json
import csv
import pandas as pd
from django.db import IntegrityError
#from .models import LOCATION, STG_TRN_DATA,TRN_DATA,PNDG_DLY_ROLLUP,STG_TRN_DATA_DEL_RECORDS,SYSTEM_CONFIG,ERR_TRN_DATA,DAILY_SKU,DAILY_ROLLUP,trn_data_history,trn_data_rev,CURRENCY,ITEM_LOCATION,ITEM_DTL,HIER1,HIER2,HIER3
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


#Transaction reversal(Fetch the record from trn_data_history table, insert the QTY*(-1) into record in STG_TRN_DATA table with new TRAN_SEQ_NO)      
@csrf_exempt             
def cancel_transaction(request):
    if request.method == 'POST':
        try:
            json_object_list = json.loads(request.body)
            list1=[]
            list3=[]
            count_1=0
            mycursor=connection.cursor()
            for json_object in json_object_list:
                for key in json_object:
                    if key!="TRAN_SEQ_NO":
                        list1.append(key)
                for k in list1:
                    json_object.pop(k)
                list1.clear()
            for json_object in json_object_list:
                if "TRAN_SEQ_NO" in json_object:
                    #Storing the value of TRAN_SEQ_NO column
                    TRANS_NO=json_object.get("TRAN_SEQ_NO", None)
                    #Taking a record from trn_data_history table based on TRAN_SEQ_NO.
                    my_data = pd.read_sql("SELECT * FROM trn_data_history WHERE TRAN_SEQ_NO={};".format(TRANS_NO),connection)
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
                    D_keys=[]
                    R_keys1=[]
                    ARCHIEVE_DATETIME=l_dict["ARCHIEVE_DATETIME"]
                    #fetching columns names
                    mycursor.execute("desc trn_data_rev")
                    d_type=mycursor.fetchall()
                    list_type=[]
                    list_val1=[]
                    for col2 in d_type:
                        list_val1.append(col2[0])
                        if col2[0] not in rec1:
                            list_type.append(col2[0])
                    for h1 in list_type:
                        list_val1.remove(h1)
                    for row in res_list:
                        #Removing extra columns from trn_data_history table when compared with trn_data_rev table
                        for f11 in row:
                            if f11 not in list_val1:
                                R_keys1.append(f11)       
                        for f21 in R_keys1:
                            row.pop(f21)
                        R_keys1.clear()
                        #Removing NULL and Empty columns.
                        for k1 in row:
                            if row[k1]=="" or (row[k1])=="NULL"  or row[k1]==None:
                                D_keys.append(k1) 
                        for k1 in D_keys:
                            row.pop(k1)
                        D_keys.clear()
                        #inserting the data.
                        row["UPDATE_DATETIME"]=ARCHIEVE_DATETIME
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
                        query="insert into trn_data_rev(" +cols + val
                        mycursor.execute(query,v_list)
                    list3.append(json_object)
            for json_object in list3:
                #Taking a record from STG_TRN_DATA of 1 record.
                TRANS_NO=json_object.get("TRAN_SEQ_NO", None)
                my_data = pd.read_sql("SELECT * FROM trn_data_history WHERE TRAN_SEQ_NO={};".format(TRANS_NO),connection)
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
                    #Removing extra columns from STG_TRN_DATA table when compared with trn_data_history table
                    for f1 in rec2:
                        if f1 not in rec3:
                            f_keys1.append(f1)
                    for f2 in f_keys1:
                        rec2.pop(f2)
                    l_counter=0 
                    for item1 in res_list:
                        #Removing extra columns from trn_data_history table when compared with STG_TRN_DATA table
                        for f6 in item1:
                            if f6 not in rec2:
                                R_keys1.append(f6)       
                        for f7 in R_keys1:
                            item1.pop(f7)
                        R_keys1.clear()
                        #Removing NULL and Empty columns.
                        for k3 in item1:
                            if item1[k3]=="" or (item1[k3])=="NULL":
                                D_keys1.append(k3)       
                        for k3 in D_keys1:
                            item1.pop(k3)
                        D_keys1.clear()
                        #Creating a new TRAN_SEQ_NO number
                        l_counter=l_counter+1
                        d= str(datetime.now()).replace('-',"").replace(':',"").replace(' ',"").replace('.',"")
                        unique_id=d+str(l_counter)+'TDC'
                        item1["TRAN_SEQ_NO"]=unique_id
                        item1["REV_TRN_NO"]=TRANS_NO
                        item1["QTY"]=item1["QTY"]*(-1)
                        item1["CREATE_DATETIME"]=str(datetime.now())
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
                        query3="insert into STG_TRN_DATA(" +cols + val
                        mycursor.execute(query3,v_list)
                        if mycursor.rowcount>0 and connection:               
                            mycursor.execute("DELETE FROM trn_data_history WHERE TRAN_SEQ_NO='{}'".format(TRANS_NO))
                            connection.commit()
                        else:
                            connection.rollback()
                        count_1=count_1+ mycursor.rowcount
                        connection.commit()
            return JsonResponse({"status": 201, "message": "Data Inserted and Records deleted: " f"{count_1}"})
        except Exception as error:
            return JsonResponse({"status": 500, "message": str(error)})
        except ValueError:
            return JsonResponse({"status": 500, "message": "error"})
        except IntegrityError:
            return JsonResponse({"status": 500, "message": "TRAN_SEQ_NO must be unique"})
        finally:
            mycursor.close()
            connection.close()


#Fetching the data from SYSTEM CONFIG table based on the TRN_TYPE and updated the record with new values:
@csrf_exempt            
def system_conf(request):
    if request.method == 'POST':
        try:
            json_object_list = json.loads(request.body)
            list1=[]
            list2=[]
            mycursor = connection.cursor()
            for json_object in json_object_list:
                keys=[]
                #Removing the NULL values columns from json input.
                for key in json_object:
                    if json_object[key]=="NULL" or json_object[key]=="":
                        keys.append(key)
                for k in keys:
                    json_object.pop(k)
                list2.append(json_object)
            for json_object in list2:
                AREF_1=json_object.get("AREF", None)
                TRNTYPE=json_object.get("TRN_TYPE", None)
                record = pd.read_sql("SELECT * FROM system_config WHERE TRN_TYPE='{}' AND AREF='{}'".format(TRNTYPE,AREF_1),connection)
                res_list=[]
                #Converting the Queryset into the json format
                for val in record.values:
                    count=0
                    l_dict={}
                    for col in record.columns:
                        l_dict[col]=val[count]
                        count=count+1
                    res_list.append(l_dict)
                rec1=res_list[0]
                remove=[]
                #Removing the same values columns from json input
                for key1 in json_object: 
                    val1=str(rec1[key1])
                    if json_object[key1]==val1:
                        remove.append(key1)
                for p in remove:
                    json_object.pop(p)
                #Assigning TRN_TYPE & AREF valuse
                json_object["AREF"]=AREF_1
                json_object["TRN_TYPE"]=TRNTYPE
                list1.append(json_object)
            count=0
            for json_object1 in list1:
                TRNTYPE=json_object1.get("TRN_TYPE")
                AREF_1=json_object1.get("AREF")
                json_object1.pop("TRN_TYPE")
                json_object1.pop("AREF")
                if len(json_object1)>0:
                    results = "update system_config set {} where TRN_TYPE='{}' AND AREF='{}'".format(', '.join('{}="{}"'.format(k,json_object1[k]) for k in json_object1),TRNTYPE,AREF_1)
                    mycursor.execute(results)
                    count=count+mycursor.rowcount
                    connection.commit()
            return JsonResponse({"status": 201, "message": "Records Updated:" f"{count}"})
        except Exception as error:
            return JsonResponse({"status": 500, "message": str(error)})
        except ValueError:
            return JsonResponse({"status": 500, "message": "error"})
        finally:
            connection.close()
            
            
#Retrieve UNIT_COST from ITEM_LOCATION with input parameters item and location
@csrf_exempt 
def get_cost_item_location(request):
    if request.method == 'POST':
        try:
            data = json.loads(request.body)
            data=data[0]
            key_list=[]
            for key in data:
              if isinstance(data[key], list):
                  if len(data[key])==0:
                      key_list.append(key)
              if data[key]=="" or data[key]=="NULL":
                  key_list.append(key)
            for key in key_list:
               data.pop(key)
            if "ITEM" in data and "LOCATION" in data:
                for key in data:
                    query="SELECT ITEM,LOCATION ,UNIT_COST FROM item_location WHERE "
                    if len(data[key])==1:
                        data[key]=(data[key])[0]
                        query=query+key+" in ("+str(data[key])+") AND "
                    else:
                        query=query+key+" in "+str(tuple(data[key]))+" AND "
                    query=query[:-4]+";"
                    #print(query)
                    result = pd.read_sql(query,connection)
                    res_list=[]
                    for val1 in result.values:
                        count=0
                        rec={}
                        for col in result.columns:
                            rec[col]=val1[count]
                            count=count+1
                        res_list.append(rec)
                    if len(res_list)==0:
                        return JsonResponse({"status": 500,"message":"Invalid input"})
                    else:
                        return JsonResponse(res_list, content_type="application/json",safe=False)    
            else:
                return JsonResponse({"status": 500,"message":"Invalid input"})
        except Exception as error:
            return JsonResponse({"status": 500, "message":str(error)})
        finally:
            connection.close()      
            
            
 
           
#Update and Retrieve UNIT_COST from ITEM_LOCATION with input parameters item and location new_cost, also update STG_TRN_DATA
@csrf_exempt
def cost_update_stg(request):
    if request.method == 'POST':
        try:
            data_list = json.loads(request.body)  
            mycursor=connection.cursor()
            update_count=0
            il_list=[]
            for data in data_list:
                key_list=[]
                for key in data:
                  if data[key]=="" or data[key]=="NULL":
                      key_list.append(key)
                #print(key_list)
                for key in key_list:
                   data.pop(key)
                data['UNIT_COST'] =Decimal(data['UNIT_COST'] )
                if data['UNIT_COST'] >0:
                    mycursor.execute('SELECT * FROM item_location WHERE ITEM={} AND LOCATION={};'.format(data['ITEM'],data['LOCATION']))
                    if mycursor.rowcount>0:
                        cost_rec = pd.read_sql('SELECT UNIT_COST,ITEM_SOH FROM item_location WHERE ITEM={} AND LOCATION={};'.format(data['ITEM'],data['LOCATION']),connection)    
                        cost_rec=(cost_rec.values)
                        l_counter=0
                        old_cost=Decimal(cost_rec[0][0])
                        if len(cost_rec)>0 and old_cost != data["UNIT_COST"]:                                            
                            qty=cost_rec[0][1]
                            cost=float(old_cost - data['UNIT_COST'] )
                            I_dict=dict()
                            #New TRAN_SEQ_NO
                            l_counter=l_counter+1
                            d= str(datetime.now()).replace('-',"").replace(':',"").replace(' ',"").replace('.',"")
                            unique_id=d+str(l_counter)+'UCC'
                            #insert into stg_trn_date
                            I_dict['TRAN_SEQ_NO']= unique_id
                            I_dict['PROCESS_IND']='N'
                            I_dict['TRN_DATE']=date.today()
                            I_dict['TRN_TYPE']='VAR'
                            I_dict['QTY']=qty
                            I_dict['UNIT_COST']=cost
                            I_dict['TOTAL_COST']=(cost*qty)
                            I_dict['CREATE_DATETIME']=datetime.today()
                            I_dict['CREATE_ID']=data['CREATE_ID']
                            I_dict["LOCATION"]=data['LOCATION']
                            I_dict["ITEM"]=data['ITEM']
                            I_dict["AREF"]=1
                            I_dict["REV_NO"]=1
                            #inserting into STG_TRN_DATA
                            cols=",".join(map(str, I_dict.keys()))                    
                            v_list=[]
                            val=') VALUES('
                            for v in I_dict.values():
                                if v== None:
                                    val=val+'NULL,'
                                else:
                                    v_list.append(v)
                                    val=val+""
                                    val=val+'%s,'
                            val=val[:-1]+')'
                            #UPDATING NEW COST  IN ITEM_LOCATION
                            mycursor.execute("update item_location set UNIT_COST={} where ITEM={} AND LOCATION={}".format(data['UNIT_COST'],data['ITEM'],data['LOCATION']))
                            #INSERTING INTO VARIANCE IN STG_TRN_DATA
                            if mycursor.rowcount>0:
                                if qty!=0:
                                    i_query="INSERT INTO STG_TRN_DATA ("+cols+val
                                    mycursor.execute(i_query,v_list)
                                    connection.commit()
                                    update_count=update_count+1
                                    il_list.extend([[data['ITEM'],data['LOCATION']]])
                            else:
                               connection.rollback()  
            if update_count>0:
                return JsonResponse({"status": 200,"message": f"Records updated : {update_count} "})
            else:
                return JsonResponse({"status": 500,"message": f"Records updated : {update_count} "})
        except Exception as error:
            return JsonResponse({"status": 500, "message":str(error)})
        #except ValueError:
            #return JsonResponse({"status": 500, "message": "error"})
        finally:
            mycursor.close()
            connection.close()

            
#location validation from LOCATION table: 
@csrf_exempt            
def location_valid(request):
    if request.method == 'POST':
        try:
            data = json.loads(request.body)
            data=data[0]
            if len(data)==0:
                data["LOCATION"]=""
            key_list=[]
            key=(list(data.keys()))[0]
            for key in data:
                if isinstance(data[key], list):
                    if len(data[key])==0:
                        key_list.append(key)
                if data[key]=="" or data[key]=="NULL":
                    key_list.append(key)
            if  len(data[key])==0:
                #Display all if no input
                result = pd.read_sql("SELECT LOCATION,LOCATION_NAME FROM location",connection)
                res_list=[]
                for val1 in result.values:
                    count=0
                    rec={}
                    for col in result.columns:
                        rec[col]=val1[count]
                        count=count+1
                    res_list.append(rec)
                if len(res_list)==0:
                    return JsonResponse({"Error ":"No Data Found"})
                else:
                    return JsonResponse(res_list, content_type="application/json",safe=False)
            else:
                if len(data.values())>0:
                    #Display if valid or invalid with input
                    query= "SELECT * FROM location WHERE "
                    
                    if len(data[key])==1:
                        data[key]=(data[key])[0]
                        query=query+key+" in ("+str(data[key])+") AND "
                    else:
                        query=query+key+" in "+str(tuple(data[key]))+" AND "
                    query=query[:-4]+";"
                    result = pd.read_sql(query,connection)
                    res_list=[]
                    for val1 in result.values:
                        count=0
                        rec={}
                        for col in result.columns:
                            rec[col]=val1[count]
                            count=count+1
                        res_list.append(rec)
                    if len(res_list)==0:
                        return JsonResponse({"status": 500,"message":"Invalid input"})
                    else:
                        return JsonResponse(res_list, content_type="application/json",safe=False)                 
        except Exception as error:
            return JsonResponse({"status": 500, "message":str(error)})
        finally:
            connection.close()
            
            
#currency validation from CURRENCY table.
@csrf_exempt 
def currency_valid(request):
    if request.method == 'POST':
        try:
            data = json.loads(request.body)
            data=data[0]
            key_list=[]
            mycursor = connection.cursor()
            key=(list(data.keys()))[0]
            for key_d in data:
              if isinstance(data[key_d], list):
                  if len(data[key_d])==0:
                      key_list.append(key_d)
              if data[key_d]=="" or data[key_d]=="NULL":
                  key_list.append(key_d)
            for key_l in key_list:
               data.pop(key_l)
            if  len(data)==0:             
                result = pd.read_sql("SELECT CURRENCY,CURRENCY_DESC FROM currency",connection)
                res_list=[]
                for val1 in result.values:
                    count=0
                    rec={}
                    for col in result.columns:
                        rec[col]=val1[count]
                        count=count+1
                    res_list.append(rec)
                if len(res_list)==0:
                    return JsonResponse({"Error ":"No Data Found"})
                else:
                    return JsonResponse(res_list, content_type="application/json",safe=False)
            else:
                 if len(data.values())>0:
                    #Display if valid or invalid with input
                    query= "SELECT * FROM currency WHERE "
                    
                    if len(data[key])==1:
                        data[key]=(data[key])[0]
                        query=query+key+" in ('"+str(data[key])+"') AND "
                    else:
                        query=query+key+" in "+str(tuple(data[key]))+" AND "
                    query=query[:-4]+";"
                    result = pd.read_sql(query,connection)
                    res_list=[]
                    for val1 in result.values:
                        count=0
                        rec={}
                        for col in result.columns:
                            rec[col]=val1[count]
                            count=count+1
                        res_list.append(rec)
                    if len(res_list)==0:
                        return JsonResponse({"status": 500,"message":"Invalid input"})
                    else:
                        return JsonResponse(res_list, content_type="application/json",safe=False) 
        except Exception as error:
            return JsonResponse({"status": 500, "message":str(error)})
        finally:
            mycursor.close()
            connection.close()
            
            
#item and location validation from ITEM_LOCATION table.
@csrf_exempt 
def item_location_valid(request):
    if request.method == 'POST':
        try:
            data = json.loads(request.body)
            data=data[0]
            key_list=[]
            mycursor = connection.cursor()
            for key in data:
              if isinstance(data[key], list):
                  if len(data[key])==0:
                      key_list.append(key)
              if data[key]=="" or data[key]=="NULL":
                  key_list.append(key)
                  
            for key in key_list:
               data.pop(key)
            if len(data)==0:                
                my_data = pd.read_sql("SELECT IT.ITEM,IT.ITEM_DESC,L.LOCATION,L.LOCATION_NAME FROM item_dtl IT,location L ,item_location IL WHERE IL.ITEM=IT.ITEM AND IL.LOCATION=L.LOCATION;",connection)
                res_list=[]                
                for val in my_data.values:
                    count=0
                    l_dict={}
                    for col in my_data.columns:
                        l_dict[col]=val[count]
                        count=count+1
                    res_list.append(l_dict)
                return JsonResponse(res_list, content_type="application/json",safe=False)
            else:
                query="SELECT IT.ITEM,IT.ITEM_DESC,L.LOCATION,L.LOCATION_NAME FROM item_dtl IT,location L ,item_location IL WHERE IL.ITEM=IT.ITEM AND IL.LOCATION=L.LOCATION AND "
                for key in data:
                    if len(data[key])==1:
                           data[key]=(data[key])[0]                           
                           query=query+"IL."+str(key)+" in ("+str(data[key])+") AND "
                    else:
                        query=query+"IL."+str(key)+" in "+str(tuple(data[key]))+" AND "
                query=query[:-4]+";"
                result=pd.read_sql(query,connection)
                res_list=[]                
                for val in result.values:
                    count=0
                    l_dict={}
                    for col in result.columns:
                        l_dict[col]=val[count]
                        count=count+1
                    res_list.append(l_dict)
                if len(res_list)==0:
                    return JsonResponse({"status": 500, "message": "No Data Found"})
                else:
                    return JsonResponse(res_list, content_type="application/json",safe=False) 
        except Exception as error:
            return JsonResponse({"status": 500, "message":str(error)})
        finally:
            mycursor.close()
            connection.close()


# "ITEM","ITEM_DESC","HIER2","HIER1","HIER3" validation from lov_item_dtl . 
@csrf_exempt
def lov_item_dtl(request):
    if request.method == 'POST':
        try:
           data = json.loads(request.body)
           data=data[0]
           key_list=[]
           #mycursor = connection.cursor()
           #deleting empty input
           for key in data:
               if isinstance(data[key], list):
                   if len(data[key])==0:
                       key_list.append(key)
               if data[key]=="" or data[key]=="NULL":
                   key_list.append(key)
           for key in key_list:
                data.pop(key)
           if  len(data)==0:
               result = pd.read_sql("SELECT ID.ITEM, ID.ITEM_DESC, ID.HIER1, DP.HIER1_DESC, ID.HIER2, C.HIER2_DESC, ID.HIER3, SC.HIER3_DESC FROM item_dtl ID, hier1 DP, hier2 C, hier3 SC WHERE DP.HIER1=ID.HIER1 AND C.HIER2=ID.HIER2 AND SC.HIER3=ID.HIER3 ORDER BY cast(ID.HIER1 as unsigned) ",connection)#
               res_list=[]                
               for val in result.values:
                   count=0
                   l_dict={}
                   for col in result.columns:
                       l_dict[col]=val[count]
                       count=count+1
                   res_list.append(l_dict)
               if len(res_list)==0:
                   return JsonResponse({"Error ":"No Data Found"})
               return JsonResponse(res_list, content_type="application/json",safe=False)
           else:
               query="SELECT ITEM,ITEM_DESC,HIER1,HIER2,HIER3 FROM item_dtl WHERE " 
               for key in data:
                   if len(data[key])==1:
                       data[key]=(data[key])[0]
                       query=query+str(key)+" in ("+str(data[key])+") AND "
                   else:
                       query=query+str(key)+" in "+str(tuple(data[key]))+" AND "
               query=query[:-4]+" order by cast(hier1 as unsigned);"
               result=pd.read_sql(query,connection)
               res_list=[]                
               for val in result.values:
                   count=0
                   l_dict={}
                   for col in result.columns:
                       l_dict[col]=val[count]
                       count=count+1
                   res_list.append(l_dict)
               if len(res_list)==0:
                   return JsonResponse({"status": 500, "message": "No Data Found"})
               else:
                   return JsonResponse(res_list, content_type="application/json",safe=False)          
        except Exception as error:
           return JsonResponse({"status": 500, "message": str(error)})
        finally:
             connection.close()


#Fetching the data from SYSTEM_CONFIG based on the input parameters:
@csrf_exempt            
def system_config_table(request):
    if request.method == 'POST':
        try:
            json_object = json.loads(request.body)
            json_object=json_object[0]
            keys=[]
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
                query="SELECT SC.*,TTD.TRN_NAME FROM system_config SC,trn_type_dtl TTD WHERE SC.TRN_TYPE=TTD.TRN_TYPE AND SC.AREF=TTD.AREF AND {}".format(' '.join('SC.{} IN ({}) AND'.format(k,str(json_object[k])[1:-1]) for k in json_object))
            else:
                query="SELECT SC.*,TTD.TRN_NAME FROM system_config SC,trn_type_dtl TTD WHERE SC.TRN_TYPE=TTD.TRN_TYPE AND SC.AREF=TTD.AREF AND {}".format(' '.join('SC.{} LIKE "%{}%" AND'.format(k,json_object[k]) for k in json_object))
            if len(json_object)==0:
                query=query[:-4]+';'
                results55=pd.read_sql(query,connection)
            else:
                query=query[:-4]+';'
                results55=pd.read_sql(query,connection)   
            res_list=[]
            rec={}
            results55 =  results55.replace(np.NaN, None, regex=True)
            for val2 in results55.values:
                count=0
                for col4 in results55.columns:
                    rec[col4]=val2[count]
                    count=count+1
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


#fetch data from ITEM_LOCATION table.
@csrf_exempt
def fetch_item_location(request):
    if request.method == 'POST':
        try:    
            data=json.loads(request.body)
            data=data[0]
            r_list=[]
            query="SELECT IT.ITEM,IT.ITEM_DESC,L.LOCATION,L.LOCATION_NAME,IL.UNIT_COST,IL.ITEM_SOH FROM item_dtl IT,location L ,item_location IL WHERE IL.ITEM=IT.ITEM AND IL.LOCATION=L.LOCATION "
            for key in data:
               if isinstance(data[key], list):
                   if len(data[key])==0:
                       r_list.append(key)
               if data[key]=="" or data[key]=="NULL":
                   r_list.append(key)
            for key in r_list:
                data.pop(key)
            if len(data)>0:
                for key in data:
                    if key=="LOCATION" or key=="ITEM":
                        if len(data[key])==1:
                            data[key]=(data[key])[0]
                            query=query+" AND IL."+key+" in ('"+str(data[key])+"')"
                        else:
                            query=query+" AND IL."+key+" in "+str(tuple(data[key]))
                            #query=query+"AND IL."+str(key)+"="+str(data[key][0])
                    else:
                        if len(data[key])==1:
                            data[key]=(data[key])[0]
                            query=query+" AND IT."+key+" in ('"+str(data[key])+"') "
                        else:
                            query=query+" AND IT."+key+" in "+str(tuple(data[key]))
                        #query=query+"AND IT."+str(key)+"="+str(data[key][0])
                query=query+";"
            else:
                query=query+";"
            my_data = pd.read_sql(query,connection)
            res_list=[]                
            for val in my_data.values:
                count=0
                l_dict={}
                for col in my_data.columns:
                    l_dict[col]=val[count]
                    count=count+1
                res_list.append(l_dict)
            if len(res_list)>0:
                return JsonResponse(res_list, content_type="application/json",safe=False)
            else:
                return JsonResponse({"status": 500, "message": "No Data Found"})
        except Exception as error:
            return JsonResponse({"status": 500, "message":str(error)})
        finally:
            connection.close()