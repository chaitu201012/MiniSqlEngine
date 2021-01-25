import os
import csv
import re
import logging
import sys
import sqlparse
from collections import OrderedDict
import copy



###############################global variables#################################
tables_info=OrderedDict() 
#FD="./files/"
FD=""
database=OrderedDict()
list_dict=[]
##############################################################


def metadataTables(metaDataFile):
    try:
        flag=0
        table_name=""
        with open(metaDataFile) as file:   
            data = file.readlines() 
        for line in data:
            value=line.strip()
            #print(value)
            if value == "<begin_table>":
                flag=1
            elif value=="<end_table>":
                flag=0
            elif flag==1:
                table_name=value
                tables_info[table_name.lower()]=[]
                flag=2
            elif flag==2:
                tables_info[table_name].append(value.lower())
    except Exception as e:
        print("Error opening the meta file "+str(e))
        exit()


def getQuery(query):
    if query[-1][-1]!=";":
        print("Invalid  query no semicolon at the end ")
        exit()
    parsed = sqlparse.parse(query)
    stmt = parsed[0]
    queryTokens=[]
    for i in stmt.tokens:
        if str(i)!=" ":
            queryTokens.append(str(i).lower())
    q=[]
    for i in queryTokens:
        #print(type(i))
        q.append(i.replace(";", ""))
    return q

def getCols(query):
    index1=query.index("from")
    final_cols=[]
    cols=[]
    for i in range(1,index1):
        final_cols.extend(query[i].split(","))
    for i in final_cols:
        cols.append(i.replace(" ",""))
    return cols
    '''
    columns=query.split(",")
    final_columns=[]
    for i in columns:
        final_columns.append(i.lower())
    return final_columns
    '''

def getTables(query):
    index=query.index("from")
    flag=False

    tables=query[index+1].split(",")
    final=[]
    for i in tables:
        final.append(i.replace(" ",""))
    return final


def checkCorrectness(queryTokens,cols,tables):
    try:
        if "select" not in queryTokens:
            print("Invalid query, select should be present")
            exit()
        if "from" not in queryTokens:
            print("Invalid query, from should be present")
            exit()
        index=queryTokens.index("from")
        flag=False

        for i in tables:
            #print(i)
            if not i in tables_info:
                print("Table "+i+" is not present in the metadata given")
                exit()
        '''
        for i in cols:
            for key,values in tables_info.items():
                if not i in values:
                    print(" Given columns "+i +" is not present in metadata given")
                    exit()
        '''


    except Exception as e:
        print(str(e))
        exit()

def getDataDict(query,columns,tables):
    try:

        data_dict=OrderedDict() 
    
        for k in tables:
            
            cols=tables_info[k]
            for i in cols:
                key=""
                key=i
                #print(k)
                data_dict[i]=[]
        for k in tables:
            table_name=FD+k+".csv"
            data=[]
            with open(table_name, mode='r') as csv_file:
                csv_reader = csv.reader(csv_file)
                line_count = 0
                for row in csv_reader:
                    data.append(row)
            cols=tables_info[k]
            for i in range(0,len(cols)):
                for k in range(0,len(data)):
                    data_dict[cols[i]].append(data[k][i])
        
        return data_dict
    except Exception as e:
        print("Error opening the meta file "+str(e))
        exit()

def simpleProject(query,columns,tables,whereTable):

    data_dict=getDataDict(query,columns,tables)

   
    #print(" ".join(map(str,query)))
    #print("\n")
    #print("output:")
    if "*" in columns:
        tablePrinting=[]
        for i in tables:
            for attribute in tables_info[i]:
                tablePrinting.append(i+"."+str(attribute))
        print(",".join(map(str,tablePrinting)))
        #print(','.join([tables[0]+"."+str(attribute) for attribute in tables_info[tables[0]]]))
        data=[]

        for i in whereTable["records"]:
            print(",".join(map(str,i)))

        '''
        for key,values in data_dict.items():
            data.append(data_dict[key])
        for i in range(0,len(data[0])):
            temp=[]
            for j in data:
                temp.append(j[i])
            print(",".join(map(str,temp)))
        '''
            
    else:
        tablePrinting=[]
        for attribute in columns:
            for i in tables:
                if (attribute.lower()!="distinct" and attribute.lower() in tables_info[i]):
                    tablePrinting.append(i+"."+str(attribute))
        print(",".join(map(str,tablePrinting)))
        cols_index=[]
        count=0
        for i in columns:
            for key,values in tables_info.items():
                #print(values)
                if not (i in values or i in aggregateDetection(query,columns)):
                    count+=1
        #print(count)
        if count==len(tables_info.items()):
            print(" Given columns "+i +" is not present in metadata given")
            exit()
        #print(whereTable)
        for j in columns:
            for i in range(len(whereTable["columns"])):
                if j.lower()==whereTable["columns"][i]:
                    cols_index.append(i)
        for k in whereTable["records"]:
            record=[]
            for i in cols_index:
                record.append(k[i])
            print(",".join(map(str,record)))
        '''
        data=[]
        for key,values in data_dict.items():
            if key in columns:
                data.append(data_dict[key])
        for i in range(0,len(data[0])):
            temp=[]
            for j in data:
                temp.append(j[i])
            print(",".join(map(str,temp)))
        '''
    

def finalDatabaseMap(query,columns,tables):
    
    
    #schema=OrderedDict()

    table_count=0
    for i in tables:
        table_count=table_count+1
        database[i]=OrderedDict()
    #print(database)
    

    for i in tables:
        new_list=[]
        new_list.append(i)
        list_dict.append(getDataDict(query,columns,new_list))
    #print(list_dict)
    #print("")
    keys_list=[]
    cols_list=[]
    for i in list_dict:
        keys=[]
        cols=[]
        for key,value in i.items():
            keys.append(key)
            cols.append(value)
        
        keys_list.append(keys)
        cols_list.append(cols)
    #print(keys_list)
    #print(cols_list)
    new_matrix=[]
    for m in cols_list:
        new_matrix.append([[row[i] for row in m] for i in range(len(m[0]))])
        #new_matrix=new_matrix+(map(list, zip(*m)))

    #print("-----------------new matrix--------------------------")
    #print(new_matrix)
    count=0
    for key,values in database.items():
        database[key]["records"]=new_matrix[count]
        count=count+1
    #print("-------------data base--------------------------")
    #print(database)
    return database

def crossProduct(tables,database,cross_table,table_no):

    if len(tables) == table_no: # when all tables are dealt with then table_no becomes number of tables
        return 
    if len(cross_table["records"]) == 0:
        cross_table["records"]=database[tables[table_no]]["records"] 
    else:
        temp_list=[]
        for row in database[tables[table_no]]["records"]:
            temp_list.extend(record+row for record in cross_table["records"])
        cross_table["records"]=temp_list

    crossProduct(tables,database,cross_table,table_no+1)

    return cross_table

def getDomains(tables):
    attrs=[]
    for key,values in tables_info.items():
        if key in tables:
            attrs.extend(values)
    return attrs

def join_tables(query,columns,tables):

    database=finalDatabaseMap(query,columns,tables)

    domains=getDomains(tables)

    cross_table=OrderedDict()
    cross_table["columns"]=domains
    cross_table["records"]=[]

    table_no=0

    #print("----------------------------Cross Join--------------------------------------")

    crossed_table=crossProduct(tables,database,cross_table,table_no)

    return crossed_table
    
def aggregateDetection(query,columns):
    cols=[]
    flag=False
    for i in columns:
        if (i[:3].lower()=="max" or i[:3].lower()=="min" or i[:3].lower()=="sum" or i[:3].lower()=="avg" or i[:5].lower()=="count") :
            cols.append(i.lower())
            flag=True
    return cols

def filterCondition(crossed_table,columns,tables,query):
    #where a>90 and/OR b>=90  or where A=B or  <,>, <= ,>= , = comparision might be between cols also 
    
    operator=["=",">=","<=",">","<"]
    operators=["@","#","$","^","&"]
    operators_map={"&":"=","@":">=","#":"<=","$":">","^":"<"}
    query=query[query.index("from")+2]
    query=query.replace("(","")
    query=query.replace(")","")
    
    query=query[5:]

    flagTwo=False

    query=query.strip()
    query=query.replace('>=',"@")
    query=query.replace('<=',"#")
    query=query.replace('>',"$")
    query=query.replace('<',"^")
    query=query.replace('=',"&")
    #query=query.replace(" ","")
    c=query.split()
    condition=[]

    orTag=False
    andTag=False
    if (" and "  in query) or ("and" in query):
        andTag=True
        c=query.split("and")
        flagTwo=True
    if (" or ") in query or ("or" in query):
        orTag=True
        c=query.split("or")
        flagTwo=True
 
    #print(condTag)
    for i in c:
        condition.append(i.replace(" ",""))
    

    
    #condition=query.split()
    #print(condition)
    
    operand1=[]
    operand2=[]
    given_operator={}
    if flagTwo:
        x=False
        y=False  #flags to check if two operators are found if found we can break the loop
        for o in operators: 
            if o in condition[0]:
                #print(o)
                operand1.extend(condition[0].split(o))
                given_operator["first"]=operators_map[o]
                #print(o+"  ".join(map(str,given_operator)))
                x=True
            if o in condition[1]:
                #print(o)
                operand2.extend(condition[1].split(o))
                given_operator["second"]=operators_map[o]
                #print(o+"  ".join(map(str,given_operator)))
                y=True
                if x and y:
                    break
        if not(x and y):
            print("operator is not defined only the following operator were defined  "+" ".join(map(str, operators)))
    if not flagTwo:
        condition=["".join(map(str,condition))]
        #print(condition)
        for o in operators:
            if o in condition[0]:
                operand1.extend(condition[0].split(o))
                given_operator["first"]=operators_map[o]
                break
        
    
    count=0
    for i in operand1:
        if i.isdigit():
            count+=1
    if count==len(operand1):
        print("Invalid condition in where all are digits")
        exit()
    count=0
    for i in operand2:
        if i.isdigit():
            count+=1
    if count==len(operand2):
        print("Invalid condition in where all are digits")
        exit()
    count=0
    reverseFlag=False
    for i in range(len(operand1)-1):
        if count==0 and (operand1[i].isdigit()) and (not operand1[i+1].isdigit()):
            reverseFlag=True
            break
    if reverseFlag:
        operand1=operand1[::-1]

    count=0
    reverseFlag=False
    for i in range(len(operand2)-1):
        if count==0 and (operand2[i].isdigit()) and (not operand2[i+1].isdigit()):
            reverseFlag=True
            break
    if reverseFlag:
        operand2=operand2[::-1]

    #print(operand1)
    #print(operand2)

        


    #print(given_operator)
    #print(crossed_table)

    
    whereTable=OrderedDict()
    whereTable["columns"]=copy.deepcopy(crossed_table["columns"])
    whereTable["records"]=[]

    ######################################if two condition with and/or  given #####################################
    if flagTwo:
        index1=crossed_table["columns"].index(operand1[0].lower())
        index2=crossed_table["columns"].index(operand2[0].lower())
        index3=0
        index4=0
        if not operand1[1].isdigit():
            index3=crossed_table["columns"].index(operand1[1].lower())
        if not operand2[1].isdigit():
            index4=crossed_table["columns"].index(operand2[1].lower())
        flag1=False
        flag2=False
        for i in crossed_table["records"]:
               # these two flags are usefule in case of "And or OR" conditions whether we need to include the row based on the flags.
            #if both true then we incude in case of AND cond
            if given_operator["first"] == ">=":
                if operand1[1].isdigit() and (int(i[index1]) >= int(operand1[1])):
                    #whereTable["records"].append(i)
                    flag1=True
                if (not(operand1[1].isdigit())) and (int(i[index1]) >= int(i[index3])):
                    #whereTable["records"].append(i)
                    flag1=True
            if given_operator["first"] == "<=":
                if operand1[1].isdigit() and (int(i[index1]) <= int(operand1[1])):
                    #whereTable["records"].append(i)
                    flag1=True
                if (not(operand1[1].isdigit())) and (int(i[index1]) <= int(i[index3])):
                    #whereTable["records"].append(i)
                    flag1=True
            if given_operator["first"] == "<":
                if operand1[1].isdigit() and (int(i[index1]) < int(operand1[1])):
                    #whereTable["records"].append(i)
                    flag1=True
                if (not(operand1[1].isdigit())) and (int(i[index1]) < int(i[index3])):
                    #whereTable["records"].append(i)
                    flag1=True
            if given_operator["first"] == ">":
                if operand1[1].isdigit() and (int(i[index1]) > int(operand1[1])):
                    #whereTable["records"].append(i)
                    flag1=True
                if (not(operand1[1].isdigit())) and (int(i[index1]) > int(i[index3])):
                    #whereTable["records"].append(i)
                    flag1=True
            if given_operator["first"] == "=":
                #print(i[index1])
                #print(int(operand1[1]))
                if operand1[1].isdigit() and (int(i[index1]) == int(operand1[1])):
                    #whereTable["records"].append(i)
                    #print("entered here")
                    flag1=True
                if (not(operand1[1].isdigit())) and (int(i[index1]) == int(i[index3])):
                    #whereTable["records"].append(i)
                    flag1=True

            ############################## For second condition in where clause ########################################

            if given_operator["second"] == ">=":
                if operand2[1].isdigit() and (int(i[index2]) >= int(operand2[1])):
                    #whereTable["records"].append(i)
                    flag2=True
                if (not(operand2[1].isdigit())) and (int(i[index2]) >= int(i[index4])):
                    #whereTable["records"].append(i)
                    flag2=True
            if given_operator["second"] == "<=":
                if operand2[1].isdigit() and (int(i[index2]) <= int(operand2[1])):
                    #whereTable["records"].append(i)
                    flag2=True
                if (not(operand2[1].isdigit())) and (int(i[index2]) <= int(i[index4])):
                    #whereTable["records"].append(i)
                    flag2=True
            if given_operator["second"] == "<":
                if operand2[1].isdigit() and (int(i[index2]) < int(operand2[1])):
                    #whereTable["records"].append(i)
                    flag2=True
                if (not(operand2[1].isdigit())) and (int(i[index2]) < int(i[index4])):
                    #whereTable["records"].append(i)
                    flag2=True
            if given_operator["second"] == ">":
                if operand2[1].isdigit() and (int(i[index2]) > int(operand2[1])):
                    #whereTable["records"].append(i)
                    flag2=True
                if (not(operand2[1].isdigit())) and (int(i[index2]) > int(i[index4])):
                    #whereTable["records"].append(i)
                    flag2=True
            if given_operator["second"] == "=":
                if operand2[1].isdigit() and (int(i[index2]) == int(operand2[1])):
                    #whereTable["records"].append(i)
                    flag2=True
                if (not(operand2[1].isdigit())) and (int(i[index2]) == int(i[index4])):
                    #whereTable["records"].append(i)
                    flag2=True
            
            if andTag and (flag1 and flag2):  
                #print("and tag")
                whereTable["records"].append(i)
                flag1=False
                flag2=False
            
            if orTag and (flag1 or flag2):
                #print("or tag")
                #print("entered here")
                whereTable["records"].append(i)
                flag1=False
                flag2=False

            
    if not flagTwo:
        index1=crossed_table["columns"].index(operand1[0].lower())
        index3=0
        if not operand1[1].isdigit():
            index3=crossed_table["columns"].index(operand1[1].lower())

        for i in crossed_table["records"]:
            flag1=False
            # these two flags are usefule in case of "And or OR" conditions whether we need to include the row based on the flags.
            #if both true then we incude in case of AND cond
            if given_operator["first"] == ">=":
                if operand1[1].isdigit() and (int(i[index1]) >= int(operand1[1])):
                    #whereTable["records"].append(i)
                    flag1=True
                if (not(operand1[1].isdigit())) and (int(i[index1]) >= int(i[index3])):
                    #whereTable["records"].append(i)
                    flag1=True
            if given_operator["first"] == "<=":
                if operand1[1].isdigit() and (int(i[index1]) <= int(operand1[1])):
                    #whereTable["records"].append(i)
                    flag1=True
                if (not(operand1[1].isdigit())) and (int(i[index1]) <= int(i[index3])):
                    #whereTable["records"].append(i)
                    flag1=True
            if given_operator["first"] == "<":
                if operand1[1].isdigit() and (int(i[index1]) < int(operand1[1])):
                    #whereTable["records"].append(i)
                    flag1=True
                if (not(operand1[1].isdigit())) and (int(i[index1]) < int(i[index3])):
                    #whereTable["records"].append(i)
                    flag1=True
            if given_operator["first"] == ">":
                if operand1[1].isdigit() and (int(i[index1]) > int(operand1[1])):
                    #whereTable["records"].append(i)
                    flag1=True
                if (not(operand1[1].isdigit())) and (int(i[index1]) > int(i[index3])):
                    #whereTable["records"].append(i)
                    flag1=True
            if given_operator["first"] == "=":
                if operand1[1].isdigit() and (int(i[index1]) == int(operand1[1])):
                    #whereTable["records"].append(i)
                    flag1=True
                if (not(operand1[1].isdigit())) and (int(i[index1]) == int(i[index3])):
                    #whereTable["records"].append(i)
                    flag1=True   
            if flag1:
                whereTable["records"].append(i)
                flag1=False


    #print("---------------------------------where conditioned table-------------------------------")       
    #print(whereTable)    

    return whereTable


def simpleAggregate(whereTable,query,columns,tables,aggrFuncWithCol):
    #print(aggrFuncWithCol)
    index=0
    aggregate_values=[]
    for n in range(len(aggrFuncWithCol)):
        args=aggrFuncWithCol[n].split("(")
        func=args[0].lower()
        col=args[1][:-1].lower()
        
        if not col == "*":
            index=whereTable["columns"].index(col)
        
        if func=="max":
            value=-9999999999
            for i in range(len(whereTable["records"])):
                data=int(whereTable["records"][i][index])
                value=value if value >= data else data 
            if value!=-9999999999:
                aggregate_values.append(value)
            else:
                aggregate_values.append("")
            value=-9999999999
        if func=="min":
            value=9999999999
            for i in range(len(whereTable["records"])):
                data=int(whereTable["records"][i][index])
                value=value if value <= data else data 
            if value!=9999999999:
                aggregate_values.append(value)
            else:
                aggregate_values.append("")
            value=9999999999
        if func=="sum":
            value=0
            for i in range(len(whereTable["records"])):
                data=int(whereTable["records"][i][index])
                value=value+data
            aggregate_values.append(value)
            value=0
        if func=="avg":
            value=0
            for i in range(len(whereTable["records"])):
                data=int(whereTable["records"][i][index])
                value=value+data
            val=value//len(whereTable["records"])
            aggregate_values.append(val)
            value=0
        
        if func=="count":
            aggregate_values.append(len(whereTable["records"]))
    
    return aggregate_values
        

def handleDistinct(whereTable,query,columns,tables,flagAggregate,aggrFuncWithCol):
    
    #print("Entered here")
    
    temp=OrderedDict()
    temp["columns"]=copy.deepcopy(whereTable["columns"])
    temp["records"]=[]
    seen=[]
    duplicates=[0]*len(whereTable["records"])     #  using dp to find visited duplicates
    index=0
    for i in range(len(whereTable["records"])):
        if duplicates[i]==0:
            temptuple=()
            for j in range(1,len(columns)):
                index=whereTable["columns"].index(columns[j].lower())
                data=int(whereTable["records"][i][index])
                temptuple=temptuple+(data,)


            flagseen=False
            for k in seen:
                if temptuple == k:
                    flagseen=True
                    duplicates[i]=1;
            if not flagseen:
                seen.append(temptuple)
    #print(duplicates)
    #print(len(whereTable["records"]))
    #print(len(duplicates))
    for l in range(len(duplicates)):
        if duplicates[l]==0:
            temp["records"].append(whereTable["records"][l])
    whereTable=copy.deepcopy(temp)
    return whereTable
                
    


def handleAggregate(whereTable,query,columns,tables,flagAggregate,aggrFuncWithCol):
    
    if flagAggregate:
        if len(columns)==1 and len(tables)==1:
            return simpleAggregate(whereTable,query,columns,tables,aggrFuncWithCol)


        if columns[0].lower()=="distinct":
            whereTable = handleDistinct(whereTable,query,columns,tables,flagAggregate,aggrFuncWithCol)
            return whereTable

def handleGroups(whereTable,query,columns,tables,flagAggregate,aggrFuncWithCol) :

    nonAggregateColumns=[]
    for i in columns:
        if i not in aggrFuncWithCol:
            nonAggregateColumns.append(i)
    
    index1=query.index("from") 
    index2=0
    groupedColumn=""
    for i in range(index1,len(query)):
        if query[i][:5].lower()=="group":
            index2=i
            break
    col=query[index2+1].lower()
    groupedColumn=col
    if groupedColumn.lower() not in nonAggregateColumns:
        print("grouped column must be present in the select columns")
        exit()
    
    grouped_table=OrderedDict()
    grouped_table["columns"]=whereTable["columns"]
    grouped_table["records"]=[]
    index1=whereTable["columns"].index(col)
    visited=[0]*len(whereTable["records"])
    for i in range(len(whereTable["records"])):
        temp=[]
        #print(visited)
        if visited.count(1)==len(whereTable["records"]):
            break
        if visited[i]==1:
            continue
        temp.append(whereTable["records"][i])
        visited[i]=1
        for j in range(i+1,len(whereTable["records"])):
            #print("j value"+str(j))
            #print(len(whereTable["records"]))
            
            if visited[j]==0 and (whereTable["records"][j][index1]==whereTable["records"][i][index1]):
                #print(" ith rocord")
                #print(whereTable["records"][i])
                #print(" jth record")
                #print(whereTable["records"][j])

                temp.append(whereTable["records"][j])
                visited[j]=1

                
        #print(temp)     
        grouped_table["records"].append(temp)
    
    #print(grouped_table)
    func=""
    for i in columns:
        func1=aggregateDetection(query,columns)
    grouped_table["columns"].extend(func1)
    
    #print(func1)
    for s in range(len(func1)):
        args=func1[s].split("(")
        func=args[0].lower()
        col=args[1][:args[1].index(")")].lower()
        
        if not col == "*":
            index=grouped_table["columns"].index(col)

        if func=="max":
            value=-9999999999
            for j in grouped_table["records"]:
                for i in j:
                    data=int(i[index])
                    value=value if value >= data else data 
                for k in j:
                    val=[value]
                    k.extend(val)
                value=-9999999999
        if func=="min":
            value=9999999999
            for j in grouped_table["records"]:
                for i in j:
                    data=int(i[index])
                    value=value if value <= data else data 
                for k in j:
                    val=[value]
                    k.extend(val)
                value=9999999999
        if func=="sum":
            value=0
            for j in grouped_table["records"]:
                for i in j:
                    data=int(i[index])
                    value=value + data 
                #print(j)
                #print("sum of the group earnings"+str(value))
                for k in j:
                    k.extend([value])
                value=0
        if func=="avg":
            value=0
            for j in grouped_table["records"]:
                for i in j:
                    data=int(i[index])
                    value=value + data 
                for k in j:
                    k.extend([value//len(j)])
                value=0
            #return 
        
        if func=="count":
            for j in grouped_table["records"]:
                #print(j)
                #print(len(j             ))
                for k in j:
                    k.extend([len(j)])

    return grouped_table,groupedColumn
    
    
def printTable(query,columns,tables,whereTable,grouped_table,flag_dict,flagAggregate,aggrFuncWithCol,groupedColumn):

    flagDistinct=False
    for i in aggrFuncWithCol:
        if i.lower()=="distinct":
            flagDistinct=True
            break

    if columns[0].lower()== "distinct":
        flagDistinct=True
    print(" ".join(map(str,query)))
    print("\noutput")
    #print(whereTable)

    if not(flag_dict["groupFlag"] or flag_dict["orderFlag"] or flagAggregate or flagDistinct):
        #print("entered here:1")
        #print(whereTable)
        if len(tables)>=1:
            simpleProject(query,columns,tables,whereTable)
            exit()

    # another case where flag is present and no flags  need to code 
    if not(flag_dict["groupFlag"] or flag_dict["orderFlag"]) and flagAggregate:
        #print("entered here:2")
        if len(tables)>=1:
            printable=[]
            #for i in aggrFuncWithCol:
                #print(i)
            #print(aggrFuncWithCol)
            #print(','.join([tables[0]+str(attribute) for attribute in columns]))
            noGroupBy=False
            for i in columns:
                for key,values in tables_info.items():
                    if i.lower() in values:
                        noGroupBy=True
                        break
            if noGroupBy:
                print("Groupby should be present for the column present along with aggregate functions")
                exit()
            #print(whereTable)
            # print(aggrFuncWithCol)
            value=simpleAggregate(whereTable,query,columns,tables,aggrFuncWithCol)
            
                #print(" ".join(map(str,query)))
                #print("")
                #print("\noutput")
            print(",".join(map(str,aggrFuncWithCol)))
            print(",".join(map(str,value)))
            exit()

    

        #print(whereTable)
    if flag_dict["groupFlag"]:
        #print("entered here:4")
        print_columnslist=[]
        col_index=[]
        for i in columns:
            for key,values in tables_info.items():
                if i.lower() in values:
                    col_index.append(grouped_table["columns"].index(i.lower()))
                    print_columnslist.append(key+"."+i)
                #if i.lower() in grouped_table["columns"]:
                #    col_index.append(grouped_table["columns"].index(i.lower()))
                
        print_columnslist.extend(aggrFuncWithCol)  

        print(",".join(map(str,print_columnslist)))
        

        for i in aggrFuncWithCol:
            if i.lower() in grouped_table["columns"]:
                col_index.append(grouped_table["columns"].index(i.lower()))


        #print(len(grouped_table["records"]))
        if not flag_dict["orderFlag"]:
            for i in grouped_table["records"]:
                record=[]
                for j in i:
                    for k in col_index:
                        record.append(j[k])
                    break
                print(",".join(map(str,record)))
            exit()
        if flag_dict["orderFlag"]:
            #print("order group")
            #print(grouped_table)
            orderTable=[]
            for i in grouped_table["records"]:
                record=[]
                for j in i:
                    for k in col_index:
                        record.append(j[k])
                    break
                orderTable.append(record)
                #print(",".join(map(str,record)))
            ind=0
            for i in query:
                if i[:5].lower()=="order":
                    ind=query.index(i)
                    break
            ascdesc=query[ind+1].split()
            #print(query[ind+1])
            particular_order=""
            orderColumn=ascdesc[0].lower()
            orderindex=columns.index(orderColumn.lower())
            if len(ascdesc)>=2:
                particular_order=ascdesc[1].lower()
            else:
                particular_order="asc"
            orderindex=0
            for i in range(len(print_columnslist)):
                if "." in print_columnslist[i]:
                    dotindex=print_columnslist[i].index(".")
                    if print_columnslist[i][dotindex+1].lower()==orderColumn.lower():
                        orderindex=i

            #orderindex=columns.index(orderColumn.lower())
            #print(orderindex)

            if particular_order=="asc":
                orderTable.sort(key = lambda x: int(x[orderindex])) 
            if particular_order=="desc":
                orderTable.sort(key = lambda x: int(x[orderindex]),reverse=True)

            for i in orderTable:
                print(",".join(map(str,i)))
            exit()

    if flagDistinct:
        #print("entered here:3")
        columnslist=[]
        columnslist.append("distinct")
        col_index=[]
        #print("Entered here")
        if "*" in columns:
            for key,values in tables_info.items():
                if key in tables:
                    columnslist.extend(values)
        else:
            columnslist=columns

        #print(columnslist)
        whereTable = handleDistinct(whereTable,query,columnslist,tables,flagAggregate,aggrFuncWithCol)
        #print(whereTable)
        #print(" ".join(map(str,query)))
        #print("\output")
         # printing the corresponding columns

        if not(flag_dict["groupFlag"] or flag_dict["orderFlag"]):
            
            print_columnslist=[]
            for i in columnslist:
                for key,values in tables_info.items():
                    if i.lower() in values:
                        col_index.append(whereTable["columns"].index(i.lower()))
                        print_columnslist.append(key+"."+i)
            #print(columns)
            #print(print_columnslist)
            #print(whereTable)
            print(",".join(map(str,print_columnslist)))
            for i in whereTable["records"]:
                record=[]
                for j in col_index:
                    record.append(i[j])
                print(",".join(map(str,record)))
            #print(",".join(map(str,print_columnslist)))
            exit()   
    if flag_dict["orderFlag"] and not(flag_dict["groupFlag"]):
        #print("entered here:5")
        orderTable=whereTable
        ind=0
        for i in query:
            if i[:5].lower()=="order":
                ind=query.index(i)
                break
        
        ascdesc=query[ind+1].split()
        #print(query[ind+1])
        particular_order=""
        orderColumn=ascdesc[0].lower()
        orderindex=orderTable["columns"].index(orderColumn.lower())
        if len(ascdesc)>=2:
            particular_order=ascdesc[1].lower()
        else:
            particular_order="asc"
            
        
        temporary=orderTable["records"]
        if particular_order=="asc":
            temporary.sort(key = lambda x: int(x[orderindex])) 
        if particular_order=="desc":
            temporary.sort(key = lambda x: int(x[orderindex]),reverse=True)

        printable_columns=[]
        col_index=[]
        for i in columns:
            for key,values in tables_info.items():
                if i.lower() in values:
                    col_index.append(orderTable["columns"].index(i.lower()))
                    printable_columns.append(key+"."+i)
        print(",".join(map(str,printable_columns)))
        for i in temporary:
            record=[]
            for j in col_index:
                record.append(i[j])

            print(",".join(map(str,record)))

    



def identifyQuery(query,columns,tables):
    crossed_table=""
    whereTable=""
    back_up_where=""
    grouped_table=""
    index1=query.index("from")
    groupedColumn=""
    flagAggregate=False
    aggrFuncWithCol=aggregateDetection(query,columns)
    #print(aggrFuncWithCol)
    if len(aggrFuncWithCol)>0:
        flagAggregate=True

    ############################ identifying where, groupby and orderby  is present or not ########################

    flag_dict={}
    flag_dict["whereFlag"]=False
    flag_dict["groupFlag"]=False
    flag_dict["orderFlag"]=False
    for i in range(index1,len(query)):
    
        if query[i][:5].lower()=="where":
            flag_dict["whereFlag"]=True
        if query[i][:5].lower()=="group":
            flag_dict["groupFlag"]=True
        if query[i][:5].lower()=="order":
            flag_dict["orderFlag"]=True
    #print(aggrFuncWithCol)

    #if len(tables)==1 :
    #    simpleProject(query,columns,tables)

    if len(tables)>=1:
        crossed_table=join_tables(query,columns,tables)
        whereTable=crossed_table
    

        
    if flag_dict["whereFlag"]:
        whereTable=filterCondition(crossed_table,columns,tables,query)

    #if flagAggregate:
    #    whereTable=handleAggregate(whereTable,query,columns,tables,flagAggregate,aggrFuncWithCol)  
    
    if flag_dict["groupFlag"]:
        back_up_where=whereTable
        grouped_table,groupedColumn=handleGroups(whereTable,query,columns,tables,flagAggregate,aggrFuncWithCol) 

    printTable(query,columns,tables,whereTable,grouped_table,flag_dict,flagAggregate,aggrFuncWithCol,groupedColumn)
    
    #print(whereTable)

if __name__ == "__main__":
    #print("This is the name of the program:", sys.argv[0]) 
    #print("Argument List:", str(sys.argv)) 
    #metaDataFile="./files/metadata.txt"
    metaDataFile=FD+"metadata.txt"
    metadataTables(metaDataFile)
    #print(tables_info)
    #print(tables_info.has_key("table3"))
    if (sys.argv[1])=="":
        print("You need to provide query ")
        exit()

    query=sys.argv[1]
    #print(query)
    ##query=(" ".join(map(str,sys.argv)))
    #print(query)
    
    #print(query)
    #everything will be in upper case 
    queryTokens=getQuery(query)  # second index  contains the columns of the tables seprated by columns
    #print(queryTokens)
   
    columns_present=getCols(queryTokens) # getting the columns of the query for further simplicity
    #print(columns_present)
    for i in columns_present:
        flagPresent=False
        for key,values in tables_info.items():
            if (i in values) or i.lower()=="distinct" or i=="*" or i[:5].lower()=="count" or i[:3].lower()=="max" or i[:3].lower()=="min" or i[:3].lower()=="sum" or i[:3].lower()=="avg":
                flagPresent=True
        if not flagPresent:
            print("column "+i+" is not present any of the table")
            exit()
    for i in columns_present:
        if i.lower()=="max(*)" or i.lower()=="min(*)" or i.lower()=="sum(*)" or i.lower()=="avd(*)" :
            print(" * can come only with count aggregate function")
            exit()

    tables=getTables(queryTokens)
    
    #print(tables)

    checkCorrectness(queryTokens,columns_present,tables) 

    identifyQuery(queryTokens,columns_present,tables)
