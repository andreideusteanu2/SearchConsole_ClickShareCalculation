# -*- coding: utf-8 -*-
"""
Created on Wed Sep 13 11:40:52 2017

@author: andrei.deusteanu
"""

def readPrepare_inputData(queriesFile,pathFiles):
     
    #input file might be a csv or an xls with multiple sheets
    fileType=queriesFile[queriesFile.find(".")+1:]
    if fileType=="csv":
        inputData=pd.read_csv(pathFiles+queriesFile)
    #for xls/x files all sheets come as collection of ordered dictionary
    elif fileType in ["xls","xlsx"]:
        Data=pd.read_excel(pathFiles+queriesFile,sheetname=None)
        inputData=pd.DataFrame()
        sheets=list(Data.keys())
        #consolidate all sheets in single Data Frame
        for sheet in sheets:
            inputData=inputData.append(Data[sheet])
        del(Data)
    
    #remove duplicate values
    inputData=inputData.drop_duplicates(subset=inputData.columns,keep="first")
    colNames=list(inputData.columns)
    #rename the columns for consistency
    inputData.rename(index=str,columns={colNames[0]:"Keyword",colNames[1]:"searchVolume",colNames[2]:"category_id"},inplace=True)
    #add the index as an unique query identifier
    #by default it's added at the end of the data frame
    #remove multiple spaces in keyword
    inputData["Keyword"]=inputData["Keyword"].apply(lambda x: " ".join(x.split(' ')))
    #count the number of words in keyword
    inputData["wordCount"]=inputData["Keyword"].str.count(" ")+1
    return inputData

def importData_byCountry(countryCode):
    from os import listdir

    sites=['https://www.emag.','https://m.emag.']
    for i,site in enumerate(sites):
        sites[i]=site+countryCode.lower()
    countryFullData=pd.DataFrame()
    bufData=pd.DataFrame()
    files=listdir(pathFiles+countryCode)
    for file in files:
        if "OUTPUT" not in file:
            bufData=readPrepare_inputData(file,pathFiles+countryCode+"\\")
            countryFullData=countryFullData.append(bufData)
            bufData=pd.DataFrame()
    
    countryFullData=countryFullData.drop_duplicates(subset=countryFullData.columns,keep="first")
    #add the index inside the keword to avoid string joins
    #integer joins are preferable
    countryFullData["Keyword"]=countryFullData["Keyword"]+"&<"+countryFullData.index.astype(str)
    
    countryFullData["queryId"]=countryFullData.index
    return (sites,countryFullData)

def setApiRequest(Date,request_query="",rowLimit=1):
    #initialize the request JSON
    request={
      "aggregationType": "byPage",
      "startDate": Date,
      "endDate": Date,
      "dimensions": [
        "page"
      ],
      "rowLimit": rowLimit,
      "dimensionFilterGroups": [
        {
          "filters": [
            {
              "dimension": "query",
              "expression": request_query,
              "operator": "equals"
            }
          ]
        }
      ]
    }
    
    return request

#define a retry mechanism for rate limit exceeded error
from tenacity import retry, wait_fixed, wait_random,retry_if_exception_type
from googleapiclient.errors import HttpError

@retry(wait=wait_fixed(5)+wait_random(0,3)
        ,retry=retry_if_exception_type(HttpError))
def queryHasData(request_id,response,exception):
    batchResponse.append(bool(response))
    
def checkForData_byQueryTerm(inputQueries,sites,startDate,endDate):
    global batchResponse
    batchResponse=[]
    
    from datetime import datetime, timedelta
    
    startDate=datetime.strptime(startDate,"%Y-%m-%d")
    endDate=datetime.strptime(endDate,"%Y-%m-%d")
    
    period=(endDate-startDate).days
    batch=service.new_batch_http_request(callback=queryHasData)
    numReqMade=0
    batchSize=0
    maxNumRequests=(period+1)*len(inputQueries)
    queriesHaveData=[]
    for site in sites:
        for i in range(period+1):
            Date=startDate+timedelta(days=i)
            Date=datetime.strftime(Date,'%Y-%m-%d')
            for query in inputQueries:
                req_query=query.translate({ord(c): None for c in "&<0123456789"})
                request=setApiRequest(Date,request_query=req_query)
                batch.add(service.searchanalytics().query(siteUrl=site, body=request
                                  ,fields="rows/clicks"))
                queriesHaveData.append([site,query,Date])
                numReqMade+=1
                batchSize+=1
                if batchSize==1000 or numReqMade==maxNumRequests:
                    batch.execute()
                    batch=service.new_batch_http_request(callback=queryHasData)
                    if batchSize==1000:
                        batchSize=0
                    else:
                        numReqMade=0
    #global bufferResult
    batchResponse=pd.DataFrame(batchResponse).rename(index=str,columns={0:"hasData"})
    queriesHaveData=pd.DataFrame(queriesHaveData)
    queriesHaveData.rename(index=str,columns={0:"site",1:"queryTerm",2:"date"},inplace=True) 
    queriesHaveData=queriesHaveData.join(batchResponse)            
    return queriesHaveData

@retry(wait=wait_fixed(5)+wait_random(0,3)
        ,retry=retry_if_exception_type(HttpError))
def getFullData(request_id,response,exception):
    if response:
        out={**response["rows"][0],**dict(queriesWithData.iloc[int(request_id),:])}
        table_full.append(out)    

def getSearchDataWhereExists(queriesWithData):
    global table_full
    table_full=[]
    
    rowLimit=5000
    
    batch=service.new_batch_http_request(callback=getFullData)
    batchSize=0
    maxNumRequests=len(queriesWithData.index)
    
    for i in range(maxNumRequests):
        row=queriesWithData.iloc[i,:]
        #keep the query with the indentifier at the end to add as column
        #logic is used to prevent string based joins
        #recQuery is the form sent as parameter to API - string without identifier
        recQuery=row.queryTerm.translate({ord(c): None for c in "&<0123456789"})
        request=setApiRequest(row.date,request_query=recQuery,rowLimit=rowLimit)
        batch.add(service.searchanalytics().query(siteUrl=row.site, body=request
                              ,fields="rows/clicks,rows/impressions,rows/keys,rows/position")
                        ,request_id=str(i))
        batchSize+=1
        #call the Search API with a retry mechanism
        if batchSize==1000 or i==maxNumRequests-1:
            batch.execute()
            batch=service.new_batch_http_request(callback=getFullData)
            batchSize=0
        
    table_full=pd.DataFrame(table_full)
    #add the landing as string column instead of list
    table_full.insert(3,"landing",table_full["keys"].str[0])

    #delete keys - it has been transformed in string column
    table_full.drop('keys',axis=1,inplace=True)
      
    table_full['clicks']=table_full['clicks'].astype(int)
    table_full['impressions']=table_full['impressions'].astype(int)
    return table_full
    
def getSearchConsole_Data(searchQueriesData,sites,startDate,endDate): 
    
    global sample_tools, service, flags, queriesWithData
    #import Google API Client library
    from googleapiclient import sample_tools
          
    #initialize the connector to Google API
    service, flags = sample_tools.init(
        ['SearchConsole_ByTermQuery_BigQuery_DataTransfer.py']
        , 'webmasters', 'v3', __doc__, pathFiles+'SearchConsole_ByTermQuery_BigQuery_DataTransfer.py',
        scope='https://www.googleapis.com/auth/webmasters.readonly')
    #define URLs for which data is requested    
    #define site properties
    sites=sites
    country=sites[0][-2:]
    #get input data
    searchQueriesData
    inputQueries=searchQueriesData[searchQueriesData.columns[0]]
    
    queriesHaveData=checkForData_byQueryTerm(inputQueries,sites,startDate=startDate,endDate=endDate)

    #split data by presence of search console information
    tableNoData=queriesHaveData[queriesHaveData["hasData"]==0]
    queriesWithData=queriesHaveData[queriesHaveData["hasData"]==1]
    del(queriesHaveData)
    
    #add columns for consistency with already existing data
    if not tableNoData.empty:
        tableNoData[["responseQuery","queryId"]]=tableNoData["queryTerm"].str.split("&<",expand=True)
        tableNoData.drop("queryTerm",axis=1,inplace=True)
    
        tableNoData=tableNoData[["responseQuery","site","date","queryId","hasData"]]
        tableNoData=tableNoData.join(searchQueriesData.set_index("queryId"),on="queryId")
        tableNoData.insert(0,"clicks",None)
        tableNoData.insert(1,"impressions",None)
        tableNoData.insert(2,"position",20)
        tableNoData.insert(3,"landing",None)
        #delete redundant columns
        tableNoData.drop(["queryId","Keyword"],axis=1,inplace=True)
    
    #the DataFrame is sent as a parameter to get the full data
    #it is based on the previous check of existance of data in Search Console
    #for that site, date, and search query
    if not queriesWithData.empty:
        table_full=getSearchDataWhereExists(queriesWithData)    
        table_full[["responseQuery","queryId"]]=table_full["queryTerm"].str.split("&<",expand=True)
        table_full.drop("queryTerm",axis=1,inplace=True)
        table_full=table_full.join(searchQueriesData.set_index("queryId"),on="queryId")
        table_full.drop(["queryId","Keyword"],axis=1,inplace=True)
        colNames=list(tableNoData.columns)
        table_full=table_full.reindex_axis(colNames,axis=1,copy=False)
        table_full=pd.concat([tableNoData,table_full])
    else:
        table_full=tableNoData
        
        
    table_full.sort_values(["date","site","category_id","responseQuery"],inplace=True)
    #create an integer key to join with click share data for the current position
    table_full["Key_PosWord"]=((table_full["position"].round().astype(int)).astype(str)+
                 table_full["wordCount"].apply(lambda x: 4 if x>=4 else x).astype(str)).astype(int)
    #create an integer key to join with click share data for the 1st position
    table_full["maxPotentialKey_PosWord"]=(str(1)+table_full["wordCount"].apply(lambda x: 4 if x>=4 else x).astype(str)).astype(int)
    table_full["country"]=country
    
    clickShares_byPosition=pd.read_csv(pathFiles+"clickShares_byPosition.csv")
    
    table_full=table_full.join(clickShares_byPosition.set_index("Key_PosWord"),on="Key_PosWord")
    table_full=table_full.join(clickShares_byPosition.set_index("Key_PosWord")
        ,on="maxPotentialKey_PosWord",lsuffix="_currentPos",rsuffix="_1stPos")
    
    return table_full

def getSearchConsoleData_forCountry(countryCodes,startDate,endDate):
    global pd, pathFiles
    pathFiles='\\\\emag.local\\HQ\\Platforms\\02_Mobile_Web\\Tracking & Analytics\\Search Console Data\\'
    import pandas as pd
    table_full=pd.DataFrame()
    if isinstance(countryCodes,str):
        country=countryCodes
        buf=importData_byCountry(country)
        sites=buf[0]
        searchQueriesData=buf[1]
        del(buf)
        table_full=getSearchConsole_Data(searchQueriesData,sites,startDate,endDate)
    elif isinstance(countryCodes,list):
        for country in countryCodes:
            buf=importData_byCountry(country)
            sites=buf[0]
            searchQueriesData=buf[1]
            del(buf)
            table_full=getSearchConsole_Data(searchQueriesData,sites,startDate,endDate)
    #write the data in a csv file
    csvName=pathFiles+country+"\\"+"OUTPUT_searchTermsData_"+startDate+"_"+endDate+".csv"
    table_full.to_csv(csvName,index=False)
    return csvName
    
countryCodes="RO"
startDate="2017-09-13"
endDate="2017-09-20"

from google.cloud import bigquery
bigquery_client = bigquery.Client(project='emagbigquery')
dataset_name = 'SearchConsoleData'

# Prepares the new dataset
dataset = bigquery_client.dataset(dataset_name)

table=dataset.table('ClickShareCalculation')

def wait_for_job(job):
    import time
    while True:
        job.reload()
        if job.state == 'DONE':
            if job.error_result:
                raise RuntimeError(job.errors)
            return
        time.sleep(1)
        
def uploadFileToBQ(sourceFile):
    with open(sourceFile,'rb') as source_file:
        job=table.upload_from_file(source_file,source_format='text/csv'
                               ,skip_leading_rows=1)
        wait_for_job(job)

csvName=getSearchConsoleData_forCountry("RO","2017-09-13","2017-09-20")
uploadFileToBQ(csvName)
#remove(csvName)



