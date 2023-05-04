import urllib.request
import json
import json
import boto3
import pymysql
from datetime import date
import csv
import urllib3
import requests
import io
import uuid
from datetime import datetime
import calendar
import traceback

def lambda_handler(event, context):
    # TODO implement
    fileId = event['file_data_src_id']
    session = boto3.session.Session()
    counter = 0
    query_results = ''
    timestamp=datetime.now()
    unix_epoch_time = calendar.timegm(timestamp.timetuple())
    time = str(timestamp).split('.')[0]
    current_timestamp = str(timestamp).replace('-','').replace(' ','').replace(':','').split('.')[0]
    response_API = requests.get(f'http://dataprovider5-env.eba-bjm63ugz.us-east-2.elasticbeanstalk.com/api/getFileDetailsForAcquisition/{fileId}')
    data = response_API.text
    query_results = json.loads(data)
    fileInstanceId =str(fileId)+"-"+str(unix_epoch_time)
    client_kinesis = boto3.client('kinesis')
    type_job_started = "acquisition.job.started"
    try:    
        #acquisition job started event 
        content='''{
        "eventType": "acquisition.job.started",
        "eventSource": "acquire-file-via-http",
        "eventTime": "%s",
        "eventData":{
          "fileId": "%s",
          "fileInstanceId": "%s"
          }
    }'''%(time,fileId,fileInstanceId)
        response = client_kinesis.put_record(StreamName = 'iaas_kinesis_acquisition_stream', Data=content, PartitionKey = str(uuid.uuid4()))
        print(response)
        if query_results != '':
            baseurl = query_results["dataProviderConnectionDetails"]['baseUrl']
            src_loc = query_results["fileDataSrcSrcLoc"]['endpoint']
            url = baseurl + src_loc
            bucket_name = query_results["fileDataSrcTgtLoc"]['s3Bucket']
            s3_path = query_results["fileDataSrcTgtLoc"]['s3Path']
            file_name = query_results["fileDataSrcFileNamePattern"]
            file_type = query_results["fileDataSrcFileType"]
            file_name2 = file_name.split(".")[0]
            file_extention  = file_name.split(".")[1]
            data_provider = query_results["dataProviderName"]
            formatted_date = datetime.now().strftime("%Y-%m-%d")
            timestamp = datetime.now().strftime("%Y%m%d-%H:%M:%S")
            s3_path= f"{data_provider}/{formatted_date}/{file_name2}_{timestamp}"
            file_name_in_s3 = file_name2+"_"+timestamp
            response = requests.get(url)
            s3 = boto3.client("s3")
            #s3.put_object(Bucket=bucket_name, Key=s3_path, Body=io.BytesIO(response.content))
            try:
                s3.put_object(Bucket=bucket_name, Key=s3_path+"."+file_extention, Body=io.BytesIO(response.content))
                print("File Uploaded to S3!")
                counter += 1
                type_job = "acquisition.job.succeeded"
                
                content='''{
        "eventType": "acquisition.job.succeeded",
        "eventSource": "acquire-file-via-http",
        "eventTime": "%s",
        "eventData":{
          "fileId": "%s",
          "fileName": "%s",
          "filePath": "%s",
          "fileInstanceId": "%s"
          }
    }'''%(time,fileId,file_name_in_s3,s3_path,fileInstanceId)
            
            
            except Exception as e:
                error_message = traceback.format_exc()
                print(error_message)
                m = error_message.split()
                print(m)
                type_job = "acquisition.job.failed_event"
                print(type)
                str1 = ''
                for i in m:
                    str1 = str1 +' '+ i
                print(str1) 
                error_msg = str1.replace('"',"'")
                content='''{
        "eventType": "acquisition.job.failed",
        "eventSource": "acquire-file-via-http",
        "eventTime": "%s",
        "eventData":{
          "fileId": "%s",
          "errorMessage":"%s"
          }
    }'''%(time,fileId,error_msg)
        
            query_results=''            
        if counter == 0:
            print("----No Records Found----")
        
        
        
        print(content)
        print(type(content))
        
        # content_dumps = json.dumps(content)
        content_json = json.loads(content)
        print(content_json)
        print(type(content_json))
        
        json_str = json.dumps(content_json["eventData"])
        print(json_str)
        
        json_str = "'"+ json_str +"'"
        
        
        event_path='event-payloads/acquisition-events/'+file_name2+"_"+str(current_timestamp)+'.json'
        s3 = boto3.client('s3')
        s3.put_object(Bucket=bucket_name, Key=event_path, Body=content)
    
    
        response = client_kinesis.put_record(StreamName = 'iaas_kinesis_acquisition_stream', Data=content, PartitionKey = str(uuid.uuid4()))
        print(response)
    
    
        #extracting sequence number from kinesis response
        kinesis_seq_no = response['SequenceNumber']
        content_json['eventSequence'] = kinesis_seq_no
        print(type(content_json))
        print(content_json)
        
        
        #inserting into db
        url = "http://eventservice-env.eba-2bajizrw.us-east-2.elasticbeanstalk.com/api/addNewEvent"
    
        headers = {
            "Content-Type": "application/json",
            "Accept-Encoding": "gzip, deflate, br",
            "Accept": "*/*"
        }
        
        
        response = requests.post(url, json=content_json, headers=headers)
        
        print(response.status_code)
        print(response.text)
    
    except Exception as e:
        #except block for failure of acquisition job
        print("Except block")
        error_message = traceback.format_exc()
        print(error_message)
        m = error_message.split()
        print(m)
        type_job = "acquisition.job.failed___"
        print(type)
        str1 = ''
        for i in m:
            str1 = str1 +' '+ i
        print(str1) 
        error_msg = str1.replace('"',"'")
        content='''{
        "eventType": "acquisition.job.failed__",
        "eventSource": "acquire-file-via-http",
        "eventTime": "%s",
        "eventData":{
          "fileId": "%s",
          "errorMessage":"%s"
          }
    }'''%(time,fileId,error_msg)
        response = client_kinesis.put_record(StreamName = 'iaas_kinesis_acquisition_stream', Data=content, PartitionKey = str(uuid.uuid4()))
        print(response)
    

