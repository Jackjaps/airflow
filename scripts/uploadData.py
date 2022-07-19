from google.cloud import storage
import sys,os,datetime,csv

def upload_blob(bucket_name, source_file_name, destination_blob_name,cred):
    storage_client = storage.Client.from_service_account_json(cred)
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    datetoday = datetime.datetime.today().date()
    start_time = datetime.datetime.today()
    blob.upload_from_filename(source_file_name)
    end_time =  datetime.datetime.today()
    LogUploadMetaData(start_time,end_time,source_file_name,datetoday)

    


def LogUploadMetaData(stime,etime,filename,ldate):
    file_stats = os.stat(filename)
    fsize= file_stats.st_size
    ltime = (etime - stime).seconds
    metadata = [stime.strftime("%m/%d/%Y %H:%M:%S"), etime.strftime("%m/%d/%Y %H:%M:%S"),str(ltime),str(fsize),filename,ldate.strftime("%m/%d/%Y")]
    with open("metadata.csv" ,'a+', newline='') as fp:
        writer = csv.writer(fp)
        writer.writerow(metadata)

"""
Parameters
Bucket Name: 
Source File path: 
Destination blob:
key file path: 

Output
start time 
end time
File size
time to upload: 
File Name:
Load Date:  

running example 
/usr/local/bin/python3 uploadData.py "jacobs_bucket" "/Users/jacobomunoz/Documents/data/accidents_opendata.csv" "accidents_opendata.csv" "/Users/jacobomunoz/Documents/python_code/googleStorage/secure/doproject-354402-5b7eddef9af7.json"
"""

def main(argv):
   if len(argv) > 1 : 
    bucket_name=argv[1]
    source_file=argv[2]
    dest_blob=argv[3]
    key_file=argv[4]
    upload_blob(bucket_name, source_file, dest_blob,key_file)

if __name__ == "__main__":
   main(sys.argv)

