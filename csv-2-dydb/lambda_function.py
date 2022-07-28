# Copy Code n Paste CODE into Lambda Function on AWS

import json
import csv 
import boto3 

def lambda_handler(event, context):
    region = 'us-west-2'
    record_list = []
    try:
       s3 = boto3.client('s3')
       dynamodb = boto3.client('dynamodb', region_name = region)
       bucket = event['Records'][0]['s3']['bucket']['name']
       key = event['Records'][0]['s3']['object']['key']
       
       print('Bucket:  ', bucket, 'Key:  ', key)
       
       csv_file = s3.get_object(Bucket = bucket, Key = key)
       
       record_list = csv_file['Body'].read().decode('utf-8').split('\n')
       
       csv_reader = csv.reader(record_list, delimiter=',' ,quotechar='"')
       
       for row in csv_reader:
           client_id = row[0]
           first_name = row[1]
           last_name = row[2]
           email = row[3]
           gender = row[4]
           ip_address = row[5]
           
           print('Client ID: ', client_id, 'Firstname: ', first_name, 'Lastname: ', last_name, 'email: ', email, 'gender: ', gender, 'IP Address: ', ip_address)
           
           add_to_db = dynamodb.put_item(
               TableName= 'sflcsv-2-DB',
               Item = {
                  'client_id' : {'S': str(client_id)},
                  'first_name' : {'S': str(first_name)}, 
                  'last_name' : {'S': str(last_name)}, 
                  'email' : {'S': str(email)},
                  'gender' : {'S': str(gender)}, 
                  'ip_address' : {'S': str(ip_address)}, 
                   
                })
                
           print('Successfully added the records to the DynamoDB Table')   
           
    except Exception as e:
       print(str(e))
       
       
    return {
        'statusCode': 200,
        'body': json.dumps('CSV DynamoDB Success!')
    }