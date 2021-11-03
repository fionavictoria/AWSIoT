import json
import boto3
from datetime import datetime

def lambda_handler(event, context):
    date = datetime.now()
    time = date.strftime("%H:%M:%S") #Get time from date variable

    #Raw_data
    raw_text = str(str(event['Temperature']) + str(event['Speed'])) #Get temperature and cpu fan speed

    #Processed_data
    temp_in_celsius = round(((event['Temperature'] - 32) * (5.0/9.0)) ,3) #(xF -32)*5/9 Converting Fahrenheit to Celsius
    message_text_temp = str("Laptop reports a temperature of {} Fahrenheit (approximately {} Celsius).".format(str(event['Temperature']), str(temp_in_celsius)))
    message_text_speed = str("\nDevice also reports a CPU Fan Speed of {}.".format(str(event['Speed'])))
    processed_text = message_text_temp + message_text_speed

    bucket_name = "laptop-metrics"
    s3 = boto3.resource("s3")
    #Store data using timestamp for easy retrieval
    s3.Bucket(bucket_name).put_object(Key="raw_data/Year_{}/Month_{}/Date_{}/{}_raw.txt".format(date.year,date.month,date.day,time), Body=raw_text)
    s3.Bucket(bucket_name).put_object(Key="processed_data/Year_{}/Month_{}/Date_{}/{}_processed.txt".format(date.year,date.month,date.day,time), Body=processed_text)
