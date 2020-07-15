#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Jul 11 18:20:39 2020

@author: anandkadam
"""


import boto3
import csv

def lambda_handler(event, context):
    region='us-east-1'
    recList=[2000]
    try:            
        s3=boto3.client('s3')            
        dyndb = boto3.client('dynamodb', region_name=region)
        confile= s3.get_object(Bucket='netflix-ak-27062015', Key='netflix.csv')
        recList = confile['Body'].read().decode().split('\n') 
        firstrecord=True
        csv_reader = csv.reader(recList, delimiter=',', quotechar='"')
        
        for row in csv_reader:
            
            if (firstrecord):
                firstrecord=False
                continue
            title = row[0]
            rating = row[1]
            rating_level = row[2]
            ratingdescription = row[3]
            release_year = row[4]
            user_rating_score = row[5]
            user_rating_size = row[6]
            
            response = dyndb.put_item(
                TableName='netflix',
                Item={
                    'title' : {'S':title},
                    'rating': {'S':rating},
                    'rating_level': {'S':rating_level},
                    'ratingdescription' : {'S':ratingdescription},
                    'release_year': {'S':release_year},
                    'user_rating_score': {'S':user_rating_score},
                    'user_rating_size': {'S':user_rating_size}
                    }
             )
        print('Put succeeded:')
    except Exception as e1:
        print (str(e1))
 
