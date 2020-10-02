#!/usr/bin/python
# -*- coding: utf-8 -*-
import logging
import threading
import boto3
from botocore.exceptions import ClientError
import random
import subprocess
import re
import json

'''
add_SQS helps to upload the cloudit() message to the SQS queue when the localqueue is full
'''
def add_SQS(obj):
    sqs = boto3.client('sqs')
    queue_url = \
        'https://sqs.us-east-1.amazonaws.com/067610562392/serviceFifo.fifo'
    try:
        sqs.send_message(QueueUrl=queue_url, MessageGroupId=obj[0],
                         MessageBody=obj)
    except e:
        logging.error(e)
        return False
    return True

'''
Upload_file_output is a Helper function that
uploads output files to the desired S3 Bucket in JSON format
'''
def upload_file_output(dictt,file_name, bucket, object_name=None):
    if object_name is None:
        object_name = file_name
    s3 = boto3.resource('s3')
    s3object = s3.Bucket(bucket).Object(str(file_name)+'.json')
    try:
        s3object.put(Body=(bytes(json.dumps(dictt).encode('UTF-8'))))
    except e:
        logging.error(e)
        return False
    return (True, object_name)

'''
Upload_file is a Helper function that
uploads input files to the desired S3 Bucket
'''
def upload_file(file_name, bucket, object_name=None):
    if object_name is None:
        object_name = file_name
    s3 = boto3.client('s3')
    try:
        s3.upload_file(file_name,bucket,object_name)
    except e:
        logging.error(e)
        return False
    return (True, object_name)

'''
cloudIt function uploads the video to the S3 input bucket
if the localQueue is full
'''
def cloudIt(fn):
    print('Clouding video' + str(fn))
    BUCKET_NAME = 'videobucket-01'
    FILE_NAME = fn
    result, filename = upload_file(FILE_NAME, BUCKET_NAME)
    if result:
        add_SQS(filename)
    else:
        print ('ERROR')

'''
Execute it function creates a subprocess and runs Object detection on it
The objects detected are then stored to the S3 Output Bucket
'''
def executeIt(fn):
    print('Executing on pi video' + str(fn))
    outfilepath = str('output-Pi(' + fn + ')')
    output = subprocess.run([                                   # Subprocess to run darknet object detection
        './darknet',                                            # Model used is YOLO3
        'detector',                                             # And dataset is coco dataset
        'demo',
        'cfg/coco.data',
        'cfg/yolov3-tiny.cfg',
        'yolov3-tiny.weights',
        fn,
        ], stdout=subprocess.PIPE, universal_newlines=True)
    parseit = str(output.stdout)
    clean = set()                                               # Set to store all the detected objects
    put = parseit.split('\n\x1b')                              
    for i in put:                                               # Cleaning the returned output and adding all the 
        if i.endswith('%'):                                     # Detected Objects to the set
            t = i.split('Objects:')
            for j in t:
                if j.find('FPS')== -1:
                    x = j.split(':')
                    for k in x:
                        clean.add(re.sub('[^A-Za-z]+', '', k))
    if '' in clean:
        clean.remove('')
    clean = list(clean)
    final = {}
    final['Video'] = str(fn)
    final['Objects'] = str(clean)
    BUCKET_NAME = 'output-01'
    result, filename = upload_file_output(final,outfilepath,BUCKET_NAME)





