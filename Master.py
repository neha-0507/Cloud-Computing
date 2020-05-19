import time

import boto3
from collections import deque

sqs = boto3.client('sqs', region_name='us-east-1')
ec2 = boto3.resource('ec2', region_name='us-east-1')
amiId = ''
Queue = 'https://sqs.us-east-1.amazonaws.com/067610562392/serviceFifo.fifo'
def checkQueueSize():
    QSize = sqs.get_queue_attributes(
            QueueUrl=Queue,
            AttributeNames=['ApproximateNumberOfMessages']
        )
    QSize = QSize['Attributes']['ApproximateNumberOfMessages']
    return QSize

def getInstances():
    instances = ec2.instances.filter( Filters=[{
                    'Name': 'instance-state-name',
                    'Values': ['stopped']
                }])
    instanceList=[]
    for instance in instances:
        instanceList.append(instance.id)
    print(instanceList)
    return instanceList

def startInstances():
    size = int(checkQueueSize())
    instanceList = getInstances()
    start = min(size,len(instanceList))
    ec2.instances.filter(InstanceIds = instanceList[:start]).start()



startInstances()
