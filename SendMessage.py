import boto3


sqs =boto3.client('sqs')
queue_url = 'https://sqs.us-east-1.amazonaws.com/067610562392/serviceFifo.fifo'
messages = ['11.h264','9.h264','2.h264','3.h264','4.h264','5.h264','6.h264']
for i in messages:
    sqs.send_message(QueueUrl=queue_url, MessageGroupId=i, MessageBody=i)