import json
import socket
import time

connection = None
is_cold = True
REPS = 100

def get_object(obj: dict):
    return next(iter(obj.values()))           

def benchmarker(event, context):
    records = len(event['Records'])
    global is_cold
    for idx, record in enumerate(event['Records']):
        if 'dynamodb' in record:
            payload = record["dynamodb"]["NewImage"]
            ip = get_object(payload["ip"])
            port = int(get_object(payload["port"]))
            index = record['dynamodb']['Keys']['timestamp']['S']
        elif 'body' in record:
            payload = record["body"]
            payload = json.loads(payload)
            ip = payload['ip']
            port = int(payload['port'])
            if 'MessageDeduplicationId' in record['attributes']:
                index = record['attributes']['MessageDeduplicationId']
            else:
                index = record['messageId']
        else:
            payload = record
            ip = payload['ip']
            port = int(payload['port'])
            index = context.aws_request_id

        connection = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        connection.settimeout(2)
        try:
            #b = time.time()
            #connection.connect((payload['ip'], int(payload['port'])))
            connection.connect((ip, int(port))) #payload['ip'], int(payload['port'])))
            #e = time.time()
            #by = len(json.dumps({"is_cold": is_cold, 'events': len(records), 'idx': record['attributes']['MessageDeduplicationId']}))
            #print("SEND", by)
            if idx == 0:
                #print("send", len(json.dumps({"is_cold": is_cold, 'events': records, 'idx': record['attributes']['MessageDeduplicationId']}).encode()))
                # sqs fifo
                #connection.sendall(json.dumps({"is_cold": is_cold, 'events': records, 'idx': record['attributes']['MessageDeduplicationId']}).encode())
                # sqs 
                #connection.sendall(json.dumps({"is_cold": is_cold, 'events': records, 'idx': record['messageId']}).encode())
                # dynamo
                connection.sendall(json.dumps({"is_cold": is_cold, 'events': records, 'idx': index}).encode())
            else:
                #print("send", len(json.dumps({"is_cold": is_cold, 'events': -1, 'idx': record['attributes']['MessageDeduplicationId']}).encode()))
                #connection.sendall(json.dumps({"is_cold": is_cold, 'events': -1, 'idx': record['attributes']['MessageDeduplicationId']}).encode())
                #connection.sendall(json.dumps({"is_cold": is_cold, 'events': -1, 'idx': record['messageId']}).encode())
                connection.sendall(json.dumps({"is_cold": is_cold, 'events': -1, 'idx': index}).encode())
                #connection.sendall(json.dumps({"is_cold": is_cold, 'events': -1, 'idx': record['dynamodb']['Keys']['timestamp']['S']}).encode())
            connection.close()
        except Exception as e:
            print(f"Failed connection! {payload['ip']} {payload['port']}")
            print(e)
            connection = None
            raise e
    is_cold = False
        #global connection
        #is_cold = False
        #if connection is None:
        #    connection = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        #    connection.settimeout(2)
        #    try:
        #        connection.connect((payload['ip'], int(payload['port'])))
        #    except Exception as e:
        #        print("Failed connection!")
        #        connection = None
        #        raise e
        #    is_cold = True
        #if idx == 0:
        #    connection.sendall(json.dumps({"is_cold": is_cold, 'events': records}).encode())
        #else:
        #    connection.sendall(json.dumps({"is_cold": is_cold, 'events': -1}).encode())

