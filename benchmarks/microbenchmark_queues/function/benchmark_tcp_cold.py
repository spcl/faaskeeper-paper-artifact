import json
import socket

connection = None
is_cold = False
REPS = 100

def get_object(obj: dict):
    return next(iter(obj.values()))

def benchmarker(event, context):
    for record in event['Records']:
        if 'dynamodb' in record:
            payload = record["dynamodb"]["NewImage"]
            ip = get_object(payload["ip"])
            port = int(get_object(payload["port"]))
        elif 'body' in record:
            payload = record["body"]
            payload = json.loads(payload)
            ip = payload['ip']
            port = int(payload['port'])
        else:
            payload = record
            ip = payload['ip']
            port = int(payload['port'])

        print(ip, port)
        global is_cold
        if is_cold:
            is_cold = False
        connection = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        connection.settimeout(5)
        try:
            connection.connect((ip, port))
            connection.sendall(json.dumps({"is_cold": is_cold}).encode())
            connection.close()
        except Exception as e:
            print("Failed connection!")
            connection = None
            raise e

