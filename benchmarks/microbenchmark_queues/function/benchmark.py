import json
import socket

connection = None
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

        global connection
        if connection is None:

            print("Begin RTT measurements")
            connection = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            connection.settimeout(5)
            try:
                connection.connect((ip, port))
            except Exception as e:
                print("Failed connection!")
                connection = None
                raise e
            print("Begin RTT measurements")
            connection.sendall(b'AAAAAAAAAAAAAAA')
            is_cold = True
            for i in range(REPS):
                data = connection.recv(64)
                connection.sendall(b'AAAAAAAAAAAAAAA')
            print("Finished RTT measurements")
        else:
            try:
                is_cold = False  
                connection.sendall(json.dumps({"is_cold": is_cold}).encode())
            except Exception as e:
                print(e)

