import json
import socket

connection = None
REPS = 100

def benchmarker(event, context):
    for record in event['Records']:
        payload = record["body"]
        payload = json.loads(payload)

        global connection
        if connection is None:
            connection = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            connection.settimeout(2)
            try:
                connection.connect((payload['ip'], int(payload['port'])))
            except Exception as e:
                print("Failed connection!")
                connection = None
                raise e
            print("Begin RTT measurements")
            connection.sendall(b'0')
            is_cold = True
            for i in range(REPS):
                data = connection.recv(32)
                connection.sendall(b'0')
            print("Finished RTT measurements")
        else:
            is_cold = False  
            connection.sendall(json.dumps({"is_cold": is_cold}).encode())

