import json
import socket

connection = None
is_cold = True
REPS = 100

def benchmarker(event, context):
    records = len(event['Records'])
    global is_cold
    for idx, record in enumerate(event['Records']):
        payload = record["body"]
        payload = json.loads(payload)

        connection = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        connection.settimeout(2)
        try:
            connection.connect((payload['ip'], int(payload['port'])))
            #by = len(json.dumps({"is_cold": is_cold, 'events': len(records), 'idx': record['attributes']['MessageDeduplicationId']}))
            #print("SEND", by)
            if idx == 0:
                print("send", len(json.dumps({"is_cold": is_cold, 'events': records, 'idx': record['attributes']['MessageDeduplicationId']}).encode()))
                connection.sendall(json.dumps({"is_cold": is_cold, 'events': records, 'idx': record['attributes']['MessageDeduplicationId']}).encode())
            else:
                print("send", len(json.dumps({"is_cold": is_cold, 'events': -1, 'idx': record['attributes']['MessageDeduplicationId']}).encode()))
                connection.sendall(json.dumps({"is_cold": is_cold, 'events': -1, 'idx': record['attributes']['MessageDeduplicationId']}).encode())

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

