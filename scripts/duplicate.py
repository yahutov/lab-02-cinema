"""Run inside tickets container: publish the SAME event twice."""
import json
import uuid
from app import topology
event = dict(type='TicketSold', message_id=str(uuid.uuid4()), ticket_id=str(uuid.uuid4()),
             customer_id='duplicate-demo', session_id='demo', seat='A1')
import pika
connection, channel = topology()
channel.confirm_delivery()
for _ in range(2):
    channel.basic_publish(exchange='cinema', routing_key='TicketSold', body=json.dumps(event),
        mandatory=True, properties=pika.BasicProperties(delivery_mode=2, message_id=event['message_id']))
connection.close()
print(json.dumps(event))
