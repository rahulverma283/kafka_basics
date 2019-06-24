from kafka import KafkaConsumer
from json import loads


'''
auto_offset_reset = 'earliest'. It handles when the consumer restarts reading
after breaking down.

when set to latest, the consumer starts reading at the end of the log.
when set to earliest, the consumer starts reading at the latest commited offset.


enable_auto_commit = TRue, make sure, consumer commits its read offset every interval

group_id = 'counters'. the consumer groups to which the consumer belongs.
consumer needs to be a prt of a consumer group to make the auto commit work
'''

consumer = KafkaConsumer(
    'numtest',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='my-group',
    value_deserializer=lambda x: loads(x.decode('utf-8'))
)

for message in consumer:
    details = message.value

    temp = len(details)+1

    for i in range(temp):
        if int(details['Items'][i]['sku']) >9200:
         print('message is {} '.format(details['Items'][i]))
