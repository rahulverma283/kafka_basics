from time import sleep
from json import dumps
from kafka import KafkaProducer
import falcon
import json
import sys


'''
bootstrap_servers = set the host and the port the producer should contact to bootstrap initial cluster

'''

list_temp = []


class Basic(object):

    def on_get(self, req, resp):
        "Handle get request. on_put will handle post request"
        resp.status = falcon.HTTP_200
        # resp.body = '{"Name":"Rhul Verma"}'
        # resp.body = json.dump(dict_temp[1], sys.stdout)
        resp.body = json.dumps(list_temp)

    def on_post(self, req, resp):
        data = json.loads(req.stream.read())
        producer.send('numtest', value=data)


producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x:
                         dumps(x).encode('utf-8')
                         )

app = falcon.API()

app.add_route('/basic', Basic())