#!/usr/bin/env python

import json
from urllib.parse import urlparse
from google.cloud import pubsub_v1
import base64
import os
import datetime
import pytz

import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore
#import redis

# Use the application default credentials
cred = credentials.ApplicationDefault()
firebase_admin.initialize_app(cred, {'projectId': 'albert-demo'})
db = firestore.client()

# Create a function called "chunks" with two arguments, l and n:
def chunks(l, n):
    # For item i in a range that is a length of l,
    for i in range(0, len(l), n):
        # Create an index range for l of n items:
        yield l[i:i+n]

#def process_to_redis(datastring):
#    r = redis.StrictRedis(host='10.0.0.3', port=6379, db=0)
#    r.publish('pubsubCounters', datastring)
    
def consume_new_segment_event_realtime(data, context):
    if 'data' in data:
        data = base64.b64decode(data['data']).decode('utf-8')
    else:
        data = 'None'

    jsonData = json.loads(data)
    print (jsonData)
    doc_id = jsonData['record_id']
    
    # Add a new doc in collection 'segment_event' with ID record_id
    db.collection('segment_event').document(doc_id).set(jsonData)
    print ('Wrote to firestorecompleted')
    print ('Consume from Pub/Sub completed')


if __name__ == '__main__':
	#call_realtime_api ('None', 'None')
    #call_microbatch_api ('None', 'None')
    pass

