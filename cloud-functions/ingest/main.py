#!/usr/bin/env python

# make sure to install these packages before running:
# pip install sodapy

from sodapy import Socrata
import json
from urllib.parse import urlparse
from google.cloud import datastore
from google.cloud import pubsub_v1
import base64
import os
import datetime
import pytz


# Unauthenticated client only works with public data sets. Note 'None'
# in place of application token, and no username or password:
#client = Socrata("data.cityofchicago.org", None)

apptoken = os.environ.get('apptoken', 'Specified environment variable is not set.')
username = os.environ.get('username', 'Specified environment variable is not set.')
password = os.environ.get('password', 'Specified environment variable is not set.')

# Example authenticated client (needed for non-public datasets):
client = Socrata("data.cityofchicago.org",
                  apptoken,
                  username=username,
                  password=password)

import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore

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
        
def call_realtime_api(data, context):        
    """Background Cloud Function to be triggered by Pub/Sub.
    Args:
         data (dict): The dictionary with data specific to this type of event.
         context (google.cloud.functions.Context): The Cloud Functions event
         metadata.

    """
    if 'source' in data:
        source = base64.b64decode(data['source']).decode('utf-8')
    else:
        source = 'None'

    # Real time feed
    # https://data.cityofchicago.org/resource/n4j6-wkkf.json?$$app_token=XXXXXXXXXXXXXXXXXXX&$order=_last_updt DESC&$limit=5000&$select=*
    results = client.get("n4j6-wkkf", limit=10000)
    #print (results)

    # Get existing state
    existingCol = db.collection('segment_state').get()
    existingDict = {}
    for doc in existingCol:
        existingDict[doc.id] = doc.get("time")

    #Prepare a list for PubSub
    new_event_list = []
    new_state_list = []

    # Writing to last state table in firestore
    chunksOf500 = list(chunks(results, 500))
    for chunk in chunksOf500:        
        # Start a batch
        batch = db.batch()
        
        for s in chunk:

            # All data stored in firestore / bigquery is adjusted to UTC            
            dt = datetime.datetime.strptime(s['_last_updt'] + "-0600", '%Y-%m-%d %H:%M:%S.%f%z')
            dt_utc = dt.astimezone(pytz.utc)

            new_segment = {
                "segment_id" : s['segmentid'],
                "street" : s['street'],
                "direction" : s['_direction'],
                "from_street" : s['_fromst'],
                "to_street" : s['_tost'],
                "length" : s['_length'],
                "street_heading" : s['_strheading'],
                "comments" : s.get('_comments'),
                "start_longitude" : s['start_lon'],
                "start_latitude" : s['_lif_lat'],
                "end_longitude" : s['_lit_lon'],                
                "end_latitude" : s['_lit_lat'],                
                "speed" : s['_traffic'],                                
                "time" : dt_utc.strftime('%Y-%m-%d %H:%M:%S.%f')
            }
            new_state_list.append(new_segment)

            # Check for delta change, by comparing last update date from API and from Firestore
            if new_segment["segment_id"] in existingDict:                
                # Check if last update date is less than the state from API
                lastupdate = existingDict[new_segment["segment_id"]]
                #print ("State Exists - " + new_segment["segment_id"] + ", API Time: " + new_segment["time"] + " - State Updated: " + lastupdate)
                
                if new_segment["time"] > lastupdate:
                    # New event has happened, publish to Pub/Sub"
                    print ("New Event - " + new_segment["segment_id"] + ", API Time: " + new_segment["time"] + " - State Updated: " + lastupdate)

                    #0581-201903010140
                    record_date_str = dt_utc.strftime('%Y%m%d%H%M')
                    record_segment = new_segment['segment_id'].zfill(4)
                    record_id = record_segment + "-" + record_date_str

                    new_event = {
                        "time" : new_segment['time'],
                        "segment_id" : new_segment['segment_id'],
                        "speed" : new_segment['speed'],
                        # "bus_count" : None,
                        # "messaging_count" : None,
                        "hour" : str(dt.hour),
                        "day_of_week" : str(dt.weekday()),
                        "month" : str(dt.month),
                        "record_id" : record_id,
                        "source" : "realtime"
                    }
                    new_event_list.append(new_event)

            # Set the data for segment state
            segment_ref = db.collection(u'segment_state').document(new_segment['segment_id'])
            batch.set(segment_ref, new_segment)

        # Commit the batch
        batch.commit()
        print('Completed a batch set')

    # Fire all the Publish message
    project_id = "albert-demo"
    topic_name = "new_segment_event_realtime"

    publisher = pubsub_v1.PublisherClient()
    # The `topic_path` method creates a fully qualified identifier
    # in the form `projects/{project_id}/topics/{topic_name}`
    topic_path = publisher.topic_path(project_id, topic_name)

    for n in new_event_list:
        data = json.dumps(n)
        # Data must be a bytestring
        data = data.encode('utf-8')
        # When you publish a message, the client returns a future.
        future = publisher.publish(topic_path, data=data)
        print('Published {} of message ID {}.'.format(data, future.result()))


    print ("Start publishing last state")
    # Publish the last state as one big json for BigQuery or others
    topic_name = "new_segment_state_realtime"
    topic_path = publisher.topic_path(project_id, topic_name)    
    
    for state in new_state_list:
        data = json.dumps(state)
        #print (data)
        # Data must be a bytestring
        data = data.encode('utf-8')
        # When you publish a message, the client returns a future.
        future = publisher.publish(topic_path, data=data)
        #print('Published {} of message ID {}.'.format(data, future.result()))

    print('RealTime API completed from ' + source)
    
def call_microbatch_api(data, context):
    
    if 'source' in data:
        source = base64.b64decode(data['source']).decode('utf-8')
    else:
        source = 'None'

    # Get existing state
    #segmentCol = db.collection('segment_state').get()
    
    # 2019-03-01 10:20:45.000 historical cut off time
    microbatchMaxDate = db.collection('microbatch_maxdate').document('qmPk0Jv0hoN4cHzqsn2P').get()
    maxDate = datetime.datetime.strptime(microbatchMaxDate.get('maxdate'),'%Y-%m-%d %H:%M:%S.%f')
    maxDate = maxDate - datetime.timedelta(hours=6)

    # Fire all the Publish message
    project_id = "albert-demo"
    topic_name = "new_segment_event_microbatch"

    publisher = pubsub_v1.PublisherClient()
    # The `topic_path` method creates a fully qualified identifier
    # in the form `projects/{project_id}/topics/{topic_name}`
    topic_path = publisher.topic_path(project_id, topic_name)

    # https://data.cityofchicago.org/resource/sxs8-h27x.json?$$app_token=XXXXXXXXXXXXXXXXXXX&$order=time%20DESC&$limit=5000&$select=time,segment_id,speed,bus_count,message_count,hour,day_of_week,month,record_id&time > 2018-07-28T17:01:00
    results = client.get(
        "sxs8-h27x", 
        where='time >= "' + maxDate.strftime('%Y-%m-%dT%H:%M:%S.%f') + '"',
        limit=30000,
        select="time,segment_id,speed,bus_count,message_count,hour,day_of_week,month,record_id",
        order="time ASC"
        )

    # Set a last max date, for tracking the progress of microbatch
    lastMaxDate = maxDate

    counter = 0
    # Prepare a list for PubSub
    for s in results:
        # All data stored in firestore / bigquery is adjusted to UTC            
        dt = datetime.datetime.strptime(s['time'] + "-0600",'%Y-%m-%dT%H:%M:%S.%f%z')
        dt_utc = dt.astimezone(pytz.utc)

        #0581-201903010140
        record_date_str = dt_utc.strftime('%Y%m%d%H%M')
        record_segment = s['segment_id'].zfill(4)
        record_id = record_segment + "-" + record_date_str
        new_microbatch = {
            "time" : dt_utc.strftime('%Y-%m-%d %H:%M:%S.%f'),
            "segment_id" : s['segment_id'],
            "speed" : s['speed'],
            "bus_count" : s['bus_count'],
            "message_count" : s['message_count'],
            "hour" : str(dt.hour),
            "day_of_week" : str(dt.weekday()),
            "month" : str(dt.month),
            "record_id" : record_id,
            "source" : "microbatch"
        }
        
        data = json.dumps(new_microbatch)
        # Data must be a bytestring
        data = data.encode('utf-8')
        # When you publish a message, the client returns a future.
        future = publisher.publish(topic_path, data=data)
        #print('Published {} of message ID {}.'.format(data, future.result()))

        lastMaxDate = new_microbatch['time']
        counter = counter + 1        
        if counter % 2000 == 0:
            # Checkpoint - set the last record max date
            data = { "maxdate" : lastMaxDate }    
            db.collection('microbatch_maxdate').document('qmPk0Jv0hoN4cHzqsn2P').set(data)
            print('Checkpointed after 2000 records')    

    
    # Final - set the last record max date
    data = { "maxdate" : lastMaxDate }    
    db.collection('microbatch_maxdate').document('qmPk0Jv0hoN4cHzqsn2P').set(data)

    print('Completed a segment micro batch')    
    print('MicroBatch API completed from ' + source)


if __name__ == '__main__':
	call_realtime_api ('None', 'None')
    #call_microbatch_api ('None', 'None')


