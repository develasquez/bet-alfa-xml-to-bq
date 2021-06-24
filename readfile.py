import argparse
import time
import logging

import apache_beam as beam
from apache_beam import io, DoFn
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import StandardOptions

from apache_beam.runners import DataflowRunner, DirectRunner
import json

class ReadFromGCS(DoFn):
    def __init__(self):
        pass

    def process(self, filename):
        import xmltodict
        content=''
        try:
            with io.gcsio.GcsIO().open(filename=filename, mode="r") as f:
                for line in f:
                    content = content + line 
            
            doc =  xmltodict.parse(content)
            file=filename.split('/')
            customer=file[len(file) -1].split('_')
            doc['customerID']=customer[0]
        except Exception as e:
            print(e)
            return
        yield doc

def run():

    table_schema = {
        'fields': [
            {'name' : 'CustomerID', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name' : 'activityName', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name' : 'startTime', 'type': 'DATETIME', 'mode': 'NULLABLE'},
            {'name' : 'calories', 'type': 'FLOAT', 'mode': 'NULLABLE'},
            {'name' : 'distance', 'type': 'FLOAT', 'mode': 'NULLABLE'},
            {'name' : 'duration', 'type': 'FLOAT', 'mode': 'NULLABLE'},
            {'name' : 'intensity', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name' : 'origin', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name' : 'speed', 'type': 'FLOAT', 'mode': 'NULLABLE'},
            {'name' : 'averageHeartRate', 'type': 'STRING', 'mode': 'NULLABLE'},

        ]
    }

    def cleanup(x):
        y = {}
        try:
            customerID = x['customerID']
            origin = x['TrainingCenterDatabase']['Author']['Name']
            print(origin)
            print(customerID)
            
            elements=[]
            try:
                if (x['TrainingCenterDatabase']['Activities']['Activity'][0]):
                    elements=x['TrainingCenterDatabase']['Activities']['Activity']

            except Exception as e:
                elements=[x['TrainingCenterDatabase']['Activities']['Activity']]

            for activity in elements:

                y = {
                    "activityName": activity['@Sport'],
                    "startTime": activity['Lap']['@StartTime'].replace('Z',''),
                    "calories": activity['Lap']['Calories'],
                    "distance": activity['Lap']['DistanceMeters'],
                    "duration": activity['Lap']['TotalTimeSeconds'],
                    "intensity": activity['Lap']['Intensity'],
                    "origin": origin,
                    "CustomerID": customerID
                }
                print(y)
        except Exception as e:
            print(e)
            return
        yield y
        

    parser = argparse.ArgumentParser()
    parser.add_argument('--output', required=True, help=('Specify text file orders.txt or BigQuery table project:dataset.table '))
    parser.add_argument('--topic', required=True, help=('Specify GCS topic'))
    parser.add_argument('--rawFilesLocation', required=True, help=('Specify GCS bucket to store output files'))
    opts, beam_args = parser.parse_known_args()
    options = PipelineOptions(beam_args)
    options.view_as(StandardOptions).streaming = True 
  
    with beam.Pipeline(options=options) as p:


        orders = (p 
             | 'read from topic' >> beam.io.ReadFromPubSub(topic=opts.topic).with_output_types(bytes)  
             | 'decoding' >> beam.Map(lambda x: x.decode('utf-8'))
             | "convert msg to dict" >> beam.Map(lambda x: json.loads(x))
             | "extract filename" >> beam.Map(lambda x : 'gs://{}/{}'.format(x['bucket'], x['name'] ))
             | "Read files from GCS" >> beam.ParDo(ReadFromGCS())
             | 'activities' >> beam.FlatMap(lambda doc: cleanup(doc)))

        orders | 'tobq' >> beam.io.WriteToBigQuery(opts.output,
            schema=table_schema,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)

    
if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()