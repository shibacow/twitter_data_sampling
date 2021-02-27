#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""A streaming word-counting workflow.
"""

# pytype: skip-file

from __future__ import absolute_import
import json
import argparse
import logging
import re
from past.builtins import unicode
from apache_beam.metrics import Metrics

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.io.gcp.bigquery import WriteToBigQuery, BigQueryDisposition
import logging
logging.basicConfig(level=logging.INFO)
from datetime import datetime

class DevideDict(beam.DoFn):
  def __init__(self):
    self.call_metrics = Metrics.counter(self.__class__, 'call_metrics')
  def __conv_date(self,d):
    dt=datetime.strptime(d,"%a %b %d %H:%M:%S +0000 %Y")
    return dt.strftime("%Y-%m-%d %H:%M:%S")

  def __user(self,d,twid):
    dkt={'tweet_id':twid}
    user=d.pop('user')
    for k in ('id','screen_name','created_at'):
      dkt[k]=user[k]
    dkt['created_at']=self.__conv_date(dkt['created_at'])
    return dkt
  def __text(self,k,d,twid):
    dkt={'tweet_id':twid}
    dkt['user_id']=d['user']['id']
    for k in ('id','text','created_at'):
      dkt[k]=d[k]
    dkt['created_at']=self.__conv_date(dkt['created_at'])
    return dkt
    
  def __separate(self,d):
    result=[]
    twid=d['id']
    dkt={}
    for k in ('retweeted_status','quoted_status','extended_tweet'):
      if k in d:
        dkt[k]=d.pop(k)
        if k!='extended_tweet':
          tid=dkt[k]['id']
          r2=self.__text(k,dkt[k],twid)
          r2['table_name']=k
          result.append((r2))
          rr=self.__user(dkt[k],tid)
          rr['table_name']='user'
          result.append(rr)
    t2=self.__text('tweet',d,twid)
    del(t2['tweet_id'])
    t2['table_name']='tweet'
    result.append(t2)
    rr=self.__user(d,twid)
    rr['table_name']='user'
    result.append(rr)
    return result
    
  def process(self, element):
    self.call_metrics.inc()
    d=json.loads(element)
    result=self.__separate(d)
    #logging.info("in process")
    return result

def schema_fn(tbl_name):
  logging.info("in schema tbl_name={}".format(tbl_name))
  if re.search('temp_user',tbl_name):
    table_schema = {
      'fields': [
        {'name': 'table_name', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name':'tweet_id','type':'INTEGER','mode':'NULLABLE'},
        {'name': 'id', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'screen_name', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'created_at', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'},
      ]
    }
    return table_schema
  elif re.search('temp_tweet',tbl_name):
    table_schema = {
      'fields': [
        {'name': 'table_name', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'id', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'user_id', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'text', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'created_at', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'},
      ]
    }
    return table_schema
  elif re.search('retweeted_status|quoted_status',tbl_name):
    table_schema = {
      'fields': [
        {'name': 'table_name', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name':'tweet_id','type':'INTEGER','mode':'NULLABLE'},
        {'name': 'id', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'user_id', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'text', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'created_at', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'},
      ]
    }
    return table_schema
    

def table_fn(elem):
  logging.info("in table elm={}".format(elem['table_name']))
  if elem['table_name']=='user':
    return 'foobar:twitter_test.temp_user'
  elif elem['table_name']=='tweet':
    return 'foobar:twitter_test.temp_tweet'
  elif elem['table_name'] in ('retweeted_status','quoted_status'):
    return 'foobar:twitter_test.temp_{}'.format(elem['table_name'])
  return 'foobar:twitter_test.none'
def run(argv=None, save_main_session=True):
  """Build and run the pipeline."""
  parser = argparse.ArgumentParser()
  parser.add_argument(
      '--input_subscription',
      help=(
          'Input PubSub subscription of the form '
          '"projects/<PROJECT>/subscriptions/<SUBSCRIPTION>."'))
  known_args, pipeline_args = parser.parse_known_args(argv)

  # We use the save_main_session option because one or more DoFn's in this
  # workflow rely on global context (e.g., a module imported at module level).

  #| 'window into' >> beam.WindowInto(window.FixedWindows(20),
  #                                   allowed_lateness=Duration(seconds=5*24*60*60))
  #| 'collect_key_pair' >> beam.GroupByKey()

  pipeline_options = PipelineOptions(pipeline_args)
  pipeline_options.view_as(SetupOptions).save_main_session = save_main_session
  pipeline_options.view_as(StandardOptions).streaming = True
  with beam.Pipeline(options=pipeline_options) as p:
    # Read from PubSub into a PCollection.
    if known_args.input_subscription:
      messages = (
        p
        | beam.io.ReadFromPubSub(subscription=known_args.input_subscription).\
        with_output_types(bytes))
    
    lines = messages | 'decode' >> beam.Map(lambda x: x.decode('utf-8'))
    devide_dict = (
      lines
      | 'devide_dict' >> (beam.ParDo(DevideDict()))
      | "to BQ" >>  WriteToBigQuery(table_fn,schema=schema_fn,project="shibacow-test")
      #| "print" >> (beam.Map(print))
    )
      

if __name__ == '__main__':
      logging.getLogger().setLevel(logging.INFO)
      run()
