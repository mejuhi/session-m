
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam import pvalue
import re


class Printer(beam.DoFn):
    def process(self, data_item):
        print data_item


p = beam.Pipeline(options=PipelineOptions())


pcoll1 = ReadFromText('customer1.csv')
pcoll2 = ReadFromText('customer2.csv')

timings = (p
           | 'ReadMyFile1' >> ReadFromText('customer1.csv'))

users = (p
         | 'ReadMyFile2' >> ReadFromText('customer2.csv'))

to_be_joined = (
    {
        'timings': timings,
        'users': users
    }
    | beam.CoGroupByKey()
    | 'Printer the data' >> beam.ParDo(Printer())
)

p.run()