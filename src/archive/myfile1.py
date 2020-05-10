import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
import re

p = beam.Pipeline(options=PipelineOptions())

class Printer(beam.DoFn):
  def process(self, data_item):
    print data_item
    mailid = str(data_item).split(",")[3]
    # pass the regualar expression 
    regex = '^\w+([\.-]?\w+)*@\w+([\.-]?\w+)*(\.\w{2,3})+$'
    # check (mailid)
    # and the string in search() method
    if(re.search(regex, mailid)):
      print("valid")  
    else:
      print("in-valid")  

data_from_source = (p
 | 'ReadMyFile' >> ReadFromText('customer1.csv')
 #| 'Printer the data' >> beam.ParDo(Printer())
 | 'jsjosola the data' >> WriteToText("hello.txt")
 )



result = p.run()