import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam import pvalue
import re

p = beam.Pipeline(options=PipelineOptions())

class Printer(beam.DoFn):
  # These tags will be used to tag the outputs of this DoFn.
  OUTPUT_TAG_VALID = 'Valid-mailid'
  OUTPUT_TAG_INVALID = 'Invalid-mailid'

  def process(self, data_item):
    #print data_item
    mailid = str(data_item).split(",")[3]
    # pass the regualar expression 
    regex = '^\w+([\.-]?\w+)*@\w+([\.-]?\w+)*(\.\w{2,3})+$'
    # check (mailid)
    # and the string in search() method
    if(re.search(regex, mailid)):
      #print("valid")
      data_item = data_item + ",hello"
      yield pvalue.TaggedOutput(Printer.OUTPUT_TAG_VALID, data_item)  
    else:
      #print("in-valid")
      yield pvalue.TaggedOutput(Printer.OUTPUT_TAG_INVALID, data_item)

data_from_source = (p
 | 'ReadMyFile' >> ReadFromText('customer1.csv')
 | 'Printer the data' >> beam.ParDo(Printer()).with_outputs(
                              Printer.OUTPUT_TAG_VALID,
                              Printer.OUTPUT_TAG_INVALID))

valid_records = data_from_source[Printer.OUTPUT_TAG_VALID] | "write valid records" >> WriteToText("valid/customer1",".csv")
invaid_records = data_from_source[Printer.OUTPUT_TAG_INVALID] | "write invalid records" >> WriteToText("invalid/customer1",".csv")

result = p.run().wait_until_finish()
print(result)