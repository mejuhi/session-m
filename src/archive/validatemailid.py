import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam import pvalue
import re


class ValidateMailID(beam.DoFn):
    # These tags will be used to tag the outputs of this DoFn.
    OUTPUT_TAG_VALID = 'Valid-mailid'
    OUTPUT_TAG_INVALID = 'Invalid-mailid'

    def process(self, data_item):
        # print data_item
        mailid = data_item.split(",")[3]
        # print(type(mailid))
        # pass the regualar expression
        regex = '^\w+([\.-]?\w+)*@\w+([\.-]?\w+)*(\.\w{2,3})+$'
        # check (mailid)
        # and the string in search() method
        if(re.search(regex, str(mailid))):
            # print("valid")
            yield pvalue.TaggedOutput(ValidateMailID.OUTPUT_TAG_VALID,
                                      data_item)
        else:
            # print("in-valid")
            yield pvalue.TaggedOutput(ValidateMailID.OUTPUT_TAG_INVALID,
                                      data_item)


class Printer(beam.DoFn):
    def process(self, data_item):
        # print("hello")
        print data_item


p = beam.Pipeline(options=PipelineOptions())


validate_data = (p
                 | 'Read Customer1 File' >> ReadFromText('input/customer1.csv',
                                                         skip_header_lines=1
                                                         )
                 | 'Validate Mail id' >> beam.ParDo(ValidateMailID())
                                             .with_outputs(
                                              ValidateMailID.OUTPUT_TAG_VALID,
                                              ValidateMailID.OUTPUT_TAG_INVALID
                                                           )
                 )

valid_records = validate_data[ValidateMailID.OUTPUT_TAG_VALID] | "write valid records" >> WriteToText("valid/customer1",".csv")
invaid_records = validate_data[ValidateMailID.OUTPUT_TAG_INVALID] | "write invalid records" >> WriteToText("invalid/customer1",".csv")

#print_from_source = (p
#                     | 'ReadMyFile' >> ReadFromText(file_pattern='valid/customer1*')
#                     # | 'Printer the data' >> beam.ParDo(Printer())
#                     | 'jsjosola the data' >> WriteToText("hello.txt")
#                     )


result = p.run().wait_until_finish()
print(result)
