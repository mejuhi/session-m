import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam import pvalue
import re
import json


class Printer(beam.DoFn):
    def process(self, data_item):
        # print("hello")
        print data_item


class ConvertToJsonDict(beam.DoFn):
    def process(self, data_item):
        y = data_item.replace("'", '"')
        json1_data = json.loads(y)
        #print (json1_data.keys())
        #print (type(json1_data))
        #print(json1_data["name"])
        json2_data = {}

        json2_data["external_id"] = "NONE"
        json2_data["opted_in"] = "true"
        json2_data["external_id_type"] = "NONE"
        json2_data["locale"] = "NONE"
        json2_data["ip"] = 	"NONE"
        json2_data["dob"] = "NONE"
        json2_data["address"] =	"NONE"
        json2_data["city"] = "NONE"
        json2_data["state"]	= "NONE"
        json2_data["zip"] = "NONE"
        json2_data["country"] = "NONE"
        json2_data["referral"] = "NONE"
        json2_data["custom_1"] = "NONE"
        json2_data["custom_2"] = "NONE"
        json2_data["gender"] = str(json1_data["sex"])
        json2_data["first_name"] = str(json1_data["name"])
        json2_data["last_name"] = str(json1_data["lastname"])
        json2_data["email"] = str(json1_data["email"]) 
        json2_data["phone_numbers"] = str(json1_data["attr2"])
        print(json2_data)

p = beam.Pipeline(options=PipelineOptions())

print_json = (p
              | 'ReadMyFile' >> ReadFromText(file_pattern='JoinedTable/Data*')
              | 'Printer the data' >> beam.ParDo(ConvertToJsonDict())
              )


result = p.run().wait_until_finish()
print(result)