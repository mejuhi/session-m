
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam import pvalue
import re


class SplitCustomer1(beam.DoFn):
    def process(self, element):
        # print (element)
        myid = element.split(',')[0]
        name = element.split(',')[1]
        lastname = element.split(',')[2]
        email = element.split(',')[3]
        engagement = element.split(',')[4]
        attr1 = element.split(',')[5]
        # print(myid)
        # myid, name, lastname, email, engagement, attr1 = str(element).split(",")
        # print("hello")
        return [{
            'myid': str(myid),
            'name': str(name),
            'lastname': str(lastname),
            'email': str(email),
            'engagement': str(engagement),
            'attr1': str(attr1)
        }]


class SplitCustomer2(beam.DoFn):
    def process(self, element):
        # print (element)
        myid = element.split(',')[0]
        sex = element.split(',')[1]
        tier = element.split(',')[2]
        lastcontact = element.split(',')[3]
        pets = element.split(',')[4]
        attr1 = element.split(',')[5]
        attr2 = element.split(',')[6]
        # print(myid)
        # myid, name, lastname, email, engagement, attr1 = str(element).split(",")
        # print("hello")
        return [{
            'myid': str(myid),
            'sex': str(sex),
            'tier': str(tier),
            'lastcontact': str(lastcontact),
            'pets': str(pets),
            'attr1': str(attr1),
            'attr2': str(attr2)
        }]


class Printer(beam.DoFn):
    def process(self, data_item):
        # print("hello")
        print data_item

class LeftJoin(beam.PTransform):
    """This PTransform performs a left join given source_pipeline_name, source_data,
     join_pipeline_name, join_data, common_key constructors"""

    def __init__(self, source_pipeline_name, source_data, join_pipeline_name, join_data, common_key):
        self.join_pipeline_name = join_pipeline_name
        self.source_data = source_data
        self.source_pipeline_name = source_pipeline_name
        self.join_data = join_data
        self.common_key = common_key

    def expand(self, pcolls):
        def _format_as_common_key_tuple(data_dict, common_key):
            return data_dict[common_key], data_dict

        """This part here below starts with a python dictionary comprehension in case you 
        get lost in what is happening :-)"""
        return ({pipeline_name: pcoll | 'Convert to ({0}, object) for {1}'
                .format(self.common_key, pipeline_name)
                                >> beam.Map(_format_as_common_key_tuple, self.common_key)
                 for (pipeline_name, pcoll) in pcolls.items()}
                | 'CoGroupByKey {0}'.format(pcolls.keys()) >> beam.CoGroupByKey()
                | 'Unnest Cogrouped' >> beam.ParDo(UnnestCoGrouped(),
                                                   self.source_pipeline_name,
                                                   self.join_pipeline_name)
                )


class UnnestCoGrouped(beam.DoFn):
    """This DoFn class unnests the CogroupBykey output and emits """

    def process(self, input_element, source_pipeline_name, join_pipeline_name):
        group_key, grouped_dict = input_element
        join_dictionary = grouped_dict[join_pipeline_name]
        source_dictionaries = grouped_dict[source_pipeline_name]
        for source_dictionary in source_dictionaries:
            try:
                source_dictionary.update(join_dictionary[0])
                yield source_dictionary
            except IndexError:  # found no join_dictionary
                yield source_dictionary


p = beam.Pipeline(options=PipelineOptions())

source_pipeline_name = "csv_lines1"
csv_lines1 = (p
              | 'ReadMyFile Customer1' >> beam.io.ReadFromText('customer1.csv')
              | 'Creating Json file Customer1' >> beam.ParDo(SplitCustomer1())
              # | 'Printer the data Customer1' >> beam.ParDo(Printer())
              )

join_pipeline_name = "csv_lines2"
csv_lines2 = (p
              | 'ReadMyFile Customer2' >> beam.io.ReadFromText('customer2.csv')
              | 'Creating Json file Customer2' >> beam.ParDo(SplitCustomer2())
              # | 'Printer the data Customer2' >> beam.ParDo(Printer())
              )

common_key = "myid"

pipeline_dictionary = {source_pipeline_name: csv_lines1,
                       join_pipeline_name: csv_lines2
                       }

test_pipeline = (pipeline_dictionary
                 | 'Left join' >> LeftJoin(source_pipeline_name, csv_lines1,
                                           join_pipeline_name, csv_lines2,
                                           common_key)
                 # | 'Printer the data Customer1' >> beam.ParDo(Printer())
                 | 'Write the data' >> WriteToText("JoinedTable/Data", ".json")
                 )


result = p.run()
