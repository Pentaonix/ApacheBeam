import apache_beam as beam
import os
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions

# Define options for pipeline
options = {
    'project': 'dataflow-apachebeam-397608',
    'runner': 'DataFlowRunner',
    'region': 'asia-east2', #Maybe there is no effection because we can choose the region on Dataflow creating Jobs console
    'staging_location': 'gs://dataflow-apachebeam/temp',
    'temp_location': 'gs://dataflow-apachebeam/temp',
    'template_location': 'gs://dataflow-apachebeam/template/batch_job_bq',
    'save_main_session': True 
}

options = PipelineOptions.from_dictionary(options)
p1 = beam.Pipeline(options=options)

# Define BQ env
table_schema = 'airport:STRING, delayed_num:INTEGER, delayed_time:INTEGER'
table = 'dataflow-apachebeam-397608:flights_dataflow.flights'

SA_credentials = "credentials.json"
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = SA_credentials

class Filter(beam.DoFn):
  def process(self, record):
    if int(record[8]) > 0:
        yield record

def unset_record(record):
    rec_dict = {}
    record = list(record)
    rec_dict["airport"] = record[0]
    rec_dict["delayed_num"] = record[1].get("delayed_num")[0]
    rec_dict["delayed_time"] = record[1].get("delayed_time")[0]
    return rec_dict

delayed_num = (
p1
  | "Import data: number of delayed flights" >> beam.io.ReadFromText("gs://dataflow-apachebeam/input/samples.csv")
  | "Split: number of delayed flights" >> beam.Map(lambda record: record.split(","))
  | "Filter: number of delayed flights " >> beam.ParDo(Filter())
  | "Create Key-value pair: number of delayed flights" >> beam.Map(lambda record: (record[4], int(record[8])))
  | "Count by key: number of delayed flights" >> beam.combiners.Count.PerKey()
)

delayed_total_time = (
p1
  | "Import: data total mins of delayed flights" >> beam.io.ReadFromText("gs://dataflow-apachebeam/input/samples.csv")
  | "Split: total mins of delayed flights" >> beam.Map(lambda record: record.split(","))
  | "Filter: total mins of delayed flights " >> beam.ParDo(Filter())
  | "Create Key-value pair: total mins of delayed flights" >> beam.Map(lambda record: (record[4], int(record[8])))
  | "Combine by key: total mins of delayed flights" >> beam.CombinePerKey(sum)
)

Join_delayed_table = (
  {'delayed_num': delayed_num, 'delayed_time': delayed_total_time}
  | "Join 2 PCol" >> beam.CoGroupByKey()
  | "Unset record" >> beam.Map(lambda record: unset_record(record))
  | "Write to BQ" >> beam.io.WriteToBigQuery(
                                table,
                                project='dataflow-apachebeam-397608',
                                schema=table_schema,
                                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                custom_gcs_temp_location = 'gs://dataflow-apachebeam/temp')
)

p1.run()