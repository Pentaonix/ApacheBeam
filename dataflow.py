import apache_beam as beam
import os
from apache_beam.options.pipeline_options import PipelineOptions

# Define options for pipeline
options = {
    'project': 'dataflow-apachebeam-397608',
    'runner': 'DataFlowRunner',
    'region': 'asia-east2-c', #Maybe there is no effection because we can choose the region on Dataflow creating Jobs console
    'staging_location': 'gs://dataflow-apachebeam/temp',
    'temp_location': 'gs://dataflow-apachebeam/temp',
    'template_location': 'gs://dataflow-apachebeam/template/batch_job_gcs'
}

options = PipelineOptions.from_dictionary(options)
p1 = beam.Pipeline(options=options)

SA_credentials = "credentials.json"
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = SA_credentials

class Filter(beam.DoFn):
  def process(self, record):
    if int(record[8]) > 0:
      # yield k can convert record sang iterator
      yield record
      # return [record] (-> tao ra list, co 1 phan tu la record -> print ra record, neu khong record se bi convert sang iterable -> tung element trong list)

delayed_num = (
p1
  | "Import data num" >> beam.io.ReadFromText("gs://dataflow-apachebeam/input/samples.csv")
  | "Split num" >> beam.Map(lambda record: record.split(","))
  | "Filter num " >> beam.ParDo(Filter())
  | "Create Key-value pair num" >> beam.Map(lambda record: (record[4], int(record[8])))
  | "Count by key" >> beam.combiners.Count.PerKey()
)

delayed_total_time = (
p1
  | "Import data sum" >> beam.io.ReadFromText("gs://dataflow-apachebeam/input/samples.csv")
  | "Split sum" >> beam.Map(lambda record: record.split(","))
  # | "Filter sum" >> beam.Filter(lambda record: int(record[8]) > 0)
  | "Filter sum " >> beam.ParDo(Filter())
  | "Create Key-value pair sum" >> beam.Map(lambda record: (record[4], int(record[8])))
  | "Combine by key" >> beam.CombinePerKey(sum)
)

Join_delayed_table = (
  {'delayed times': delayed_num, 'delayed time': delayed_total_time}
  | "Join 2 PCol" >> beam.CoGroupByKey()
  | "Save to GCS" >> beam.io.WriteToText("gs://dataflow-apachebeam/output/output.csv")
)

p1.run()