# ParDo function (customize own function and do it in parallel)
# ParDo need a Pcol(iterable param)

import apache_beam as beam
import os


SA_credentials = "credentials.json"
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = SA_credentials

p1 = beam.Pipeline()

class Filter(beam.DoFn):
  def process(self, record):
    if int(record[8]) > 0:
      # yield k can convert record sang iterator
      yield record
      # return [record] (-> tao ra list, co 1 phan tu la record -> print ra record, neu khong record se bi convert sang iterable -> tung element trong list)

delayed_num = (
p1
  | "Import data num" >> beam.io.ReadFromText("voos_sample.csv")
  | "Split num" >> beam.Map(lambda record: record.split(","))
  | "Filter num " >> beam.ParDo(Filter())
  | "Create Key-value pair num" >> beam.Map(lambda record: (record[4], int(record[8])))
  | "Count by key" >> beam.combiners.Count.PerKey()
  # | "Write result" >> beam.Map(print)
)

delayed_total_time = (
p1
  | "Import data sum" >> beam.io.ReadFromText("voos_sample.csv")
  | "Split sum" >> beam.Map(lambda record: record.split(","))
  # | "Filter sum" >> beam.Filter(lambda record: int(record[8]) > 0)
  | "Filter sum " >> beam.ParDo(Filter())
  | "Create Key-value pair sum" >> beam.Map(lambda record: (record[4], int(record[8])))
  | "Combine by key" >> beam.CombinePerKey(sum)
  # | "Write result" >> beam.Map(print)
)

Join_delayed_table = (
  {'delayed times': delayed_num, 'delayed time': delayed_total_time}
  | "Join 2 PCol" >> beam.CoGroupByKey()
  | "Save to GCS" >> beam.io.WriteToText("gs://dataflow-apachebeam/output.csv")
)

p1.run()