#Filter function

import apache_beam as beam

p1 = beam.Pipeline()


def filterstt(i):
  if i in ["quadros", "dos"]:
    return True
  return False

Pcol = (
p1
  # Read files
  | "Import data" >> beam.io.ReadFromText("Poem.txt")
  | "Split by comma" >> beam.FlatMap(lambda record: record.split(" "))
  # | "Filter by lambda" >> beam.Filter(lambda record: record[4] == "OGG")
  | "Filter by func" >> beam.Filter(filterstt)
  | "Print Result" >> beam.Map(print)
  # | "Write result" >> beam.io.WriteToText("output.txt")
)

p1.run()
