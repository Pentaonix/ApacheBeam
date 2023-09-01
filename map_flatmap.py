#Map&FlatMap function

import apache_beam as beam

p1 = beam.Pipeline()

Pcol = (
p1
  # Read files
  | "Import data" >> beam.io.ReadFromText("Poem.txt")
  | "Split with Map" >> beam.Map(lambda record: record.split(" "))
  # | "Split with FlatMap" >> beam.FlatMap(lambda record: record.split(" "))
  | "Print Result" >> beam.Map(print)
)

p1.run()