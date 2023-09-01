# Read&Write function

import apache_beam as beam

p1 = beam.Pipeline()

voos = (
p1
  # Read files
  | "Import data" >> beam.io.ReadFromText("voos_sample.csv", skip_header_lines = 1)
  | "Print Result" >> beam.Map(print)
  | "Write result" >> beam.io.WriteToText("output.txt")
)

p1.run()