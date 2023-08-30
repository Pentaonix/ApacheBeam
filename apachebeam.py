import apache_beam as beam

input = "consultas.txt"
output = "consultas_cancel.txt"
print(input)
p1 = beam.Pipeline()

consultas_deviso = (
p1
    | beam.io.ReadFromText(input, skip_header_lines = 1)
    | beam.Map(lambda record: record.split(','))
    | beam.Filter(lambda record: float(record[2]) < 10)
    | beam.Filter(lambda record: str(record[1])=='Cancelled')
    | beam.io.WriteToText(output)
)

p1.run()