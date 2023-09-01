#Flatten function (for union data with same datatype)

import apache_beam as beam

p1 = beam.Pipeline()

Color = ["Black", "White", "Blue"]
Car = ["BMW", "Toyota"]
City = ["Tokyo", "Hanoi"]

color_pc = p1 | "color" >> beam.Create(Color)
car_pc = p1 | "car" >> beam.Create(Car)
city_pc = p1 | "city" >> beam.Create(City)

my_col = (
  (color_pc, car_pc, city_pc)
  | beam.Flatten()
  | beam.Map(print)
)

p1.run()
