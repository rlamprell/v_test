import  apache_beam as beam
# from    apache_beam.options.pipeline_options import PipelineOptions


class Convert2UpperCase(beam.DoFn):
  def process(self, element):
      return [element.upper()]

def print_row(row):
    print(row)


pipeline = beam.Pipeline()
counts = (pipeline 
  | "ReadMyFile"  >> beam.io.ReadFromText('gs://cloud-samples-data/bigquery/sample-transactions/transactions.csv')
  | "print"       >> beam.Map(print_row)
)

result = pipeline.run()
result.wait_until_finish()

# with beam.Pipeline(options=PipelineOptions()) as pipeline:
#   lines       = pipeline | 'ReadMyFile' >> beam.io.ReadFromText('gs://cloud-samples-data/bigquery/sample-transactions/transactions.csv')
#   pipeline | "print" >> beam.Map(print_row)
#   # upper_lines = lines | beam.ParDo(Convert2UpperCase())
#   # upper_lines | beam.io.WriteToText('gs://simple-airbeam-test/output')


# with beam.Pipeline(options=PipelineOptions()) as pipeline:
# lines       = pipeline | 'ReadMyFile' >> beam.io.ReadFromText('gs://cloud-samples-data/bigquery/sample-transactions/transactions.csv')
# pipeline | "print" >> beam.Map(print_row)
# # upper_lines = lines | beam.ParDo(Convert2UpperCase())
# # upper_lines | beam.io.WriteToText('gs://simple-airbeam-test/output')