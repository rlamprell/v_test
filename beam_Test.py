import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

# lets have a sample string
# data = ["this is sample data", "this is yet another sample data"]
with beam.Pipeline(options=PipelineOptions()) as pipeline:
    data  = pipeline | 'ReadMyFile' >> beam.io.ReadFromText('gs://cloud-samples-data/bigquery/sample-transactions/transactions.csv')

# create a pipeline
pipeline = beam.Pipeline()
counts = (pipeline | "create" >> beam.Create(data)
    | "split" >> beam.ParDo(lambda row: row.split(" "))
    | "pair" >> beam.Map(lambda w: (w, 1))
    | "group" >> beam.CombinePerKey(sum))

# lets collect our result with a map transformation into output array
output = []
def collect(row):
    output.append(row)
    return True

counts | "print" >> beam.Map(collect)

# Run the pipeline
result = pipeline.run()

# lets wait until result a available
result.wait_until_finish()

# print the output
print(output)