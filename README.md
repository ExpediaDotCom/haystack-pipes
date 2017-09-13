# haystack-pipes
Packages to send ("pipe") Haystack data to external sinks (like AWS Firehose)

The haystack-pipes unit delivers a human-friendly version of Haystack messages to zero or more "durable" locations for 
more permanent storage. Current "plug`in" candidates for such storage include:
1. [Amazon Kinesis Firehose](https://aws.amazon.com/kinesis/firehose/) is an AWS service that facilitates loading 
streaming data into AWS. Note that its 
[PutRecordBatch API](http://docs.aws.amazon.com/firehose/latest/APIReference/API_PutRecordBatch.html) accepts up to
500 records, with a maximum size of 4 MB for each put request. The plug in will batch the records appropriately.
Kinesis Firehose can be configured to deliver the data to
    * [Amazon S3](https://aws.amazon.com/s3/)
    * [Amazon Redshift](https://aws.amazon.com/redshift/)
    * [Amazon Elasticsearch Service](https://aws.amazon.com/elasticsearch-service/)
    
## json-transformer    
The [json-transformer](https://github.com/ExpediaDotCom/haystack-pipes/tree/master/json-transformer) package is a
lightweight service that uses [Kafka Streams](https://kafka.apache.org/documentation/streams/) to read the protobuf 
records from Kafka, transform them to JSON, and write them to another topic in Kafka. The plugins will then consume
from the latter topic, and then write the JSON records just consumed to their destinations.

## firehose-writer
