# firehose-writer

The `firehose-writer` service uses [Kafka Streams](https://kafka.apache.org/documentation/streams/) to read protobuf
records from Kafka, transform them to tags-flattened JSON, batch multiple requests into a batch request, and send the
batched request to [AWS Firehose](https://aws.amazon.com/kinesis/data-firehose/).
The code is simple and self-explanatory and consists of the following classes:
1. A [collector](https://github.com/ExpediaDotCom/haystack-pipes/blob/master/firehose-writer/src/main/java/com/expedia/www/haystack/pipes/firehoseWriter/FirehoseCollector.java)
that collects spans into a "batch" until the batch is full (after which it is sent to Firehose and a new batch is 
started).
2. An [producer](https://github.com/ExpediaDotCom/haystack-pipes/blob/master/firehose-writer/src/main/java/com/expedia/www/haystack/pipes/firehoseWriter/ProtobufToFirehoseProducer.java)
that wires serializers and deserializers into a
[Kafka Streams](https://cwiki.apache.org/confluence/display/KAFKA/Kafka+Streams) pipeline.
2. A simple Spring Boot [application](https://github.com/ExpediaDotCom/haystack-pipes/blob/master/firehose-writer/src/main/java/com/expedia/www/haystack/pipes/firehoseWriter/FirehoseIsActiveController.java)
that provides an HTTP endpoint, used for health checks.
3. [Configurations](https://github.com/ExpediaDotCom/haystack-pipes/blob/master/firehose-writer/src/main/java/com/expedia/www/haystack/pipes/firehoseWriter/FirehoseConfigurationProvider.java)
for the Firehose S3 bucket to which the Spans' JSON is sent.
4. A [Kafka for/each action](https://github.com/ExpediaDotCom/haystack-pipes/blob/master/firehose-writer/src/main/java/com/expedia/www/haystack/pipes/firehoseWriter/FirehoseAction.java)
at the end the Kafka Streams pipeline that writes to the Firehose S3 bucket.
5. [Unit tests](https://github.com/ExpediaDotCom/haystack-pipes/tree/master/firehose-writer/src/test/java/com/expedia/www/haystack/pipes/firehoseWriter)

Various classes from the [commons](https://github.com/ExpediaDotCom/haystack-pipes/tree/master/commons)
package are also used.
