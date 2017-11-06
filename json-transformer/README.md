# json-transformer

The `json-transformer` service uses [Kafka Streams](https://kafka.apache.org/documentation/streams/) to read the 
protobuf records from Kafka, transform them to JSON, and write the transformed record to another topic in Kafka.
The code is simple and self-explanatory and consists of the following classes:
1. A [transformer](https://github.com/ExpediaDotCom/haystack-pipes/blob/master/json-transformer/src/main/java/com/expedia/www/haystack/pipes/ProtobufToJsonTransformer.java)
that wires the deserializer and serializer into a
[Kafka Streams](https://cwiki.apache.org/confluence/display/KAFKA/Kafka+Streams) pipeline.
2. [Unit tests](https://github.com/ExpediaDotCom/haystack-pipes/tree/master/json-transformer/src/test/java/com/expedia/www/haystack/pipes)

Various classes from the [commons](https://github.com/ExpediaDotCom/haystack-pipes/tree/master/commons)
package are also used.
