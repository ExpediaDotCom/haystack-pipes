# kafka-producer

The `kafka-producer` service uses [Kafka Streams](https://kafka.apache.org/documentation/streams/) to read the protobuf
records from Kafka, transform them to tags-flattened JSON, and write the transformed record to another (typically
external) Kafka queue and topic. The code is simple and self-explanatory and consists of the following classes:
1. A [transformer](https://github.com/ExpediaDotCom/haystack-pipes/blob/master/kafka-producer/src/main/java/com/expedia/www/haystack/pipes/kafka-producer/ProtobufToJsonTransformer.java)
that wires the deserializer and serializer into a
[Kafka Streams](https://cwiki.apache.org/confluence/display/KAFKA/Kafka+Streams) pipeline.
2. [Unit tests](https://github.com/ExpediaDotCom/haystack-pipes/tree/master/kafka-producer/src/test/java/com/expedia/www/haystack/pipes)

Various classes from the [commons](https://github.com/ExpediaDotCom/haystack-pipes/tree/master/commons)
package are also used.
