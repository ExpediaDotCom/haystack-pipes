# json-transformer

The `json-transformer` service uses [Kafka Streams](https://kafka.apache.org/documentation/streams/) to read protobuf
records from Kafka, transform them to JSON, and write the transformed record to another topic in Kafka. The destination
Kafka is the same Kafka queue with a different topic name than one from which the protobuf records were read. (This 
behavior is due to the behavior of Kafka streams; to write to a different Kafka queue, see the
[kafka-producer](https://github.com/ExpediaDotCom/haystack-pipes/tree/master/kafka-producer) module.)
The code is simple and self-explanatory and consists of the following classes:
1. A [transformer](https://github.com/ExpediaDotCom/haystack-pipes/blob/master/json-transformer/src/main/java/com/expedia/www/haystack/pipes/jsonTransformer/ProtobufToJsonTransformer.java)
that wires the deserializer and serializer into a
[Kafka Streams](https://cwiki.apache.org/confluence/display/KAFKA/Kafka+Streams) pipeline.
2. A simple Spring Boot [application](https://github.com/ExpediaDotCom/haystack-pipes/blob/master/json-transformer/src/main/java/com/expedia/www/haystack/pipes/jsonTransformer/JsonTransformerIsActiveController.java)
that provides an HTTP endpoint, used for health checks.
3. [Unit tests](https://github.com/ExpediaDotCom/haystack-pipes/tree/master/json-transformer/src/test/java/com/expedia/www/haystack/pipes/jsonTransformer)

Various classes from the [commons](https://github.com/ExpediaDotCom/haystack-pipes/tree/master/commons)
package are also used.
