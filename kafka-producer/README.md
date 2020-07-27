# kafka-producer

The `kafka-producer` service uses [Kafka Streams](https://kafka.apache.org/documentation/streams/) to read protobuf
records from Kafka, transform them to tags-flattened JSON, and write the transformed record to another (typically
external) Kafka queue and topic. The code is simple and self-explanatory and consists of the following classes:
1. A [Kafka producer](https://github.com/ExpediaDotCom/haystack-pipes/blob/master/kafka-producer/src/main/java/com/expedia/www/haystack/pipes/kafkaProducer/ProtobufToKafkaProducer.java)
that wires serializers and deserializers into a
[Kafka Streams](https://cwiki.apache.org/confluence/display/KAFKA/Kafka+Streams) pipeline.
2. A simple Spring Boot [application](https://github.com/ExpediaDotCom/haystack-pipes/blob/master/kafka-producer/src/main/java/com/expedia/www/haystack/pipes/kafkaProducer/App.java)
that provides an HTTP endpoint, used for health checks.
3. [Configurations](https://github.com/ExpediaDotCom/haystack-pipes/blob/master/kafka-producer/src/main/java/com/expedia/www/haystack/pipes/kafkaProducer/ExternalKafkaConfig.java)
for the external (probably) Kafka queue to which the Spans' JSON is written.
4. A [Kafka for/each action](https://github.com/ExpediaDotCom/haystack-pipes/blob/master/kafka-producer/src/main/java/com/expedia/www/haystack/pipes/kafkaProducer/ProduceIntoExternalKafkaAction.java)
at the end the Kafka Streams pipeline that writes ("produces") to the external Kafka queue.
5. [Unit tests](https://github.com/ExpediaDotCom/haystack-pipes/tree/master/kafka-producer/src/test/java/com/expedia/www/haystack/pipes/kafkaProducer)

Various classes from the [commons](https://github.com/ExpediaDotCom/haystack-pipes/tree/master/commons)
package are also used.
