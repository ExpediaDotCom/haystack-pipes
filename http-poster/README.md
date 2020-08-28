# http-poster

The `http-poster` service uses [Kafka Streams](https://kafka.apache.org/documentation/streams/) to read protobuf
records from Kafka, transform them to tags-flattened JSON, and send the transformed record to an HTTP endpoint as a
[POST](https://en.wikipedia.org/wiki/POST_(HTTP)) message with the Span JSON in the
[HTTP message body](https://en.wikipedia.org/wiki/HTTP_message_body).
The code is simple and self-explanatory and consists of the following classes:
1. A [collector](https://github.com/ExpediaDotCom/haystack-pipes/blob/master/http-poster/src/main/java/com/expedia/www/haystack/pipes/httpPoster/ContentCollector.java)
that collects spans into a "batch" until the batch is full (after which it is posted to the HTTP endpoint and a new
batch is started). Configurations control what text is used for the batch prefix, batch suffix, span prefix, and span 
suffix.
2. An [HTTP Poster](https://github.com/ExpediaDotCom/haystack-pipes/blob/master/http-poster/src/main/java/com/expedia/www/haystack/pipes/httpPoster/ProtobufToHttpPoster.java)
that wires serializers and deserializers into a
[Kafka Streams](https://cwiki.apache.org/confluence/display/KAFKA/Kafka+Streams) pipeline.
3. A simple Spring Boot [application](https://github.com/ExpediaDotCom/haystack-pipes/blob/master/http-poster/src/main/java/com/expedia/www/haystack/pipes/httpPoster/HttpPostIsActiveController.java)
that provides an HTTP endpoint, used for health checks.
4. [Configurations](https://github.com/ExpediaDotCom/haystack-pipes/blob/master/http-poster/src/main/java/com/expedia/www/haystack/pipes/httpPoster/HttpPostConfigurationProvider.java)
for the HTTP endpoint to which the Spans' JSON is posted.
5. A [Kafka for/each action](https://github.com/ExpediaDotCom/haystack-pipes/blob/master/http-poster/src/main/java/com/expedia/www/haystack/pipes/httpPoster/HttpPostAction.java)
at the end the Kafka Streams pipeline that posts to the HTTP endpoint.
6. [Unit tests](https://github.com/ExpediaDotCom/haystack-pipes/tree/master/http-poster/src/test/java/com/expedia/www/haystack/pipes/httpPoster)

Various classes from the [commons](https://github.com/ExpediaDotCom/haystack-pipes/tree/master/commons)
package are also used.
