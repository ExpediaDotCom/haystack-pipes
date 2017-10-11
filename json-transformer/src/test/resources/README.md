The files in this directory are not used by the unit tests; they facilitate sending a test message to Kafka.
The file `span.decoded` is the text file representation of a Span object defined in the haystack-idl
protobuf definitions, and the file `span.encoded` contains the protobuf-encoded data from `span.decoded`, 
created with the following command:
```
cat ../../json-transformer/src/test/resources/span.decoded | protoc --encode=Span span.proto >>../../json-transformer/src/test/resources/span.encoded
```
Assuming Kafka is running in your minikube installation, and you have kafkacat installed, 
you can then issue the following:
```
kafkacat -P -b $(minikube ip):9092 -t proto-spans -Z ../../json-transformer/src/test/resources/span.encoded
```
to post a message to Kafka. If the main() method in ProtobufToJsonTransformer.java is running (start it by running
IsActiveController.main()), it will read that message from Kafka and post the message's JSON equivalent to the 
`json-spans` topic, ªwhich you can then see with:
```
kafkacat -C -b $(minikube ip):9092 -t json-spans
```
All of the above commands should be run in the directory in which this README.md file is found.
You should have no need to run the first command (the one that creates span.encoded). 