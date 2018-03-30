package com.expedia.www.haystack.pipes.commons.kafka;

/**
 * Interface for a main() method that can be changed, under configuration control, to point to different Kafka Streams
 * applications for a particular subsystem to run.
 */
public interface Main {
    void main();
}
