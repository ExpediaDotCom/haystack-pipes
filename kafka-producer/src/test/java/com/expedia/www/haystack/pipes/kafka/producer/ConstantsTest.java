package com.expedia.www.haystack.pipes.kafka.producer;

import org.junit.Test;

import static org.junit.Assert.*;

public class ConstantsTest {

    @Test
    public void testConstants(){
        assertEquals("haystack-pipes-kafka-producer",Constants.APPLICATION);
    }

}