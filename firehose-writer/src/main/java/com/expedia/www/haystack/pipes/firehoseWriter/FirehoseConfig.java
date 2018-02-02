package com.expedia.www.haystack.pipes.firehoseWriter;

public interface FirehoseConfig {
    int retrycount();

    String url();

    String streamname();

    String signingregion();
}
