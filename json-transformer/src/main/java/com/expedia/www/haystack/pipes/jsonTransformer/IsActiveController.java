package com.expedia.www.haystack.pipes.jsonTransformer;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.web.support.SpringBootServletInitializer;

/**
 * A very simple Spring Boot application that is intended to support only a single REST endpoint (index.html)
 * to indicate that the JVM is running.
 */
@SpringBootApplication
public class IsActiveController extends SpringBootServletInitializer {
    static Factory factory = new Factory();

    public static void main(String[] args) {
        final ProtobufToJsonTransformer protobufToJsonTransformer = factory.createProtobufToJsonTransformer();
        protobufToJsonTransformer.main();
        final SpringApplication springApplication = factory.createSpringApplication();
        springApplication.run(args);
    }

    static class Factory {
        ProtobufToJsonTransformer createProtobufToJsonTransformer() {
            return new ProtobufToJsonTransformer();
        }

        SpringApplication createSpringApplication() {
            final Object[] sources = {IsActiveController.class};
            return new SpringApplication(sources);
        }
    }
}
