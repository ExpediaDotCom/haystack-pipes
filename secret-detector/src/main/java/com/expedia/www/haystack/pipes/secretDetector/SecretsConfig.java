package com.expedia.www.haystack.pipes.secretDetector;

import java.util.List;

public interface SecretsConfig {
    String from();

    List<String> tos();

    String host();

    String subject();
}
