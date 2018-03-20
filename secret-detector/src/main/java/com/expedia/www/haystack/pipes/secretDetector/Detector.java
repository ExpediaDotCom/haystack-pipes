package com.expedia.www.haystack.pipes.secretDetector;

import com.expedia.open.tracing.Log;
import com.expedia.open.tracing.Span;
import com.expedia.open.tracing.Tag;
import io.dataapps.chlorine.finder.FinderEngine;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;

public class Detector {
    private final FinderEngine finderEngine;

    Detector(FinderEngine finderEngine) {
        this.finderEngine = finderEngine;
    }

    List<String> findSecrets(Span span) {
        final List<String> secrets = new ArrayList<>();
        findSecretsInTags(secrets, span);
        findSecretsInLogs(secrets, span);
        return secrets;
    }

    private void findSecretsInTags(List<String> secrets, Span span) {
        findSecrets(secrets, span.getTagsList());
    }

    private void findSecretsInLogs(List<String> secrets, Span span) {
        for(final Log log : span.getLogsList()) {
            findSecrets(secrets, log.getFieldsList());
        }
    }

    private void findSecrets(List<String> secrets, List<Tag> tags) {
        for(final Tag tag : tags) {
            if(StringUtils.isNotEmpty(tag.getVStr())) {
                secrets.addAll(finderEngine.find(tag.getVStr()));
            } else if(tag.getVBytes().size() > 0) {
                secrets.addAll(finderEngine.find(new String(tag.getVBytes().toByteArray())));
            }
        }
    }

}