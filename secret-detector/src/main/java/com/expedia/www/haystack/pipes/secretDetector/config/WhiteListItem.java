package com.expedia.www.haystack.pipes.secretDetector.config;

import java.util.Objects;

public class WhiteListItem {
    public final String finderName;
    public final String serviceName;
    public final String operationName;
    public final String tagName;

    public WhiteListItem(String finderName, String serviceName, String operationName, String tagName) {
        this.finderName = finderName;
        this.serviceName = serviceName;
        this.operationName = operationName;
        this.tagName = tagName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        WhiteListItem that = (WhiteListItem) o;
        return Objects.equals(finderName, that.finderName) &&
                Objects.equals(serviceName, that.serviceName) &&
                Objects.equals(operationName, that.operationName) &&
                Objects.equals(tagName, that.tagName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(finderName, serviceName, operationName, tagName);
    }
}
