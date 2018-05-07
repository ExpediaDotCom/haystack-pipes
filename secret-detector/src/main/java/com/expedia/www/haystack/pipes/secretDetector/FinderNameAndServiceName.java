package com.expedia.www.haystack.pipes.secretDetector;

import java.util.Objects;

public class FinderNameAndServiceName {
    private final String finderName;
    private final String serviceName;

    FinderNameAndServiceName(String finderName, String serviceName) {
        this.finderName = finderName;
        this.serviceName = serviceName;
    }

    String getFinderName() {
        return finderName;
    }

    String getServiceName() {
        return serviceName;
    }

    // equals and hashCode are overridden with this IDE-created code so that FinderNameAndServiceName objects can
    // be the key in the static Detector.COUNTERS object.
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FinderNameAndServiceName that = (FinderNameAndServiceName) o;
        return Objects.equals(finderName, that.finderName) &&
                Objects.equals(serviceName, that.serviceName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(finderName, serviceName);
    }
}
