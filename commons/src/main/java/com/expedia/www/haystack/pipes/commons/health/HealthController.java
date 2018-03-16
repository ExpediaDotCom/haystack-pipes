/*
 * Copyright 2018 Expedia, Inc.
 *
 *       Licensed under the Apache License, Version 2.0 (the "License");
 *       you may not use this file except in compliance with the License.
 *       You may obtain a copy of the License at
 *
 *           http://www.apache.org/licenses/LICENSE-2.0
 *
 *       Unless required by applicable law or agreed to in writing, software
 *       distributed under the License is distributed on an "AS IS" BASIS,
 *       WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *       See the License for the specific language governing permissions and
 *       limitations under the License.
 *
 */

package com.expedia.www.haystack.pipes.commons.health;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static com.expedia.www.haystack.pipes.commons.health.HealthController.HealthStatus.HEALTHY;
import static com.expedia.www.haystack.pipes.commons.health.HealthController.HealthStatus.NOT_SET;
import static com.expedia.www.haystack.pipes.commons.health.HealthController.HealthStatus.UNHEALTHY;

public class HealthController {

    public enum HealthStatus {
        HEALTHY,
        UNHEALTHY,
        NOT_SET
    }

    private final AtomicReference<HealthStatus> status;
    private final List<HealthStatusListener> listeners;

    public HealthController() {
        this.status = new AtomicReference<>(NOT_SET);
        this.listeners = new ArrayList<>();
    }

    public void setHealthy() {
        if(status.getAndSet(HEALTHY) != HEALTHY) {
            notifyChange(HEALTHY);
        }
    }

    public void setUnhealthy() {
        if(status.getAndSet(UNHEALTHY) != UNHEALTHY) {
            notifyChange(UNHEALTHY);
        }
    }

    public synchronized void addListener(final HealthStatusListener listener) {
        listeners.add(listener);
    }

    private void notifyChange(HealthStatus status) {
        listeners.forEach(l -> l.onChange(status));
    }

    public HealthStatus getStatus() {
        return status.get();
    }
}
