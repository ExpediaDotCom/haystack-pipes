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

import com.expedia.www.haystack.pipes.commons.health.HealthController.HealthStatus;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Objects;

import static com.expedia.www.haystack.pipes.commons.health.HealthController.HealthStatus.HEALTHY;
import static java.nio.charset.StandardCharsets.UTF_8;

public class UpdateHealthStatusFile implements HealthStatusListener {

    private final String statusFilePath;

    public UpdateHealthStatusFile(final String statusFilePath) {
        this.statusFilePath = statusFilePath;
    }

    @Override
    public void onChange(HealthStatus status) {
        final boolean isHealthy = (status == HEALTHY);
        try {
            Files.write(Paths.get(statusFilePath), Boolean.toString(isHealthy).getBytes(UTF_8));
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }
}
