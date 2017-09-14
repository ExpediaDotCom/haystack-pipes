/*
 * Copyright 2017 Expedia, Inc.
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
package com.expedia.www.haystack.pipes;

public interface Constants {
    String SUBSYSTEM = "pipes";
    String APPLICATION = "haystack-external-json-transformer";

    // TODO Move topics to a centralized location to be used by all services
    String KAFKA_FROM_TOPIC = "SpanObject-ProtobufFormat-Topic-1";
    String KAFKA_TO_TOPIC = "SpanObject-JsonFormat-Topic-3";
}
