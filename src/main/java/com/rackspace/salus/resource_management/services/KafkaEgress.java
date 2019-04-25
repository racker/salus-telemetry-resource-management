/*
 * Copyright 2019 Rackspace US, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.rackspace.salus.resource_management.services;

import com.rackspace.salus.common.messaging.KafkaTopicProperties;
import com.rackspace.salus.telemetry.messaging.KafkaMessageType;
import com.rackspace.salus.telemetry.messaging.ResourceEvent;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class KafkaEgress {

    private final KafkaTemplate<String,Object> kafkaTemplate;
    private final KafkaTopicProperties kafkaTopicProperties;

    @Autowired
    public KafkaEgress(KafkaTemplate<String,Object> kafkaTemplate, KafkaTopicProperties kafkaTopicProperties) {
        this.kafkaTemplate = kafkaTemplate;
        this.kafkaTopicProperties = kafkaTopicProperties;
    }

    public void sendResourceEvent(ResourceEvent event) {
        final String topic = kafkaTopicProperties.getResources();
        if (topic == null) {
            throw new IllegalArgumentException(String.format("No topic configured for %s", KafkaMessageType.RESOURCE));
        }

        String key = String.format("%s:%s", event.getTenantId(), event.getResourceId());
        kafkaTemplate.send(topic, key, event);
    }
}