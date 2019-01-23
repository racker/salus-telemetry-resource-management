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

import com.rackspace.salus.resource_management.config.ResourceManagementProperties;
import com.rackspace.salus.telemetry.messaging.KafkaMessageType;
import com.rackspace.salus.telemetry.messaging.ResourceEvent;
import com.rackspace.salus.telemetry.model.Resource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class KafkaEgress {

    private final KafkaTemplate<String,Object> kafkaTemplate;
    private final ResourceManagementProperties properties;

    @Autowired
    public KafkaEgress(KafkaTemplate<String,Object> kafkaTemplate, ResourceManagementProperties properties) {
        this.kafkaTemplate = kafkaTemplate;
        this.properties= properties;
    }

    public void sendResourceEvent(ResourceEvent event) {
        final String topic = properties.getKafkaTopics().get(KafkaMessageType.RESOURCE);
        if (topic == null) {
            throw new IllegalArgumentException(String.format("No topic configured for %s", KafkaMessageType.RESOURCE));
        }

        Resource resource = event.getResource();
        String key = String.format("%s:%s", resource.getTenantId(), resource.getResourceId());
        kafkaTemplate.send(topic, key, event);
    }
}