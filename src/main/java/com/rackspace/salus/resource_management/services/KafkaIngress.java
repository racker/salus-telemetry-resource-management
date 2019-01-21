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

package com.rackspace.salus.resource_management.services.services;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rackspace.salus.resource_management.services.config.ResourceManagementProperties;
import com.rackspace.salus.resource_management.services.types.KafkaMessageType;
import com.rackspace.salus.telemetry.events.AttachEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class KafkaIngress {

    private final ObjectMapper objectMapper;
    private final ResourceManagementProperties properties;
    private final ResourceManagement resourceManagement;
    private final String topic;

    @Autowired
    public KafkaIngress(ObjectMapper objectMapper, ResourceManagementProperties properties, ResourceManagement resourceManagement) {
        this.objectMapper = objectMapper;
        this.properties = properties;
        this.resourceManagement = resourceManagement;
        this.topic = this.properties.getKafkaTopics().get(KafkaMessageType.ATTACH);
    }

    /**
     * This method is used by the __listener.topic magic in the KafkaListener
     * @return The topic to consume
     */
    public String getTopic() {
        return this.topic;
    }

    /**
     * This receives an envoy attach event from Kafka and passes it to the resource manager to do whatever is needed.
     * @param cr The AttachEvent read from Kafka.
     * @throws Exception
     */
    @KafkaListener(topics = "#{__listener.topic}")
    //@KafkaListener(topics = "#{'${resource-management.topics}'.split(',')}")
    public void consumeAttachEvents(ConsumerRecord<String, String> cr) throws Exception {
        log.info("Received new resource event from {}: {}", cr.toString(), cr.topic());
        String value = cr.value();
        AttachEvent attachEvent = objectMapper.readValue(value, AttachEvent.class);
        log.info("Processing new attach event: {}", attachEvent.toString());
        resourceManagement.handleEnvoyAttach(attachEvent);
    }
}
