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

import static org.mockito.Mockito.verify;

import com.rackspace.salus.common.messaging.KafkaTopicProperties;
import com.rackspace.salus.telemetry.messaging.ReattachedEnvoyResourceEvent;
import com.rackspace.salus.telemetry.messaging.ResourceEvent;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.kafka.core.KafkaTemplate;

@RunWith(MockitoJUnitRunner.class)
public class KafkaEgressTest {

  @Mock
  KafkaTemplate<String,Object> kafkaTemplate;
  private KafkaEgress kafkaEgress;
  private KafkaTopicProperties topicProperties;

  @Before
  public void setUp() {
    topicProperties = new KafkaTopicProperties();
    kafkaEgress = new KafkaEgress(kafkaTemplate, topicProperties);
  }

  @Test
  public void testSendResourceEvent() {
    final ResourceEvent event = new ResourceEvent()
        .setTenantId("t-1")
        .setResourceId("r-1");

    kafkaEgress.sendResourceEvent(event);

    verify(kafkaTemplate).send(topicProperties.getResources(), "t-1:r-1", event);
  }

  @Test
  public void testSendReattachedResourceEvent() {
    final ResourceEvent event = new ReattachedEnvoyResourceEvent()
        .setEnvoyId("e-1")
        .setTenantId("t-1")
        .setResourceId("r-1");

    kafkaEgress.sendResourceEvent(event);

    verify(kafkaTemplate).send(topicProperties.getResources(), "t-1:r-1", event);
  }
}