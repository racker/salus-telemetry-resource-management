package com.rackspace.salus.resource_management.services;

import com.rackspace.salus.common.messaging.KafkaMessageKeyBuilder;
import com.rackspace.salus.resource_management.config.ResourceManagementProperties;
import com.rackspace.salus.resource_management.types.KafkaMessageType;
import com.rackspace.salus.telemetry.messaging.AttachEvent;
import java.util.Collections;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class AmbassadorSimulator {

  private final KafkaTemplate<String,Object> kafkaTemplate;
  private final ResourceManagementProperties properties;

  @Autowired
  public AmbassadorSimulator(KafkaTemplate<String,Object> kafkaTemplate, ResourceManagementProperties properties) {
    this.kafkaTemplate = kafkaTemplate;
    this.properties = properties;
  }

  @Scheduled(fixedRate = 1000L)
  public void sendAttach() {
    final AttachEvent event = new AttachEvent()
        .setTenantId("t-1")
        .setEnvoyId("e-1")
        .setIdentifierName("hostname")
        .setIdentifierValue("localhost")
        .setEnvoyAddress("127.0.0.1")
        .setLabels(Collections.singletonMap("hostname", "localhost"));

    log.info("SIMULATING: {}", event);

    kafkaTemplate.send(
        properties.getKafkaTopics().get(KafkaMessageType.ATTACH),
        KafkaMessageKeyBuilder.buildMessageKey(event),
        event
    );
  }
}
