package com.rackspace.salus.resource_management.web.model;

import com.fasterxml.jackson.annotation.JsonView;
import com.rackspace.salus.telemetry.entities.Resource;
import com.rackspace.salus.common.web.View;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.Map;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class ResourceDTO {
  @JsonView(View.Admin.class)
  Long id;
  @JsonView(View.Internal.class)
  String tenantId;
  @JsonView(View.Admin.class)
  String envoyId;
  @JsonView(View.Internal.class)
  boolean associatedWithEnvoy;
  String resourceId;
  Map<String,String> labels = Collections.emptyMap();
  Map<String,String> metadata = Collections.emptyMap();
  Boolean presenceMonitoringEnabled;
  String createdTimestamp;
  String updatedTimestamp;

  public ResourceDTO(Resource resource, String envoyId) {
    this.id = resource.getId();
    this.tenantId = resource.getTenantId();
    this.resourceId = resource.getResourceId();
    this.labels = resource.getLabels();
    this.metadata = resource.getMetadata();
    this.presenceMonitoringEnabled = resource.getPresenceMonitoringEnabled();
    this.associatedWithEnvoy = envoyId != null;
    this.envoyId = envoyId;
    this.associatedWithEnvoy = envoyId != null;
    this.createdTimestamp = DateTimeFormatter.ISO_INSTANT.format(resource.getCreatedTimestamp());
    this.updatedTimestamp = DateTimeFormatter.ISO_INSTANT.format(resource.getUpdatedTimestamp());
  }
}
