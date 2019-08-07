package com.rackspace.salus.resource_management.web.model;

import com.fasterxml.jackson.annotation.JsonView;
import com.rackspace.salus.telemetry.entities.Resource;
import com.rackspace.salus.telemetry.model.View;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class ResourceDTO {
  @JsonView(View.Admin.class)
  Long id;
  @JsonView(View.Public.class) // This will be changed to Internal or Admin when we switch to role based views.
  String tenantId;
  String resourceId;
  Map<String,String> labels;
  Map<String,String> metadata;
  Boolean presenceMonitoringEnabled;
  String region;
  boolean associatedWithEnvoy;
  String createdTimestamp;
  String updatedTimestamp;

  public ResourceDTO(Resource resource) {
    this.id = resource.getId();
    this.tenantId = resource.getTenantId();
    this.resourceId = resource.getResourceId();
    this.labels = resource.getLabels();
    this.metadata = resource.getMetadata();
    this.presenceMonitoringEnabled = resource.getPresenceMonitoringEnabled();
    this.region = resource.getRegion();
    this.associatedWithEnvoy = resource.isAssociatedWithEnvoy();
    this.createdTimestamp = DateTimeFormatter.ISO_INSTANT.format(resource.getCreatedTimestamp());
    this.updatedTimestamp = DateTimeFormatter.ISO_INSTANT.format(resource.getUpdatedTimestamp());
  }
}
