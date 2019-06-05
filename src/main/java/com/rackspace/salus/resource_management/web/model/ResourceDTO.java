package com.rackspace.salus.resource_management.web.model;

import com.fasterxml.jackson.annotation.JsonView;
import com.rackspace.salus.telemetry.model.View;
import java.util.Map;
import lombok.Data;

@Data
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
}
