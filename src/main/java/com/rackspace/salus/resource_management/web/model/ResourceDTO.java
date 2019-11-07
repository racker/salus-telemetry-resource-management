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
  //@JsonView(View.Public.class) // This will be changed to Internal or Admin when we switch to role based views.
  // Disabling this until we have a way for our internal requests to bypass that view (this might need new endpoints)
  String tenantId;
  String resourceId;
  Map<String,String> labels;
  Map<String,Object> metadata;
  Boolean presenceMonitoringEnabled;
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
    this.associatedWithEnvoy = resource.isAssociatedWithEnvoy();
    this.createdTimestamp = DateTimeFormatter.ISO_INSTANT.format(resource.getCreatedTimestamp());
    this.updatedTimestamp = DateTimeFormatter.ISO_INSTANT.format(resource.getUpdatedTimestamp());
  }
}
