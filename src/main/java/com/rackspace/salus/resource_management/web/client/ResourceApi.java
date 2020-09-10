/*
 * Copyright 2020 Rackspace US, Inc.
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

package com.rackspace.salus.resource_management.web.client;

import com.rackspace.salus.resource_management.web.model.ResourceCreate;
import com.rackspace.salus.resource_management.web.model.ResourceDTO;
import com.rackspace.salus.telemetry.model.LabelSelectorMethod;
import java.util.List;
import java.util.Map;
import org.springframework.util.MultiValueMap;

/**
 * This interface declares a subset of internal REST API calls exposed by the Resource Management
 * service.
 *
 * @see ResourceApiClient
 */
public interface ResourceApi {

  List<ResourceDTO> getResourcesWithLabels(String tenantId,
                                        Map<String, String> labels,
                                        LabelSelectorMethod labelSelector);

  List<String> getAllDistinctTenantIds();

  ResourceDTO createResource(String tenantId, ResourceCreate create, MultiValueMap<String, String> headers);
}
