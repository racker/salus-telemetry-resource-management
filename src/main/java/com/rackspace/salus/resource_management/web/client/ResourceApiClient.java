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

package com.rackspace.salus.resource_management.web.client;

import com.rackspace.salus.telemetry.model.Resource;
import org.springframework.web.client.RestTemplate;

/**
 * This client component provides a small subset of Resource Management REST operations that
 * can be called internally by other microservices in Salus.
 *
 * <p>
 *   It is required that the {@link RestTemplate} provided to this instance has been
 *   configured with the appropriate root URI for locating the resource management service.
 *   The following is an example of a configuration bean that does that:
 * </p>
 *
 * <pre>
 {@literal @}Configuration
 public class RestClientsConfig {

   {@literal @}Bean
   public ResourceApi resourceApi(RestTemplateBuilder restTemplateBuilder) {
     return new ResourceApiClient(
       restTemplateBuilder
       .rootUri("http://localhost:8085")
       .build()
     );
   }
 }
 * </pre>
 *
 */
public class ResourceApiClient implements ResourceApi {

  private final RestTemplate restTemplate;

  public ResourceApiClient(RestTemplate restTemplate) {
    this.restTemplate = restTemplate;
  }

  @Override
  public Resource getByResourceId(String tenantId, String resourceId) {
    return restTemplate.getForObject(
        "/api/tenant/{tenantId}/resources/{resourceId}",
        Resource.class,
        tenantId, resourceId
    );
  }
}
