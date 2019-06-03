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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rackspace.salus.resource_management.entities.Resource;
import com.rackspace.salus.resource_management.web.model.ResourceDTO;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;

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
@Slf4j
public class ResourceApiClient implements ResourceApi {

  private static final ParameterizedTypeReference<List<ResourceDTO>> LIST_OF_RESOURCE =
      new ParameterizedTypeReference<List<ResourceDTO>>() {};

  private ObjectMapper objectMapper;
  private final RestTemplate restTemplate;
  private static final String SSEHdr = "data:";

  public ResourceApiClient(ObjectMapper objectMapper, RestTemplate restTemplate) {
    this.objectMapper = objectMapper;
    this.restTemplate = restTemplate;
  }

  @Override
  public ResourceDTO getByResourceId(String tenantId, String resourceId) {
    try {
      return restTemplate.getForObject(
          "/api/tenant/{tenantId}/resources/{resourceId}",
          ResourceDTO.class,
          tenantId, resourceId
      );
    } catch (HttpClientErrorException e) {
      if (e.getStatusCode() == HttpStatus.NOT_FOUND) {
        return null;
      }
      else {
        throw new IllegalArgumentException(e);
      }
    }
  }

  @Override
  public List<ResourceDTO> getResourcesWithLabels(String tenantId, Map<String, String> labels) {
    String endpoint = "/api/tenant/{tenantId}/resourceLabels";
    UriComponentsBuilder uriComponentsBuilder = UriComponentsBuilder.fromUriString(endpoint);
    for (Map.Entry<String, String> e : labels.entrySet()) {
      uriComponentsBuilder.queryParam(e.getKey(), e.getValue());
    }
    String uriString = uriComponentsBuilder.buildAndExpand(tenantId).toUriString();
    ResponseEntity<List<ResourceDTO>> resp = restTemplate.exchange(
        uriString,
        HttpMethod.GET,
        null,
        LIST_OF_RESOURCE
    );

    return resp.getBody();
  }

  @Override
  public List<ResourceDTO> getExpectedEnvoys() {
    String endpoint = "/api/envoys";
    List<ResourceDTO> resources = new ArrayList<>();

    restTemplate.execute(endpoint, HttpMethod.GET, request -> {}, response -> {
      BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(response.getBody()));
      String line;
      while ((line = bufferedReader.readLine()) != null) {
        if (line.length() > SSEHdr.length())
          try {
            ResourceDTO resource;
            // remove the "data:" hdr
            resource = objectMapper.readValue(line.substring(SSEHdr.length()), ResourceDTO.class);
            resources.add(resource);
          } catch (IOException e) {
            log.warn("Failed to parse Resource", e);
          }
      }
      return response;
    });
    return resources;
  }
}
