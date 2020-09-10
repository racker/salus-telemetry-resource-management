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

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.requestTo;
import static org.springframework.test.web.client.response.MockRestResponseCreators.withStatus;
import static org.springframework.test.web.client.response.MockRestResponseCreators.withSuccess;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rackspace.salus.resource_management.web.model.ResourceDTO;
import com.rackspace.salus.telemetry.model.LabelSelectorMethod;
import com.rackspace.salus.telemetry.repositories.TenantMetadataRepository;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.client.RestClientTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.client.MockRestServiceServer;
import uk.co.jemos.podam.api.PodamFactory;
import uk.co.jemos.podam.api.PodamFactoryImpl;

@RunWith(SpringRunner.class)
@RestClientTest
public class ResourceApiClientTest {

  @TestConfiguration
  public static class ExtraTestConfig {
    @Bean
    public ResourceApiClient resourceApiClient(RestTemplateBuilder restTemplateBuilder) {
      return new ResourceApiClient(objectMapper, restTemplateBuilder.build());
    }
  }

  @Autowired
  MockRestServiceServer mockServer;

  @Autowired
  ResourceApiClient resourceApiClient;

  @MockBean
  TenantMetadataRepository tenantMetadataRepository;

  private PodamFactory podamFactory = new PodamFactoryImpl();

  private static final ObjectMapper objectMapper = new ObjectMapper();

  private static final String SSEHdr = "data:";

  @Test
  public void testGetResourcesWithLabels() throws JsonProcessingException {
    final List<ResourceDTO> expectedResources = IntStream.range(0, 4)
        .mapToObj(value -> podamFactory.manufacturePojo(ResourceDTO.class))
        .collect(Collectors.toList());

    mockServer.expect(requestTo("/api/admin/resources-by-label/t-1/AND?env=prod"))
        .andRespond(withSuccess(
            objectMapper.writeValueAsString(expectedResources), MediaType.APPLICATION_JSON
        ));

    final List<ResourceDTO> resources = resourceApiClient
        .getResourcesWithLabels("t-1", Collections.singletonMap("env", "prod"), LabelSelectorMethod.AND);

    assertThat(resources, equalTo(expectedResources));
  }

  @Test
  public void testGetAllDistinctTenants() throws JsonProcessingException {
    final List<String> tenantIds = podamFactory.manufacturePojo(ArrayList.class, String.class);

    mockServer.expect(requestTo("/api/admin/tenants"))
        .andRespond(withSuccess(
            objectMapper.writeValueAsString(tenantIds), MediaType.APPLICATION_JSON
        ));

    final List<String> result = resourceApiClient
        .getAllDistinctTenantIds();

    assertThat(result, equalTo(tenantIds));
  }
}