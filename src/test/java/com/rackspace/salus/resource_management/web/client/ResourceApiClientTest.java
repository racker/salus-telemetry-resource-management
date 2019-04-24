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

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.requestTo;
import static org.springframework.test.web.client.response.MockRestResponseCreators.withSuccess;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rackspace.salus.telemetry.model.Resource;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.client.RestClientTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.context.annotation.Bean;
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
      return new ResourceApiClient(restTemplateBuilder.build());
    }
  }

  @Autowired
  MockRestServiceServer mockServer;

  @Autowired
  ObjectMapper objectMapper;

  @Autowired
  ResourceApiClient resourceApiClient;

  private PodamFactory podamFactory = new PodamFactoryImpl();

  @Test
  public void getByResourceId() throws JsonProcessingException {

    Resource expectedResource = podamFactory.manufacturePojo(Resource.class);
    mockServer.expect(requestTo("/api/tenant/t-1/resources/r-1"))
        .andRespond(withSuccess(
            objectMapper.writeValueAsString(expectedResource), MediaType.APPLICATION_JSON
        ));

    final Resource resource = resourceApiClient.getByResourceId("t-1", "r-1");

    assertThat(resource, equalTo(expectedResource));
  }

  @Test
  public void testGetResourcesWithLabels() throws JsonProcessingException {
    final List<Resource> expectedResources = IntStream.range(0, 4)
        .mapToObj(value -> podamFactory.manufacturePojo(Resource.class))
        .collect(Collectors.toList());

    mockServer.expect(requestTo("/api/tenant/t-1/resourceLabels?env=prod"))
        .andRespond(withSuccess(
            objectMapper.writeValueAsString(expectedResources), MediaType.APPLICATION_JSON
        ));

    final List<Resource> resources = resourceApiClient
        .getResourcesWithLabels("t-1", Collections.singletonMap("env", "prod"));

    assertThat(resources, equalTo(expectedResources));
  }
}