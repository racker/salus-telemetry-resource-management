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

package com.rackspace.salus.resource_management.web.controller;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rackspace.salus.resource_management.services.ResourceManagement;
import com.rackspace.salus.telemetry.model.Resource;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Bean;
import org.springframework.core.task.SyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;
import org.springframework.http.MediaType;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.servlet.MockMvc;
import uk.co.jemos.podam.api.PodamFactory;
import uk.co.jemos.podam.api.PodamFactoryImpl;

@RunWith(SpringRunner.class)
@WebMvcTest(ResourceApiController.class)
public class ResourceApiControllerTest {

  @TestConfiguration
  public static class ExtraTestConfig {
    @Bean
    TaskExecutor taskExecutor() {
      return new SyncTaskExecutor();
    }
  }

  @Autowired
  private MockMvc mvc;

  @Autowired
  private ObjectMapper objectMapper;

  @MockBean
  private ResourceManagement resourceManagement;

  private PodamFactory podamFactory = new PodamFactoryImpl();

  @Test
  public void getByResourceId() throws Exception {

    final Resource expectedResource = podamFactory.manufacturePojo(Resource.class);
    when(resourceManagement.getResource(any(), any()))
        .thenReturn(expectedResource);

    mvc.perform(get(
        "/api/tenant/{tenantId}/resources/{resourceId}",
        "t-1", "r-1"
    ).accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk())
        .andExpect(content().json(objectMapper.writeValueAsString(expectedResource)));

  }

  @Test
  public void testGetResourcesWithLabels() throws Exception {

    final List<Resource> expectedResources = IntStream.range(0, 4)
        .mapToObj(value -> podamFactory.manufacturePojo(Resource.class))
        .collect(Collectors.toList());

    when(resourceManagement.getResourcesFromLabels(any(), any()))
        .thenReturn(expectedResources);

    mvc.perform(get(
        "/api/tenant/{tenantId}/resourceLabels?env=prod",
        "t-1"
    ).accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk())
        .andExpect(content().json(objectMapper.writeValueAsString(expectedResources)));

    verify(resourceManagement).getResourcesFromLabels(
        Collections.singletonMap("env", "prod"),
        "t-1"
    );
  }

  @Test
  public void testGetResourcesWithPresenceMonitoringEnabled() throws Exception {
    final List<Resource> expectedResources = IntStream.range(0, 4)
            .mapToObj(value -> podamFactory.manufacturePojo(Resource.class).setPresenceMonitoringEnabled(true))
            .collect(Collectors.toList());

    when(resourceManagement.getExpectedEnvoys())
            .thenReturn(expectedResources.stream());

    mvc.perform(get(
            "/api/envoys"
    ).accept(MediaType.APPLICATION_JSON))
            .andExpect(status().isOk())
            .andExpect(content().json(objectMapper.writeValueAsString(expectedResources)));

    verify(resourceManagement).getExpectedEnvoys();
  }
}