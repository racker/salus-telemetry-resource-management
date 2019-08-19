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

import static com.rackspace.salus.resource_management.TestUtils.readContent;
import static com.rackspace.salus.test.WebTestUtils.validationError;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.stringContainsInOrder;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
import static org.springframework.test.web.servlet.result.MockMvcResultHandlers.print;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rackspace.salus.telemetry.entities.Resource;
import com.rackspace.salus.resource_management.services.ResourceManagement;
import com.rackspace.salus.resource_management.web.model.ResourceCreate;
import com.rackspace.salus.resource_management.web.model.ResourceDTO;
import com.rackspace.salus.resource_management.web.model.ResourceUpdate;
import com.rackspace.salus.telemetry.errors.AlreadyExistsException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.orm.jpa.AutoConfigureDataJpa;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.http.MediaType;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import uk.co.jemos.podam.api.PodamFactory;
import uk.co.jemos.podam.api.PodamFactoryImpl;

@RunWith(SpringRunner.class)
@WebMvcTest(controllers = ResourceApiController.class)
public class ResourceApiControllerTest {

  // A timestamp to be used in tests that translates to "1970-01-02T03:46:40Z"
  private static final Instant DEFAULT_TIMESTAMP = Instant.ofEpochSecond(100000);

  PodamFactory podamFactory = new PodamFactoryImpl();

  @Autowired
  MockMvc mockMvc;

  @MockBean
  ResourceManagement resourceManagement;

  @Autowired
  ObjectMapper objectMapper;

  @Test
  public void testGetByResourceId() throws Exception {

    final Resource expectedResource = new Resource()
        .setLabels(Collections.singletonMap("env", "prod"))
        .setMetadata(Collections.singletonMap("custom", "new"))
        .setResourceId("r-1")
        .setTenantId("t-1")
        .setCreatedTimestamp(DEFAULT_TIMESTAMP)
        .setUpdatedTimestamp(DEFAULT_TIMESTAMP)
        .setId(1001L);
    when(resourceManagement.getResource(any(), any()))
        .thenReturn(Optional.of(expectedResource));

    mockMvc.perform(get(
        "/api/tenant/{tenantId}/resources/{resourceId}",
        "t-1", "r-1"
    ).accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk())
        .andExpect(content().json(
            // id field should not be returned
            readContent("ResourceApiControllerTest/single_public_resource.json"), true));

    verify(resourceManagement).getResource("t-1", "r-1");
    verifyNoMoreInteractions(resourceManagement);
  }

  @Test
  public void testNoResourceFound() throws Exception {
    when(resourceManagement.getResource(anyString(), anyString()))
        .thenReturn(Optional.empty());

    String tenantId = RandomStringUtils.randomAlphabetic( 8 );
    String resourceId = RandomStringUtils.randomAlphabetic( 8 );
    String errorMsg = String.format("No resource found for %s on tenant %s", resourceId, tenantId);

    mockMvc.perform(get("/api/tenant/{tenantId}/resources/{resourceId}", tenantId, resourceId)
        .contentType(MediaType.APPLICATION_JSON))
        .andExpect(status().isNotFound())
        .andExpect(content()
            .contentTypeCompatibleWith(MediaType.APPLICATION_JSON))
        .andExpect(jsonPath("$.message", is(errorMsg)));

    verify(resourceManagement).getResource(tenantId, resourceId);
    verifyNoMoreInteractions(resourceManagement);
  }

  @Test
  public void testGetAllForTenant() throws Exception {
    int numberOfResources = 20;
    // Use the APIs default Pageable settings
    int page = 0;
    int pageSize = 20;
    List<Resource> resources = new ArrayList<>();
    for (int i=0; i<numberOfResources; i++) {
      resources.add(podamFactory.manufacturePojo(Resource.class));
    }

    int start = page * pageSize;
    int end = numberOfResources;
    Page<Resource> pageOfResources = new PageImpl<>(resources.subList(start, end),
        PageRequest.of(page, pageSize),
        numberOfResources);

    when(resourceManagement.getResources(anyString(), any()))
        .thenReturn(pageOfResources);

    String tenantId = RandomStringUtils.randomAlphabetic( 8 );

    mockMvc.perform(get("/api/tenant/{tenantId}/resources", tenantId)
        .contentType(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk())
        .andExpect(content()
            .contentTypeCompatibleWith(MediaType.APPLICATION_JSON))
        .andExpect(jsonPath("$.content.*", hasSize(numberOfResources)))
        .andExpect(jsonPath("$.totalPages", equalTo(1)))
        .andExpect(jsonPath("$.totalElements", equalTo(numberOfResources)));

    verify(resourceManagement).getResources(tenantId, PageRequest.of(0, 20));
    verifyNoMoreInteractions(resourceManagement);
  }

  @Test
  public void testGetAllForTenantPagination() throws Exception {
    int numberOfResources = 99;
    int pageSize = 4;
    int page = 14;
    List<Resource> resources = new ArrayList<>();
    for (int i=0; i<numberOfResources; i++) {
      resources.add(podamFactory.manufacturePojo(Resource.class));
    }
    int start = page * pageSize;
    int end = start + pageSize;
    Page<Resource> pageOfResources = new PageImpl<>(resources.subList(start, end),
        PageRequest.of(page, pageSize),
        numberOfResources);

    assertThat(pageOfResources.getContent().size(), equalTo(pageSize));

    when(resourceManagement.getResources(anyString(), any()))
        .thenReturn(pageOfResources);

    String tenantId = RandomStringUtils.randomAlphabetic( 8 );

    mockMvc.perform(get("/api/tenant/{tenantId}/resources", tenantId)
        .contentType(MediaType.APPLICATION_JSON)
        .param("page", Integer.toString(page))
        .param("size", Integer.toString(pageSize)))
        .andExpect(status().isOk())
        .andExpect(content()
            .contentTypeCompatibleWith(MediaType.APPLICATION_JSON))
        .andExpect(jsonPath("$.content.*", hasSize(pageSize)))
        .andExpect(jsonPath("$.totalPages", equalTo((numberOfResources + pageSize - 1) / pageSize)))
        .andExpect(jsonPath("$.totalElements", equalTo(numberOfResources)));

    verify(resourceManagement).getResources(tenantId, PageRequest.of(page, pageSize));
    verifyNoMoreInteractions(resourceManagement);
  }

  @Test
  public void testCreateResource() throws Exception {
    Resource resource = podamFactory.manufacturePojo(Resource.class);
    when(resourceManagement.createResource(anyString(), any()))
        .thenReturn(resource);

    String tenantId = RandomStringUtils.randomAlphabetic( 8 );

    ResourceCreate create = podamFactory.manufacturePojo(ResourceCreate.class);

    mockMvc.perform(post("/api/tenant/{tenantId}/resources", tenantId)
        .content(objectMapper.writeValueAsString(create))
        .contentType(MediaType.APPLICATION_JSON)
        .characterEncoding(StandardCharsets.UTF_8.name()))
        .andExpect(status().isCreated())
        .andExpect(content()
            .contentTypeCompatibleWith(MediaType.APPLICATION_JSON));

    verify(resourceManagement).createResource(tenantId, create);
    verifyNoMoreInteractions(resourceManagement);
  }

  @Test
  public void testCreateDuplicateResource() throws Exception {
    String error = "Zone already exists with name z-1 on tenant t-1";
    when(resourceManagement.createResource(anyString(), any()))
        .thenThrow(new AlreadyExistsException(error));

    String tenantId = RandomStringUtils.randomAlphabetic( 8 );
    ResourceCreate create = podamFactory.manufacturePojo(ResourceCreate.class);

    mockMvc.perform(post("/api/tenant/{tenantId}/resources", tenantId)
        .content(objectMapper.writeValueAsString(create))
        .contentType(MediaType.APPLICATION_JSON)
        .characterEncoding(StandardCharsets.UTF_8.name()))
        .andExpect(status().isUnprocessableEntity());

    verify(resourceManagement).createResource(tenantId, create);
    verifyNoMoreInteractions(resourceManagement);
  }

  @Test
  public void testCreateResourceWithoutIdField() throws Exception {
    String tenantId = RandomStringUtils.randomAlphabetic( 8 );

    ResourceCreate create = podamFactory.manufacturePojo(ResourceCreate.class);
    create.setResourceId(null);

    mockMvc.perform(post("/api/tenant/{tenantId}/resources", tenantId)
        .content(objectMapper.writeValueAsString(create))
        .contentType(MediaType.APPLICATION_JSON)
        .characterEncoding(StandardCharsets.UTF_8.name()))
        .andExpect(status().isBadRequest())
        .andExpect(validationError(
            "resourceId", "may not be empty"
        ));

    verifyNoMoreInteractions(resourceManagement);
  }

  @Test
  public void testCreateResourceWithoutPresenceMonitoringField() throws Exception {
    String tenantId = RandomStringUtils.randomAlphabetic( 8 );

    ResourceCreate create = podamFactory.manufacturePojo(ResourceCreate.class);
    create.setPresenceMonitoringEnabled(null);

    mockMvc.perform(post("/api/tenant/{tenantId}/resources", tenantId)
        .content(objectMapper.writeValueAsString(create))
        .contentType(MediaType.APPLICATION_JSON)
        .characterEncoding(StandardCharsets.UTF_8.name()))
        .andExpect(status().isBadRequest())
        .andExpect(validationError(
            "presenceMonitoringEnabled", "must not be null"));

    verifyNoMoreInteractions(resourceManagement);
  }

  @Test
  public void testUpdateResource() throws Exception {
    Resource resource = podamFactory.manufacturePojo(Resource.class);
    when(resourceManagement.updateResource(anyString(), anyString(), any()))
        .thenReturn(resource);

    String tenantId = resource.getTenantId();
    String resourceId = resource.getResourceId();

    ResourceUpdate update = podamFactory.manufacturePojo(ResourceUpdate.class);

    mockMvc.perform(put("/api/tenant/{tenantId}/resources/{resourceId}", tenantId, resourceId)
        .content(objectMapper.writeValueAsString(update))
        .contentType(MediaType.APPLICATION_JSON)
        .characterEncoding(StandardCharsets.UTF_8.name()))
        .andExpect(status().isOk())
        .andExpect(content()
            .contentTypeCompatibleWith(MediaType.APPLICATION_JSON));

    verify(resourceManagement).updateResource(tenantId, resourceId, update);
    verifyNoMoreInteractions(resourceManagement);
  }

  @Test
  public void testGetAll() throws Exception {
    int numberOfResources = 17;
    // Use the APIs default Pageable settings
    int page = 0;
    int pageSize = 20;
    List<Resource> resources = new ArrayList<>();
    for (int i=0; i<numberOfResources; i++) {
      resources.add(podamFactory.manufacturePojo(Resource.class));
    }

    int start = page * pageSize;
    int end = numberOfResources;
    Page<Resource> pageOfResources = new PageImpl<>(resources.subList(start, end),
        PageRequest.of(page, pageSize),
        numberOfResources);

    when(resourceManagement.getAllResources(any()))
        .thenReturn(pageOfResources);

    mockMvc.perform(get("/api/admin/resources")
        .contentType(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk())
        .andExpect(content()
            .contentTypeCompatibleWith(MediaType.APPLICATION_JSON))
        .andExpect(jsonPath("$.content.*", hasSize(numberOfResources)))
        .andExpect(jsonPath("$.totalPages", equalTo(1)))
        .andExpect(jsonPath("$.totalElements", equalTo(numberOfResources)));

    verify(resourceManagement).getAllResources(PageRequest.of(0, 20));
    verifyNoMoreInteractions(resourceManagement);
  }

  @Test
  public void testGetStreamOfResources() throws Exception {
    int numberOfResources = 20;
    List<Resource> resources = new ArrayList<>();
    for (int i=0; i<numberOfResources; i++) {
      resources.add(podamFactory.manufacturePojo(Resource.class));
    }

    List<String> expectedData = resources.stream()
        .map(r -> {
          try {
            return "data:" + objectMapper.writeValueAsString(new ResourceDTO(r));
          } catch (JsonProcessingException e) {
            assertThat(e, nullValue());
            return null;
          }
        }).collect(Collectors.toList());
    assertThat(expectedData.size(), equalTo(resources.size()));

    Stream<Resource> resourceStream = resources.stream();

    when(resourceManagement.getResources(true))
        .thenReturn(resourceStream);

    mockMvc.perform(get("/api/envoys"))
        .andDo(print())
        .andExpect(status().isOk())
        .andExpect(content().contentTypeCompatibleWith("text/event-stream;charset=UTF-8"))
        .andExpect(content().string(stringContainsInOrder(expectedData)));

    verify(resourceManagement).getResources(true);
    verifyNoMoreInteractions(resourceManagement);
  }

  @Test
  public void testGetResourcesWithLabels() throws Exception {

    final List<Resource> expectedResources = IntStream.range(0, 4)
        .mapToObj(value -> podamFactory.manufacturePojo(Resource.class))
        .collect(Collectors.toList());

    when(resourceManagement.getResourcesFromLabels(any(), any(), any()))
        .thenReturn(new PageImpl<>(expectedResources, Pageable.unpaged(), expectedResources.size()));

    mockMvc.perform(get(
        "/api/tenant/{tenantId}/resources-by-label?env=prod",
        "t-1"
    ).accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk());

    verify(resourceManagement).getResourcesFromLabels(
        Collections.singletonMap("env", "prod"),
        "t-1",
        PageRequest.of(0, 20)
    );

    verifyNoMoreInteractions(resourceManagement);
  }

  @Test
  public void testGetResourceLabels() throws Exception {
    final MultiValueMap<String, String> expected = new LinkedMultiValueMap<>();
    expected.put("agent_discovered_os", Arrays.asList("linux", "darwin", "windows"));
    expected.put("agent_discovered_arch", Arrays.asList("amd64", "386"));
    expected.put("cluster", Arrays.asList("dev", "prod"));

    when(resourceManagement.getTenantResourceLabels(any()))
        .thenReturn(expected);

    mockMvc.perform(get(
        "/api/tenant/{tenantId}/resource-labels",
        "t-1"
    ).accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk())
        .andExpect(
            content().json(readContent("/ResourceApiControllerTest/resource_labels.json"), true));

    verify(resourceManagement).getTenantResourceLabels("t-1");

    verifyNoMoreInteractions(resourceManagement);
  }

  @Test
  public void testGetResourceMetadataKeys() throws Exception {
    final List<String> expected = Arrays.asList("key1", "key2", "key3");

    when(resourceManagement.getTenantResourceMetadataKeys(any()))
        .thenReturn(expected);

    mockMvc.perform(get(
        "/api/tenant/{tenantId}/resource-metadata-keys",
        "t-1"
    ).accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk())
        .andExpect(
            content().json(readContent("/ResourceApiControllerTest/resource_metadata_keys.json"), true));

    verify(resourceManagement).getTenantResourceMetadataKeys("t-1");

    verifyNoMoreInteractions(resourceManagement);
  }

  @Test
  public void testGetLabelNamespaces() throws Exception {
    final List<String> expected = Arrays.asList("agent", "system");

    when(resourceManagement.getLabelNamespaces())
        .thenReturn(expected);

    mockMvc.perform(get(
        "/api/tenant/{tenantId}/resource-label-namespaces",
        "t-1"
    ).accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk())
        .andExpect(
            content().json(readContent("/ResourceApiControllerTest/label_namespaces.json"), true));

    verify(resourceManagement).getLabelNamespaces();

    verifyNoMoreInteractions(resourceManagement);
  }
}