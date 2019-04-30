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

package com.rackspace.salus.resource_management;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.stringContainsInOrder;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
import static org.springframework.test.web.servlet.result.MockMvcResultHandlers.print;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rackspace.salus.resource_management.services.ResourceManagement;
import com.rackspace.salus.resource_management.web.controller.ResourceApiController;
import com.rackspace.salus.resource_management.web.model.ResourceCreate;
import com.rackspace.salus.resource_management.web.model.ResourceUpdate;
import com.rackspace.salus.telemetry.model.Resource;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
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
import org.springframework.http.MediaType;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import uk.co.jemos.podam.api.PodamFactory;
import uk.co.jemos.podam.api.PodamFactoryImpl;

@RunWith(SpringRunner.class)
@WebMvcTest(controllers = ResourceApiController.class)
@AutoConfigureDataJpa
public class ResourceApiTest {

    PodamFactory podamFactory = new PodamFactoryImpl();

    @Autowired
    MockMvc mockMvc;

    @MockBean
    ResourceManagement resourceManagement;

    @Autowired
    ObjectMapper objectMapper;

    @Test
    public void testGetResource() throws Exception {
        Resource resource = podamFactory.manufacturePojo(Resource.class);
        when(resourceManagement.getResource(anyString(), anyString()))
                .thenReturn(resource);

        String tenantId = RandomStringUtils.randomAlphabetic( 8 );
        String resourceId = RandomStringUtils.randomAlphabetic( 8 );
        String url = String.format("/api/tenant/%s/resources/%s", tenantId, resourceId);

        mockMvc.perform(get(url).contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(content()
                        .contentTypeCompatibleWith(MediaType.APPLICATION_JSON))
                .andExpect(jsonPath("$.resourceId", is(resource.getResourceId())));
    }

    @Test
    public void testNoResourceFound() throws Exception {
        when(resourceManagement.getResource(anyString(), anyString()))
                .thenReturn(null);

        String tenantId = RandomStringUtils.randomAlphabetic( 8 );
        String resourceId = RandomStringUtils.randomAlphabetic( 8 );
        String url = String.format("/api/tenant/%s/resources/%s", tenantId, resourceId);
        String errorMsg = String.format("No resource found for %s on tenant %s", resourceId, tenantId);

        mockMvc.perform(get(url).contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isNotFound())
                .andExpect(content()
                        .contentTypeCompatibleWith(MediaType.APPLICATION_JSON))
                .andExpect(jsonPath("$.message", is(errorMsg)));
    }

    @Test
    public void testGetAllForTenant() throws Exception {
        int numberOfResources = 20;
        // Use the APIs default Pageable settings
        int page = 0;
        int pageSize = 100;
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
        String url = String.format("/api/tenant/%s/resources", tenantId);

        mockMvc.perform(get(url).contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(content()
                        .contentTypeCompatibleWith(MediaType.APPLICATION_JSON))
                .andExpect(content().string(objectMapper.writeValueAsString(pageOfResources)))
                .andExpect(jsonPath("$.content.*", hasSize(numberOfResources)))
                .andExpect(jsonPath("$.totalPages", equalTo(1)))
                .andExpect(jsonPath("$.numberOfElements", equalTo(numberOfResources)))
                .andExpect(jsonPath("$.totalElements", equalTo(numberOfResources)))
                .andExpect(jsonPath("$.pageable.pageNumber", equalTo(page)))
                .andExpect(jsonPath("$.pageable.pageSize", equalTo(pageSize)))
                .andExpect(jsonPath("$.size", equalTo(pageSize)));
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
        String url = String.format("/api/tenant/%s/resources", tenantId);

        mockMvc.perform(get(url).contentType(MediaType.APPLICATION_JSON)
                .param("page", Integer.toString(page))
                .param("size", Integer.toString(pageSize)))
                .andExpect(status().isOk())
                .andExpect(content()
                        .contentTypeCompatibleWith(MediaType.APPLICATION_JSON))
                .andExpect(content().string(objectMapper.writeValueAsString(pageOfResources)))
                .andExpect(jsonPath("$.content.*", hasSize(pageSize)))
                .andExpect(jsonPath("$.totalPages", equalTo((numberOfResources + pageSize - 1) / pageSize)))
                .andExpect(jsonPath("$.numberOfElements", equalTo(pageSize)))
                .andExpect(jsonPath("$.totalElements", equalTo(numberOfResources)))
                .andExpect(jsonPath("$.pageable.pageNumber", equalTo(page)))
                .andExpect(jsonPath("$.pageable.pageSize", equalTo(pageSize)))
                .andExpect(jsonPath("$.size", equalTo(pageSize)));
    }

    @Test
    public void testCreateResource() throws Exception {
        Resource resource = podamFactory.manufacturePojo(Resource.class);
        when(resourceManagement.createResource(anyString(), any()))
                .thenReturn(resource);

        String tenantId = RandomStringUtils.randomAlphabetic( 8 );
        String url = String.format("/api/tenant/%s/resources", tenantId);
        ResourceCreate create = podamFactory.manufacturePojo(ResourceCreate.class);

        mockMvc.perform(post(url)
                .content(objectMapper.writeValueAsString(create))
                .contentType(MediaType.APPLICATION_JSON)
                .characterEncoding(StandardCharsets.UTF_8.name()))
                .andExpect(status().isCreated())
                .andExpect(content()
                        .contentTypeCompatibleWith(MediaType.APPLICATION_JSON));
    }

    @Test
    public void testCreateResourceWithoutIdField() throws Exception {
        String tenantId = RandomStringUtils.randomAlphabetic( 8 );
        String url = String.format("/api/tenant/%s/resources", tenantId);

        ResourceCreate create = podamFactory.manufacturePojo(ResourceCreate.class);
        create.setResourceId(null);

        String errorMsg = "\"resourceId\" may not be empty";

        mockMvc.perform(post(url)
                .content(objectMapper.writeValueAsString(create))
                .contentType(MediaType.APPLICATION_JSON)
                .characterEncoding(StandardCharsets.UTF_8.name()))
                .andExpect(status().isBadRequest())
                .andExpect(content()
                    .contentTypeCompatibleWith(MediaType.APPLICATION_JSON))
                .andExpect(jsonPath("$.message", is(errorMsg)));
    }

    @Test
    public void testCreateResourceWithoutPresenceMonitoringField() throws Exception {
        String tenantId = RandomStringUtils.randomAlphabetic( 8 );
        String url = String.format("/api/tenant/%s/resources", tenantId);

        ResourceCreate create = podamFactory.manufacturePojo(ResourceCreate.class);
        create.setPresenceMonitoringEnabled(null);

        String errorMsg = "\"presenceMonitoringEnabled\" must not be null";

        mockMvc.perform(post(url)
                .content(objectMapper.writeValueAsString(create))
                .contentType(MediaType.APPLICATION_JSON)
                .characterEncoding(StandardCharsets.UTF_8.name()))
                .andExpect(status().isBadRequest())
                .andExpect(content()
                        .contentTypeCompatibleWith(MediaType.APPLICATION_JSON))
                .andExpect(jsonPath("$.message", is(errorMsg)));
    }

    @Test
    public void testUpdateResource() throws Exception {
        Resource resource = podamFactory.manufacturePojo(Resource.class);
        when(resourceManagement.updateResource(anyString(), anyString(), any()))
                .thenReturn(resource);

        String tenantId = resource.getTenantId();
        String resourceId = resource.getResourceId();
        String url = String.format("/api/tenant/%s/resources/%s", tenantId, resourceId);

        ResourceUpdate update = podamFactory.manufacturePojo(ResourceUpdate.class);

        mockMvc.perform(put(url)
                .content(objectMapper.writeValueAsString(update))
                .contentType(MediaType.APPLICATION_JSON)
                .characterEncoding(StandardCharsets.UTF_8.name()))
                .andExpect(status().isOk())
                .andExpect(content()
                        .contentTypeCompatibleWith(MediaType.APPLICATION_JSON));
    }

    @Test
    public void testGetAll() throws Exception {
        int numberOfResources = 20;
        // Use the APIs default Pageable settings
        int page = 0;
        int pageSize = 100;
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

        String url = "/api/resources";

        mockMvc.perform(get(url).contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(content()
                        .contentTypeCompatibleWith(MediaType.APPLICATION_JSON))
                .andExpect(content().string(objectMapper.writeValueAsString(pageOfResources)))
                .andExpect(jsonPath("$.content.*", hasSize(numberOfResources)))
                .andExpect(jsonPath("$.totalPages", equalTo(1)))
                .andExpect(jsonPath("$.numberOfElements", equalTo(numberOfResources)))
                .andExpect(jsonPath("$.totalElements", equalTo(numberOfResources)))
                .andExpect(jsonPath("$.pageable.pageNumber", equalTo(page)))
                .andExpect(jsonPath("$.pageable.pageSize", equalTo(pageSize)))
                .andExpect(jsonPath("$.size", equalTo(pageSize)));
    }

    @Test
    public void testGetStreamOfResources() throws Exception {
        int numberOfResources = 2000;
        List<Resource> resources = new ArrayList<>();
        for (int i=0; i<numberOfResources; i++) {
            resources.add(podamFactory.manufacturePojo(Resource.class));
        }

        List<String> expectedData = resources.stream()
                .map(r -> {
                    try {
                        return objectMapper.writeValueAsString(r);
                    } catch (JsonProcessingException e) {
                        assertThat(e, nullValue());
                        return null;
                    }
                }).collect(Collectors.toList());
        assertThat(expectedData.size(), equalTo(resources.size()));

        String url = "/api/envoys";
        Stream<Resource> resourceStream = resources.stream();

        when(resourceManagement.getExpectedEnvoys())
                .thenReturn(resourceStream);

        mockMvc.perform(get(url))
                // these may or may not be needed.
                //.andExpect(request().asyncStarted())
                //.andDo(MvcResult::getAsyncResult)
                .andDo(print())
                .andExpect(status().isOk())
                .andExpect(content().contentTypeCompatibleWith("application/stream+json;charset=UTF-8"))
                .andExpect(content().string(stringContainsInOrder(expectedData)));
    }
}