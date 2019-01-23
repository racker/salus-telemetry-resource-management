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

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.hasSize;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rackspace.salus.resource_management.services.ResourceManagement;
import com.rackspace.salus.resource_management.web.controller.ResourceApi;
import com.rackspace.salus.resource_management.web.model.ResourceCreate;
import com.rackspace.salus.resource_management.web.model.ResourceUpdate;
import com.rackspace.salus.telemetry.model.Resource;
import com.rackspace.salus.telemetry.repositories.ResourceRepository;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.http.MediaType;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.servlet.MockMvc;
import uk.co.jemos.podam.api.PodamFactory;
import uk.co.jemos.podam.api.PodamFactoryImpl;

import javax.persistence.EntityManagerFactory;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

@RunWith(SpringRunner.class)
@WebMvcTest(controllers = ResourceApi.class)
public class ResourceApiTest {

    String apiUrl = "http://localhost:8085/api";

    PodamFactory podamFactory = new PodamFactoryImpl();

    @Autowired
    MockMvc mockMvc;

    @MockBean
    ResourceManagement resourceManagement;

    @MockBean
    EntityManagerFactory entityManagerFactory;

    @MockBean
    ResourceRepository resourceRepository;

    private ObjectMapper objectMapper = new ObjectMapper();

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
        List<Resource> resources = new ArrayList<>();
        for (int i=0; i<numberOfResources; i++) {
            resources.add(podamFactory.manufacturePojo(Resource.class));
        }

        Page<Resource> pageOfResources = new PageImpl<>(resources);

        when(resourceManagement.getResources(anyString(), any()))
                .thenReturn(pageOfResources);

        String tenantId = RandomStringUtils.randomAlphabetic( 8 );
        String url = String.format("/api/tenant/%s/resources", tenantId);

        mockMvc.perform(get(url).contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(content()
                        .contentTypeCompatibleWith(MediaType.APPLICATION_JSON))
                .andExpect(jsonPath("$.*", hasSize(numberOfResources)));
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
}