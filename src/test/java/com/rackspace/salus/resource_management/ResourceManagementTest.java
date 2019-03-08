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

import static junit.framework.TestCase.assertEquals;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.Maps;
import com.rackspace.salus.resource_management.services.KafkaEgress;
import com.rackspace.salus.resource_management.services.ResourceManagement;
import com.rackspace.salus.resource_management.web.model.ResourceCreate;
import com.rackspace.salus.resource_management.web.model.ResourceUpdate;
import com.rackspace.salus.telemetry.messaging.AttachEvent;
import com.rackspace.salus.telemetry.model.Resource;
import com.rackspace.salus.telemetry.repositories.ResourceRepository;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.Stream;
import javax.persistence.EntityManager;
import javax.persistence.FlushModeType;

import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Import;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.test.context.junit4.SpringRunner;
import uk.co.jemos.podam.api.PodamFactory;
import uk.co.jemos.podam.api.PodamFactoryImpl;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.stream.Stream;

@RunWith(SpringRunner.class)
@DataJpaTest
@Import({ResourceManagement.class})
@EntityScan(basePackageClasses = Resource.class)
public class ResourceManagementTest {

    @Autowired
    ResourceManagement resourceManagement;

    @Autowired
    ResourceRepository resourceRepository;

    @Autowired
    EntityManager entityManager;

    @MockBean
    KafkaEgress kafkaEgress;

    PodamFactory podamFactory = new PodamFactoryImpl();

    @Before
    public void setUp() {
        Resource resource = new Resource()
                .setTenantId("abcde")
                .setResourceId("host:test")
                .setLabels(Collections.singletonMap("key", "value"))
                .setPresenceMonitoringEnabled(false);
        entityManager.setFlushMode(FlushModeType.AUTO);
        resourceRepository.save(resource);
    }

    private void createResources(int count) {
        for (int i=0; i<count; i++) {
            String tenantId = RandomStringUtils.randomAlphanumeric(10);
            ResourceCreate create = podamFactory.manufacturePojo(ResourceCreate.class);
            resourceManagement.createResource(tenantId, create);
        }
    }

    private void createResourcesForTenant(int count, String tenantId) {
        for (int i=0; i<count; i++) {
            ResourceCreate create = podamFactory.manufacturePojo(ResourceCreate.class);
            resourceManagement.createResource(tenantId, create);
        }
    }

    @Test
    public void testGetResource() {
        Resource r = resourceManagement.getResource("abcde", "host:test");

        assertThat(r.getId(), notNullValue());
        assertThat(r.getLabels(), hasEntry("key", "value"));
    }

    @Test
    public void testCreateNewResource() {
        ResourceCreate create = podamFactory.manufacturePojo(ResourceCreate.class);
        String tenantId = RandomStringUtils.randomAlphanumeric(10);

        Resource returned = resourceManagement.createResource(tenantId, create);

        assertThat(returned.getId(), notNullValue());
        assertThat(returned.getResourceId(), equalTo(create.getResourceId()));
        assertThat(returned.getPresenceMonitoringEnabled(), notNullValue());
        assertThat(returned.getPresenceMonitoringEnabled(), equalTo(create.getPresenceMonitoringEnabled()));
        assertThat(returned.getLabels().size(), greaterThan(0));
        assertTrue(Maps.difference(create.getLabels(), returned.getLabels()).areEqual());

        Resource retrieved = resourceManagement.getResource(tenantId, create.getResourceId());

        assertThat(retrieved.getResourceId(), equalTo(returned.getResourceId()));
        assertThat(retrieved.getPresenceMonitoringEnabled(), equalTo(returned.getPresenceMonitoringEnabled()));
        assertTrue(Maps.difference(returned.getLabels(), retrieved.getLabels()).areEqual());
    }

    @Test
    public void testGetAll() {
        Random random = new Random();
        int totalResources = random.nextInt(150 - 50) + 50;
        int pageSize = 10;

        Pageable page = PageRequest.of(0, pageSize);
        Page<Resource> result = resourceManagement.getAllResources(page);

        assertThat(result.getTotalElements(), equalTo(1L));

        // There is already one resource created as default
        createResources(totalResources - 1);

        page = PageRequest.of(0, 10);
        result = resourceManagement.getAllResources(page);

        assertThat(result.getTotalElements(), equalTo((long) totalResources));
        assertThat(result.getTotalPages(), equalTo((totalResources + pageSize - 1) / pageSize));
    }

    @Test
    public void testGetAllForTenant() {
        Random random = new Random();
        int totalResources = random.nextInt(150 - 50) + 50;
        int pageSize = 10;
        String tenantId = RandomStringUtils.randomAlphanumeric(10);

        Pageable page = PageRequest.of(0, pageSize);
        Page<Resource> result = resourceManagement.getAllResources(page);

        assertThat(result.getTotalElements(), equalTo(1L));

        createResourcesForTenant(totalResources , tenantId);

        page = PageRequest.of(0, 10);
        result = resourceManagement.getResources(tenantId, page);

        assertThat(result.getTotalElements(), equalTo((long) totalResources));
        assertThat(result.getTotalPages(), equalTo((totalResources + pageSize - 1) / pageSize));
    }

    @Test
    public void testGetResourcesWithPresenceMonitoringAsStream() {
        int totalResources = 100;
        createResources(totalResources);
        Stream s = resourceManagement.getResources(true);
        // The one default resource doesn't have presence monitoring enabled so can be ignored.
        assertThat(s.count(), equalTo((long) totalResources));
    }

    @Test
    public void testNewEnvoyAttach() {
        AttachEvent attachEvent = podamFactory.manufacturePojo(AttachEvent.class);
        resourceManagement.handleEnvoyAttach(attachEvent);

        final Resource resource = resourceManagement.getResource(
                attachEvent.getTenantId(),
                attachEvent.getResourceId());
        assertThat(resource, notNullValue());
        assertThat(resource.getId(), greaterThan(0L));
        assertThat(resource.getTenantId(), equalTo(attachEvent.getTenantId()));
        assertThat(resource.getLabels().size(), greaterThan(0));
        assertThat(resource.getLabels().size(), equalTo(attachEvent.getLabels().size()));
        attachEvent.getLabels().forEach((name, value) -> {
            String prefixedLabel = "envoy." + name;
            assertTrue(resource.getLabels().containsKey(prefixedLabel));
            assertThat(resource.getLabels().get(prefixedLabel), equalTo(value));
        });
        assertThat(resource.getResourceId(), equalTo(attachEvent.getResourceId()));
    }

    @Test
    public void testEnvoyAttachAndQueryByLabels() {
        final Map<String, String> labels = new HashMap<>();
        labels.put("environment", "localdev");
        labels.put("arch", "X86_64");
        labels.put("os", "DARWIN");
        final AttachEvent attachEvent = new AttachEvent()
            .setEnvoyId("envoy-1")
            .setEnvoyAddress("localhost")
            .setTenantId("tenant-1")
            .setResourceId("development:0")
            .setLabels(labels);

        resourceManagement.handleEnvoyAttach(attachEvent);

        final Resource resourceByResourceId =
            resourceManagement.getResource("tenant-1", "development:0");
        assertThat(resourceByResourceId, notNullValue());

        final Map<String, String> labelsToQuery = new HashMap<>();
        labelsToQuery.put("os", "DARWIN");
        final List<String> resourceIdsWithEnvoyLabels = resourceManagement
            .getResourceIdsWithEnvoyLabels(labelsToQuery, "tenant-1");

        assertThat(resourceIdsWithEnvoyLabels, hasSize(1));
        assertThat(resourceIdsWithEnvoyLabels, hasItem("development:0"));
    }

    @Test
    public void testUpdateExistingResource() {
        Resource resource = resourceManagement.getAllResources(PageRequest.of(0, 1)).getContent().get(0);
        Map<String, String> newLabels = new HashMap<>(resource.getLabels());
        newLabels.put("newLabel", "newValue");
        boolean presenceMonitoring = !resource.getPresenceMonitoringEnabled();
        ResourceUpdate update = new ResourceUpdate();

        // lombok chaining isn't working when I compile, so doing this way for now.
        update.setLabels(newLabels);
        update.setPresenceMonitoringEnabled(presenceMonitoring);

        Resource newResource;
        try {
            newResource = resourceManagement.updateResource(
                    resource.getTenantId(),
                    resource.getResourceId(),
                    update);
        } catch (Exception e) {
            assertThat(e, nullValue());
            return;
        }

        assertThat(newResource.getLabels(), equalTo(resource.getLabels()));
        assertThat(newResource.getId(), equalTo(resource.getId()));
        assertThat(newResource.getPresenceMonitoringEnabled(), equalTo(presenceMonitoring));
    }

    @Test
    public void testRemoveResource() {
        ResourceCreate create = podamFactory.manufacturePojo(ResourceCreate.class);
        String tenantId = RandomStringUtils.randomAlphanumeric(10);
        resourceManagement.createResource(tenantId, create);

        Resource resource = resourceManagement.getResource(tenantId, create.getResourceId());
        assertThat(resource, notNullValue());

        resourceManagement.removeResource(tenantId, create.getResourceId());
        resource = resourceManagement.getResource(tenantId, create.getResourceId());
        assertThat(resource, nullValue());
    }

    @Test
    public void testLabelMatchingQuery() {
        Map<String, String> labels = new HashMap<>();
        labels.put("key", "value");


    }

    @Test
    public void testSpecificCreate() {
        final Map<String, String> labels = new HashMap<>();
        labels.put("os", "DARWIN");

        ResourceCreate create = podamFactory.manufacturePojo(ResourceCreate.class);
        create.setLabels(labels);
        String tenantId = RandomStringUtils.randomAlphanumeric(10);
        resourceManagement.createResource(tenantId, create);
        entityManager.flush();
        List<Resource> resources = resourceManagement.getResourcesFromLabels(labels, tenantId);
        assertEquals(1, resources.size());
        assertNotNull(resources);
    }

    @Test
    public void testResourcesWithSameLabelsAndDifferentTenants() {
        final Map<String, String> labels = new HashMap<>();
        labels.put("key", "value");

        ResourceCreate create = podamFactory.manufacturePojo(ResourceCreate.class);
        create.setLabels(labels);
        String tenantId = RandomStringUtils.randomAlphanumeric(10);
        String tenantId2 = RandomStringUtils.randomAlphanumeric(10);
        resourceManagement.createResource(tenantId, create);
        resourceManagement.createResource(tenantId2, create);

        List<Resource> resources = resourceManagement.getResourcesFromLabels(labels, tenantId);
        assertEquals(1, resources.size()); //make sure we only returned the one value
        assertEquals(tenantId, resources.get(0).getTenantId());
        assertEquals(create.getResourceId(), resources.get(0).getResourceId());
    }

    @Test
    public void testMatchResourceWithMultipleLabels() {
        final Map<String, String> labels = new HashMap<>();
        labels.put("os", "DARWIN");
        labels.put("env", "test");

        ResourceCreate create = podamFactory.manufacturePojo(ResourceCreate.class);
        create.setLabels(labels);
        String tenantId = RandomStringUtils.randomAlphanumeric(10);
        resourceManagement.createResource(tenantId, create);
        entityManager.flush();

        List<Resource> resources = resourceManagement.getResourcesFromLabels(labels, tenantId);
        assertEquals(1, resources.size()); //make sure we only returned the one value
        assertEquals(tenantId, resources.get(0).getTenantId());
        assertEquals(create.getResourceId(), resources.get(0).getResourceId());
        assertEquals(labels, resources.get(0).getLabels());
    }

    @Test
    public void testMatchResourceWithSupersetOfLabels() {
        final Map<String, String> resourceLabels = new HashMap<>();
        resourceLabels.put("os", "DARWIN");
        resourceLabels.put("env", "test");
        resourceLabels.put("architecture", "x86");
        resourceLabels.put("region", "DFW");
        final Map<String, String> labels = new HashMap<>();
        labels.put("os", "DARWIN");
        labels.put("env", "test");

        ResourceCreate create = podamFactory.manufacturePojo(ResourceCreate.class);
        create.setLabels(resourceLabels);
        String tenantId = RandomStringUtils.randomAlphanumeric(10);
        resourceManagement.createResource(tenantId, create);
        entityManager.flush();

        List<Resource> resources = resourceManagement.getResourcesFromLabels(labels, tenantId);
        assertEquals(1, resources.size()); //make sure we only returned the one value
        assertEquals(tenantId, resources.get(0).getTenantId());
        assertEquals(create.getResourceId(), resources.get(0).getResourceId());
        assertEquals(resourceLabels, resources.get(0).getLabels());
    }

    @Test
    public void testMatchResourceWithSubsetOfLabels() {
        final Map<String, String> resourceLabels = new HashMap<>();
        resourceLabels.put("os", "DARWIN");
        resourceLabels.put("env", "test");
        final Map<String, String> labels = new HashMap<>();
        labels.put("os", "DARWIN");
        labels.put("env", "test");
        labels.put("architecture", "x86");
        labels.put("region", "DFW");


        ResourceCreate create = podamFactory.manufacturePojo(ResourceCreate.class);
        create.setLabels(resourceLabels);
        String tenantId = RandomStringUtils.randomAlphanumeric(10);
        resourceManagement.createResource(tenantId, create);
        entityManager.flush();

        List<Resource> resources = resourceManagement.getResourcesFromLabels(labels, tenantId);
        assertEquals(1, resources.size()); //make sure we only returned the one value
        assertEquals(tenantId, resources.get(0).getTenantId());
        assertEquals(create.getResourceId(), resources.get(0).getResourceId());
        assertEquals(labels, resources.get(0).getLabels());
    }


    /*
    So the question is what tests do I need to write for the Resource Management service?

    We need the Resource before the Monitor... And we need the Monitor before the Resource

    Resources should have many labels and monitors should have few.
     */
}
