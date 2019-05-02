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

import static com.rackspace.salus.telemetry.model.LabelNamespaces.AGENT;
import static com.rackspace.salus.telemetry.model.LabelNamespaces.applyNamespace;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import com.google.common.collect.Maps;
import com.rackspace.salus.resource_management.repositories.ResourceRepository;
import com.rackspace.salus.resource_management.services.KafkaEgress;
import com.rackspace.salus.resource_management.services.ResourceManagement;
import com.rackspace.salus.resource_management.web.model.ResourceCreate;
import com.rackspace.salus.resource_management.web.model.ResourceUpdate;
import com.rackspace.salus.telemetry.messaging.AttachEvent;
import com.rackspace.salus.telemetry.messaging.ResourceEvent;
import com.rackspace.salus.telemetry.model.LabelNamespaces;
import com.rackspace.salus.telemetry.model.NotFoundException;
import com.rackspace.salus.telemetry.model.Resource;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import javax.persistence.EntityManager;
import javax.persistence.FlushModeType;

import org.apache.commons.lang3.RandomStringUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
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

@RunWith(SpringRunner.class)
@DataJpaTest
@Import({ResourceManagement.class})
@EntityScan(basePackageClasses = Resource.class)
public class ResourceManagementTest {

    public static final String TENANT = "abcde";
    public static final String RESOURCE_ID = "host:test";

    @Autowired
    ResourceManagement resourceManagement;

    @Autowired
    ResourceRepository resourceRepository;

    @Autowired
    EntityManager entityManager;

    @MockBean
    KafkaEgress kafkaEgress;

    @Captor
    ArgumentCaptor<ResourceEvent> resourceEventArg;

    PodamFactory podamFactory = new PodamFactoryImpl();

    @Before
    public void setUp() {
        Resource resource = new Resource()
                .setTenantId(TENANT)
                .setResourceId(RESOURCE_ID)
                .setLabels(Collections.singletonMap("key", "value"))
                .setPresenceMonitoringEnabled(false);
        entityManager.setFlushMode(FlushModeType.AUTO);
        resourceRepository.save(resource);
    }

    @After
    public void tearDown() throws Exception {
        resourceRepository.deleteAll();
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
        Optional<Resource> r = resourceManagement.getResource(TENANT, RESOURCE_ID);
        assertTrue(r.isPresent());
        assertThat(r.get().getId(), notNullValue());
        assertThat(r.get().getLabels(), hasEntry("key", "value"));
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

        Optional<Resource> retrieved = resourceManagement.getResource(tenantId, create.getResourceId());

        assertThat(retrieved.get().getResourceId(), equalTo(returned.getResourceId()));
        assertThat(retrieved.get().getPresenceMonitoringEnabled(), equalTo(returned.getPresenceMonitoringEnabled()));
        assertTrue(Maps.difference(returned.getLabels(), retrieved.get().getLabels()).areEqual());
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

        final Optional<Resource> resource = resourceManagement.getResource(
                attachEvent.getTenantId(),
                attachEvent.getResourceId());
        assertTrue(resource.isPresent());
        assertThat(resource.get(), notNullValue());
        assertThat(resource.get().getId(), greaterThan(0L));
        assertThat(resource.get().getTenantId(), equalTo(attachEvent.getTenantId()));
        assertThat(resource.get().getLabels().size(), greaterThan(0));
        assertThat(resource.get().getLabels().size(), equalTo(attachEvent.getLabels().size()));
        attachEvent.getLabels().forEach((name, value) -> {
            assertTrue(resource.get().getLabels().containsKey(name));
            assertThat(resource.get().getLabels().get(name), equalTo(value));
        });
        assertThat(resource.get().getResourceId(), equalTo(attachEvent.getResourceId()));
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
        entityManager.flush();
        final Resource resourceByResourceId =
            resourceManagement.getResource("tenant-1", "development:0").get();
        assertThat(resourceByResourceId, notNullValue());

        final Map<String, String> labelsToQuery = new HashMap<>();
        labelsToQuery.put("os", "DARWIN");
        final List<Resource> resourceIdsWithEnvoyLabels = resourceManagement
            .getResourcesFromLabels(labelsToQuery, "tenant-1");

        assertThat(resourceIdsWithEnvoyLabels, hasSize(1));
        assertThat(resourceIdsWithEnvoyLabels.get(0).getResourceId(), equalTo("development:0"));
    }

    @Test
    public void testEnvoyAttachAndFailedQueryByLabels() {
        final Map<String, String> labels = new HashMap<>();
        labels.put("os", "DARWIN");
        final AttachEvent attachEvent = new AttachEvent()
                .setEnvoyId("envoy-1")
                .setEnvoyAddress("localhost")
                .setTenantId("tenant-1")
                .setResourceId("development:0")
                .setLabels(labels);
        resourceManagement.handleEnvoyAttach(attachEvent);
        entityManager.flush();
        final Optional<Resource> resourceByResourceId =
                resourceManagement.getResource("tenant-1", "development:0");
        assertTrue(resourceByResourceId.isPresent());
        assertThat(resourceByResourceId.get(), notNullValue());

        final Map<String, String> labelsToQuery = new HashMap<>();
        labelsToQuery.put("os", "DARWIN");
        labelsToQuery.put("environment", "localdev");
        labelsToQuery.put("arch", "X86_64");
        final List<Resource> resourceIdsWithEnvoyLabels = resourceManagement
                .getResourcesFromLabels(labelsToQuery, "tenant-1");

        assertEquals(0, resourceIdsWithEnvoyLabels.size());

    }

    @Test
    public void testEnvoyAttach_existingResourceWithNoLabels() {
        final Resource resource = resourceRepository.save(
            new Resource()
                .setResourceId("r-1")
                .setLabels(Collections.emptyMap())
                .setTenantId("t-1")
                .setPresenceMonitoringEnabled(false)
        );
        entityManager.flush();

        final Map<String, String> envoyLabels = new HashMap<>();
        envoyLabels.put(applyNamespace(AGENT, "discovered.hostname"), "h-1");
        envoyLabels.put(applyNamespace(AGENT, "discovered.os"), "linux");
        envoyLabels.put(applyNamespace(AGENT, "discovered.arch"), "amd64");

        resourceManagement.handleEnvoyAttach(
            new AttachEvent()
            .setEnvoyAddress("localhost:1234")
            .setEnvoyId("e-1")
            .setLabels(envoyLabels)
            .setResourceId("r-1")
            .setTenantId("t-1")
        );
        entityManager.flush();

        final Optional<Resource> actualResource = resourceRepository.findById(resource.getId());

        assertThat(actualResource.isPresent(), equalTo(true));
        assertThat(actualResource.get().getLabels(), equalTo(envoyLabels));

        verify(kafkaEgress).sendResourceEvent(resourceEventArg.capture());
        assertThat(resourceEventArg.getValue().getResourceId(), equalTo("r-1"));
        assertThat(resourceEventArg.getValue().getTenantId(), equalTo("t-1"));

        verifyNoMoreInteractions(kafkaEgress);
    }

    @Test
    public void testEnvoyAttach_existingResourceWithChangedLabels() {
        final Map<String, String> resourceLabels = new HashMap<>();
        resourceLabels.put(applyNamespace(AGENT, "discovered.hostname"), "old-h-1");
        resourceLabels.put(applyNamespace(AGENT, "notInNew"), "old-agent-value");
        resourceLabels.put("nonAgentLabel", "someValue");

        final Resource resource = resourceRepository.save(
            new Resource()
                .setResourceId("r-1")
                .setLabels(resourceLabels)
                .setTenantId("t-1")
                .setPresenceMonitoringEnabled(false)
        );
        entityManager.flush();

        final Map<String, String> envoyLabels = new HashMap<>();
        envoyLabels.put(applyNamespace(AGENT, "discovered.hostname"), "new-h-1");
        envoyLabels.put(applyNamespace(AGENT, "discovered.os"), "linux");
        envoyLabels.put(applyNamespace(AGENT, "discovered.arch"), "amd64");

        resourceManagement.handleEnvoyAttach(
            new AttachEvent()
            .setEnvoyAddress("localhost:1234")
            .setEnvoyId("e-1")
            .setLabels(envoyLabels)
            .setResourceId("r-1")
            .setTenantId("t-1")
        );
        entityManager.flush();

        final Optional<Resource> actualResource = resourceRepository.findById(resource.getId());

        final Map<String, String> expectedResourceLabels = new HashMap<>();
        expectedResourceLabels.put(applyNamespace(AGENT, "discovered.hostname"), "new-h-1");
        expectedResourceLabels.put(applyNamespace(AGENT, "discovered.os"), "linux");
        expectedResourceLabels.put(applyNamespace(AGENT, "discovered.arch"), "amd64");
        expectedResourceLabels.put("nonAgentLabel", "someValue");

        assertThat(actualResource.isPresent(), equalTo(true));
        assertThat(actualResource.get().getLabels(), equalTo(expectedResourceLabels));

        verify(kafkaEgress).sendResourceEvent(resourceEventArg.capture());
        assertThat(resourceEventArg.getValue().getResourceId(), equalTo("r-1"));
        assertThat(resourceEventArg.getValue().getTenantId(), equalTo("t-1"));

        verifyNoMoreInteractions(kafkaEgress);
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
    public void testUpdateExistingResourceWithMetadata() {
        Map<String, String> labels = new HashMap<>();
        labels.put("oldlabel", "oldValue");
        labels.put(LabelNamespaces.applyNamespace(AGENT, "env"), "prod");

        Resource resource = resourceRepository.save(
            new Resource()
            .setTenantId("t-testUpdateExistingResourceWithMetadata")
            .setLabels(labels)
            .setResourceId("r-1")
            .setPresenceMonitoringEnabled(true)
        );

        Map<String, String> newLabels = new HashMap<>();
        newLabels.put("newLabel", "newValue");

        final HashMap<String, String> newMetadata = new HashMap<>();
        newMetadata.put("local_ip", "127.0.0.1");

        final boolean presenceMonitoring = !resource.getPresenceMonitoringEnabled();

        ResourceUpdate update = new ResourceUpdate()
            .setLabels(newLabels)
            .setMetadata(newMetadata)
            .setPresenceMonitoringEnabled(presenceMonitoring);

        Resource newResource = resourceManagement.updateResource(
            resource.getTenantId(),
            resource.getResourceId(),
            update
        );

        Map<String, String> expectedLabels = new HashMap<>();
        expectedLabels.put("newLabel", "newValue");
        expectedLabels.put(LabelNamespaces.applyNamespace(AGENT, "env"), "prod");

        assertThat(newResource.getLabels(), equalTo(expectedLabels));
        assertThat(newResource.getMetadata(), equalTo(resource.getMetadata()));
        assertThat(newResource.getId(), equalTo(resource.getId()));
        assertThat(newResource.getPresenceMonitoringEnabled(), equalTo(presenceMonitoring));
    }

    @Test
    public void testRemoveResource() {
        ResourceCreate create = podamFactory.manufacturePojo(ResourceCreate.class);
        String tenantId = RandomStringUtils.randomAlphanumeric(10);
        resourceManagement.createResource(tenantId, create);

        Optional<Resource> created = resourceManagement.getResource(tenantId, create.getResourceId());
        assertTrue(created.isPresent());
        assertThat(created.get(), notNullValue());

        resourceManagement.removeResource(tenantId, create.getResourceId());
        Optional<Resource> deleted = resourceManagement.getResource(tenantId, create.getResourceId());
        assertTrue(!deleted.isPresent());
    }

    @Test(expected = NotFoundException.class)
    public void testRemoveNonExistentMonitor() {
        String random = RandomStringUtils.randomAlphanumeric(10);
        resourceManagement.removeResource(random, random);
    }

    @Test
    public void testSpecificCreate() {
        final Map<String, String> labels = new HashMap<>();
        labels.put("os", "DARWIN");

        final Map<String, String> metadata = new HashMap<>();
        metadata.put("local_ip", "127.0.0.1");

        ResourceCreate create = podamFactory.manufacturePojo(ResourceCreate.class);
        create.setLabels(labels);
        create.setMetadata(metadata);
        String tenantId = RandomStringUtils.randomAlphanumeric(10);
        resourceManagement.createResource(tenantId, create);
        entityManager.flush();
        List<Resource> resources = resourceManagement.getResourcesFromLabels(labels, tenantId);
        assertEquals(1, resources.size());
        assertEquals(metadata, resources.get(0).getMetadata());
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
    public void testFailedMatchResourceWithMultipleLabels() {
        final Map<String, String> resourceLabels = new HashMap<>();
        resourceLabels.put("os", "DARWIN");
        resourceLabels.put("env", "test");

        final Map<String, String> labels = new HashMap<>();
        labels.put("os", "DARWIN");
        labels.put("env", "prod");

        ResourceCreate create = podamFactory.manufacturePojo(ResourceCreate.class);
        create.setLabels(resourceLabels);
        String tenantId = RandomStringUtils.randomAlphanumeric(10);
        resourceManagement.createResource(tenantId, create);
        entityManager.flush();

        List<Resource> resources = resourceManagement.getResourcesFromLabels(labels, tenantId);
        assertEquals(0, resources.size());
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
    public void testFailMatchResourceWithSupersetOfDifferentLabels() {
        final Map<String, String> resourceLabels = new HashMap<>();
        resourceLabels.put("os", "DARWIN");
        resourceLabels.put("env", "test");
        resourceLabels.put("architecture", "x86");
        resourceLabels.put("region", "DFW");
        final Map<String, String> labels = new HashMap<>();
        labels.put("os", "DARWIN");
        labels.put("env", "prod");

        ResourceCreate create = podamFactory.manufacturePojo(ResourceCreate.class);
        create.setLabels(resourceLabels);
        String tenantId = RandomStringUtils.randomAlphanumeric(10);
        resourceManagement.createResource(tenantId, create);
        entityManager.flush();

        List<Resource> resources = resourceManagement.getResourcesFromLabels(labels, tenantId);
        assertEquals(0, resources.size());
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
        labels.put("region", "LON");


        ResourceCreate create = podamFactory.manufacturePojo(ResourceCreate.class);
        create.setLabels(resourceLabels);
        String tenantId = RandomStringUtils.randomAlphanumeric(10);
        resourceManagement.createResource(tenantId, create);
        entityManager.flush();

        List<Resource> resources = resourceManagement.getResourcesFromLabels(labels, tenantId);
        assertEquals(0, resources.size());
    }

    @Test
    public void testGetResourcesFromLabels_noLabels() {
        final Map<String, String> resourceLabels = new HashMap<>();
        resourceLabels.put("os", "DARWIN");
        resourceLabels.put("env", "test");

        ResourceCreate create = podamFactory.manufacturePojo(ResourceCreate.class);
        create.setLabels(resourceLabels);
        String tenantId = RandomStringUtils.randomAlphanumeric(10);
        resourceManagement.createResource(tenantId, create);
        entityManager.flush();

        List<Resource> resources = resourceManagement.getResourcesFromLabels(Collections.emptyMap(), tenantId);
        assertThat(resources, hasSize(1));
        assertThat(resources.get(0).getTenantId(), equalTo(tenantId));
        assertThat(resources.get(0).getResourceId(), equalTo(create.getResourceId()));
    }

    @Test
    public void testGetResourcesFromLabels_multipleMatches() {

        final Map<String, String> labels = new HashMap<>();
        labels.put("os", "linux");
        labels.put("arch", "x86_64");

        final List<Resource> resourcesToSave = IntStream.range(0, 10)
            .mapToObj(value -> new Resource()
                .setTenantId("testGetResourcesFromLabels_multipleMatches")
                .setPresenceMonitoringEnabled(true)
                .setLabels(new HashMap<>(labels))
                .setResourceId(RandomStringUtils.randomAlphanumeric(10))
            )
            .collect(Collectors.toList());

        resourceRepository.saveAll(resourcesToSave);
        entityManager.flush();

        List<Resource> resources = resourceManagement.getResourcesFromLabels(
            Collections.singletonMap("os", "linux"), "testGetResourcesFromLabels_multipleMatches");
        assertThat(resources, hasSize(10));
        assertThat(resources.get(0).getTenantId(), equalTo("testGetResourcesFromLabels_multipleMatches"));
    }

  @Test(expected = IllegalArgumentException.class)
  public void testUserLabelConflictsWithSystemNamespace() {
    final ResourceUpdate update = new ResourceUpdate()
        .setLabels(Collections.singletonMap(LabelNamespaces.EVENT_ENGINE_TAGS +".account", "HackedAccount"));
    resourceManagement.updateResource(TENANT, RESOURCE_ID, update);
  }
}
