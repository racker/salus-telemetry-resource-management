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
 *
 */

package com.rackspace.salus.resource_management;

import static com.rackspace.salus.telemetry.model.LabelNamespaces.AGENT;
import static com.rackspace.salus.telemetry.model.LabelNamespaces.applyNamespace;
import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.containsInAnyOrder;
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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.google.common.collect.Maps;
import com.rackspace.salus.resource_management.config.DatabaseConfig;
import com.rackspace.salus.resource_management.config.ResourceManagementProperties;
import com.rackspace.salus.resource_management.services.KafkaEgress;
import com.rackspace.salus.resource_management.services.ResourceManagement;
import com.rackspace.salus.resource_management.web.model.ResourceCreate;
import com.rackspace.salus.resource_management.web.model.ResourceDTO;
import com.rackspace.salus.resource_management.web.model.ResourceUpdate;
import com.rackspace.salus.telemetry.entities.Resource;
import com.rackspace.salus.telemetry.etcd.services.EnvoyResourceManagement;
import com.rackspace.salus.telemetry.messaging.AttachEvent;
import com.rackspace.salus.telemetry.messaging.ResourceEvent;
import com.rackspace.salus.telemetry.model.LabelNamespaces;
import com.rackspace.salus.telemetry.model.LabelSelectorMethod;
import com.rackspace.salus.telemetry.model.NotFoundException;
import com.rackspace.salus.telemetry.model.ResourceInfo;
import com.rackspace.salus.telemetry.repositories.ResourceRepository;
import com.rackspace.salus.test.EnableTestContainersDatabase;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
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
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Import;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import uk.co.jemos.podam.api.PodamFactory;
import uk.co.jemos.podam.api.PodamFactoryImpl;

@SuppressWarnings("OptionalGetWithoutIsPresent")
@RunWith(SpringRunner.class)
@EnableTestContainersDatabase
@DataJpaTest
@Import({ResourceManagement.class, ResourceManagementProperties.class, DatabaseConfig.class, EnvoyResourceManagement.class,
    SimpleMeterRegistry.class})
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
    EnvoyResourceManagement envoyResourceManagement;

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
            String resourceId = RandomStringUtils.randomAlphanumeric(10);
            create.setResourceId(resourceId);

            ResourceInfo info = new ResourceInfo()
                .setEnvoyId("e-1")
                .setTenantId(tenantId)
                .setResourceId(resourceId);

            when(envoyResourceManagement.getOne(tenantId, resourceId))
                .thenReturn(CompletableFuture.completedFuture(info));

            resourceManagement.createResource(tenantId, create);
        }
    }

    private void createResourcesForTenant(int count, String tenantId) {
        for (int i=0; i<count; i++) {
            ResourceCreate create = podamFactory.manufacturePojo(ResourceCreate.class);
            String resourceId = RandomStringUtils.randomAlphanumeric(10);
            create.setResourceId(resourceId);
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
        String resourceId = RandomStringUtils.randomAlphanumeric(10);
        create.setResourceId(resourceId);

        ResourceInfo info = new ResourceInfo()
            .setEnvoyId("e-1")
            .setTenantId(tenantId)
            .setResourceId(resourceId);

        when(envoyResourceManagement.getOne(any(), any()))
            .thenReturn(CompletableFuture.completedFuture(info));

        ResourceDTO returned = resourceManagement.createResource(tenantId, create);

        assertThat(returned.getId(), notNullValue());
        assertThat(returned.getResourceId(), equalTo(create.getResourceId()));
        assertThat(returned.getPresenceMonitoringEnabled(), notNullValue());
        assertThat(returned.getPresenceMonitoringEnabled(), equalTo(create.getPresenceMonitoringEnabled()));
        assertThat(returned.getLabels().size(), greaterThan(0));
        assertThat(returned.getMetadata(), notNullValue());
        assertThat(returned.getEnvoyId(), equalTo("e-1"));
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
        ResourceInfo info = new ResourceInfo()
            .setEnvoyId("e-1")
            .setTenantId(TENANT);

        when(envoyResourceManagement.getOne(any(), any()))
            .thenReturn(CompletableFuture.completedFuture(info));

        Pageable page = PageRequest.of(0, pageSize);
        Page<ResourceDTO> result = resourceManagement.getAllResourceDTOs(page);

        assertThat(result.getTotalElements(), equalTo(1L));

        // There is already one resource created as default
        createResources(totalResources - 1);

        page = PageRequest.of(0, 10);
        result = resourceManagement.getAllResourceDTOs(page);

        assertThat(result.getTotalElements(), equalTo((long) totalResources));
        assertThat(result.getTotalPages(), equalTo((totalResources + pageSize - 1) / pageSize));
    }

    @Test
    public void testGetAllForTenant() {
        Random random = new Random();
        int totalResources = random.nextInt(150 - 50) + 50;
        int pageSize = 10;
        String tenantId = RandomStringUtils.randomAlphanumeric(10);

        ResourceInfo info = new ResourceInfo()
            .setEnvoyId("e-1")
            .setTenantId(tenantId);

        when(envoyResourceManagement.getOne(any(), any()))
            .thenReturn(CompletableFuture.completedFuture(info));

        Pageable page = PageRequest.of(0, pageSize);
        Page<ResourceDTO> result = resourceManagement.getAllResourceDTOs(page);

        assertThat(result.getTotalElements(), equalTo(1L));

        createResourcesForTenant(totalResources , tenantId);

        page = PageRequest.of(0, 10);
        result = resourceManagement.getResourceDTOs(tenantId, page);

        assertThat(result.getTotalElements(), equalTo((long) totalResources));
        assertThat(result.getTotalPages(), equalTo((totalResources + pageSize - 1) / pageSize));
    }

    @Test
    public void testGetResourcesWithPresenceMonitoringAsStream() {

        ResourceInfo info = new ResourceInfo()
            .setEnvoyId("e-1")
            .setTenantId("t-1")
            .setResourceId("r-1");

        CompletableFuture<ResourceInfo> envoyInformation = CompletableFuture.completedFuture(info);
        when(envoyResourceManagement.getOne(any(), any()))
            .thenReturn(envoyInformation);
        int totalResources = 100;
        createResources(totalResources);
        Stream s = resourceManagement.getResources(true);
        // The one default resource doesn't have presence monitoring enabled so can be ignored.
        assertThat(s.count(), equalTo((long) totalResources));
    }

    @Test
    public void testNewEnvoyAttach() {
        AttachEvent attachEvent = podamFactory.manufacturePojo(AttachEvent.class);
        String resourceId = RandomStringUtils.randomAlphanumeric(10);
        //podam doesn't know that the resourceId on the attachEvent needs to be alphanumeric, :, or -
        attachEvent.setResourceId(resourceId);
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
        assertThat(resource.get().getMetadata(), anEmptyMap());
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
        final Page<Resource> resourceIdsWithEnvoyLabels = resourceManagement
            .getResourcesFromLabels(labelsToQuery, "tenant-1", LabelSelectorMethod.AND, Pageable.unpaged());

        assertThat(resourceIdsWithEnvoyLabels.getTotalElements(), equalTo(1L));
        assertThat(resourceIdsWithEnvoyLabels.getContent().get(0).getResourceId(), equalTo("development:0"));
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
        final Page<Resource> resourceIdsWithEnvoyLabels = resourceManagement
                .getResourcesFromLabels(labelsToQuery, "tenant-1", LabelSelectorMethod.AND, Pageable.unpaged());

        assertEquals(0L, resourceIdsWithEnvoyLabels.getTotalElements());

    }

    @Test
    public void testEnvoyAttach_existingResourceWithNoLabels()
        throws ExecutionException, InterruptedException {
        final Resource resource = resourceRepository.save(
            new Resource()
                .setResourceId("r-1")
                .setLabels(Collections.emptyMap())
                .setTenantId("t-1")
                .setPresenceMonitoringEnabled(false)
        );
        entityManager.flush();

        final Map<String, String> envoyLabels = new HashMap<>();
        envoyLabels.put(applyNamespace(AGENT, "discovered_hostname"), "h-1");
        envoyLabels.put(applyNamespace(AGENT, "discovered_os"), "linux");
        envoyLabels.put(applyNamespace(AGENT, "discovered_arch"), "amd64");
        ResourceInfo info = new ResourceInfo()
            .setEnvoyId("e-1")
            .setTenantId("t-1")
            .setResourceId("r-1")
            .setLabels(envoyLabels);

        CompletableFuture<ResourceInfo> envoyInformation = CompletableFuture.completedFuture(info);
        when(envoyResourceManagement.getOne(any(), any()))
          .thenReturn(envoyInformation);
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
        assertThat(resourceEventArg.getValue(), equalTo(
            new ResourceEvent()
                .setTenantId("t-1")
                .setResourceId("r-1")
                .setLabelsChanged(true)
                .setReattachedEnvoyId(null) // should NOT indicate re-attachment
        ));

        verifyNoMoreInteractions(kafkaEgress);
    }

    @Test
    public void testEnvoyAttach_existingResourceWithChangedLabels() {
        final Map<String, String> resourceLabels = new HashMap<>();
        resourceLabels.put(applyNamespace(AGENT, "discovered_hostname"), "old-h-1");
        resourceLabels.put(applyNamespace(AGENT, "notInNew"), "old-agent-value");
        resourceLabels.put("nonAgentLabel", "someValue");

        final Map<String, String> originalMetadata = Map.of("key1", "value1");

        final Resource resource = resourceRepository.save(
            new Resource()
                .setResourceId("r-1")
                .setLabels(resourceLabels)
                .setMetadata(originalMetadata)
                .setTenantId("t-1")
                .setPresenceMonitoringEnabled(false)
                .setAssociatedWithEnvoy(true)
        );
        entityManager.flush();

        final Map<String, String> envoyLabels = new HashMap<>();
        envoyLabels.put(applyNamespace(AGENT, "discovered_hostname"), "new-h-1");
        envoyLabels.put(applyNamespace(AGENT, "discovered_os"), "linux");
        envoyLabels.put(applyNamespace(AGENT, "discovered_arch"), "amd64");

        // EXECUTE

        resourceManagement.handleEnvoyAttach(
            new AttachEvent()
            .setEnvoyAddress("localhost:1234")
            .setEnvoyId("e-1")
            .setLabels(envoyLabels)
            .setResourceId("r-1")
            .setTenantId("t-1")
        );
        entityManager.flush();

        // VERIFY

        final Optional<Resource> actualResource = resourceRepository.findById(resource.getId());

        final Map<String, String> expectedResourceLabels = new HashMap<>();
        expectedResourceLabels.put(applyNamespace(AGENT, "discovered_hostname"), "new-h-1");
        expectedResourceLabels.put(applyNamespace(AGENT, "discovered_os"), "linux");
        expectedResourceLabels.put(applyNamespace(AGENT, "discovered_arch"), "amd64");
        expectedResourceLabels.put("nonAgentLabel", "someValue");

        assertThat(actualResource.isPresent(), equalTo(true));
        assertThat(actualResource.get().getLabels(), equalTo(expectedResourceLabels));
        assertThat(actualResource.get().getMetadata(), equalTo(originalMetadata));

        verify(kafkaEgress).sendResourceEvent(resourceEventArg.capture());
        assertThat(resourceEventArg.getValue(), equalTo(
            new ResourceEvent()
                .setTenantId("t-1")
                .setResourceId("r-1")
                .setLabelsChanged(true)
                .setReattachedEnvoyId("e-1")
        ));

        verifyNoMoreInteractions(kafkaEgress);
    }

    @Test
    public void testEnvoyAttach_existingResource_sameLabels() {
        final Map<String, String> resourceLabels = new HashMap<>();
        resourceLabels.put(applyNamespace(AGENT, "discovered_hostname"), "h-1");
        resourceLabels.put("nonAgentLabel", "someValue");

        final Map<String, String> originalMetadata = Map.of("key1", "value1");

        final Resource resource = resourceRepository.save(
            new Resource()
                .setResourceId("r-1")
                .setLabels(resourceLabels)
                .setMetadata(originalMetadata)
                .setTenantId("t-1")
                .setPresenceMonitoringEnabled(false)
                .setAssociatedWithEnvoy(true)
        );
        entityManager.flush();

        final Map<String, String> envoyLabels = new HashMap<>();
        envoyLabels.put(applyNamespace(AGENT, "discovered_hostname"), "h-1");

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
        expectedResourceLabels.put(applyNamespace(AGENT, "discovered_hostname"), "h-1");
        expectedResourceLabels.put("nonAgentLabel", "someValue");

        assertThat(actualResource.isPresent(), equalTo(true));
        assertThat(actualResource.get().getLabels(), equalTo(expectedResourceLabels));
        assertThat(actualResource.get().getMetadata(), equalTo(originalMetadata));

        // ONLY sends ReattachedEnvoyResourceEvent and NOT a resource change event

        verify(kafkaEgress).sendResourceEvent(resourceEventArg.capture());

        assertThat(resourceEventArg.getValue(), equalTo(
            new ResourceEvent()
                .setTenantId("t-1")
                .setResourceId("r-1")
                .setLabelsChanged(false)
                .setReattachedEnvoyId("e-1")
        ));

        verifyNoMoreInteractions(kafkaEgress);
    }

    @Test
    public void testUpdateExistingResource() {
        ResourceInfo info = new ResourceInfo()
            .setEnvoyId("e-1")
            .setTenantId(TENANT)
            .setResourceId(RESOURCE_ID);

        when(envoyResourceManagement.getOne(any(), any()))
          .thenReturn(CompletableFuture.completedFuture(info));
        ResourceDTO resource = resourceManagement.getAllResourceDTOs(PageRequest.of(0, 1)).getContent().get(0);
        Map<String, String> newLabels = new HashMap<>(resource.getLabels());
        newLabels.put("newLabel", "newValue");
        boolean presenceMonitoring = !resource.getPresenceMonitoringEnabled();
        ResourceUpdate update = new ResourceUpdate()
          .setLabels(newLabels)
          .setPresenceMonitoringEnabled(presenceMonitoring);

        ResourceDTO newResource;
        try {
            newResource = resourceManagement.updateResource(
                    resource.getTenantId(),
                    resource.getResourceId(),
                    update);
        } catch (Exception e) {
            assertThat(e, nullValue());
            return;
        }

        resource = resourceManagement.getAllResourceDTOs(PageRequest.of(0, 1)).getContent().get(0);

        assertThat(newResource.getEnvoyId(), equalTo("e-1"));
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

        ResourceInfo info = new ResourceInfo()
            .setEnvoyId("e-1")
            .setTenantId("t-1")
            .setResourceId("r-1");

        CompletableFuture<ResourceInfo> envoyInformation = CompletableFuture.completedFuture(info);
        when(envoyResourceManagement.getOne(any(), any()))
            .thenReturn(envoyInformation);
        ResourceDTO newResource = resourceManagement.updateResource(
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
        String resourceId = RandomStringUtils.randomAlphanumeric(10);
        create.setResourceId(resourceId);

        ResourceInfo info = new ResourceInfo()
            .setEnvoyId("e-1")
            .setTenantId(tenantId)
            .setResourceId(resourceId);

        when(envoyResourceManagement.getOne(any(), any()))
            .thenReturn(CompletableFuture.completedFuture(info));

        resourceManagement.createResource(tenantId, create);

        Optional<Resource> created = resourceManagement.getResource(tenantId, create.getResourceId());
        assertTrue(created.isPresent());
        assertThat(created.get(), notNullValue());

        // EXECUTE

        resourceManagement.removeResource(tenantId, create.getResourceId());

        // VERIFY

        Optional<Resource> deleted = resourceManagement.getResource(tenantId, create.getResourceId());
        assertTrue(!deleted.isPresent());

        verify(kafkaEgress, times(2)).sendResourceEvent(resourceEventArg.capture());
        // for the create, before the thing be testing
        assertThat(resourceEventArg.getAllValues().get(0), equalTo(
            new ResourceEvent()
                .setTenantId(tenantId)
                .setResourceId(create.getResourceId())
                .setLabelsChanged(true)
        ));
        // for the remove, that is actually be tested
        assertThat(resourceEventArg.getAllValues().get(1), equalTo(
            new ResourceEvent()
                .setTenantId(tenantId)
                .setResourceId(create.getResourceId())
                .setDeleted(true)
        ));

        verifyNoMoreInteractions(kafkaEgress);
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
        String resourceId = RandomStringUtils.randomAlphanumeric(10);
        create.setResourceId(resourceId);

        ResourceInfo info = new ResourceInfo()
            .setEnvoyId("e-1")
            .setTenantId(tenantId)
            .setResourceId(resourceId);

        when(envoyResourceManagement.getOne(any(), any()))
            .thenReturn(CompletableFuture.completedFuture(info));

        resourceManagement.createResource(tenantId, create);
        entityManager.flush();
        Page<Resource> resources = resourceManagement.getResourcesFromLabels(labels, tenantId, LabelSelectorMethod.AND, Pageable.unpaged());
        assertEquals(1L, resources.getTotalElements());
        assertEquals(metadata, resources.getContent().get(0).getMetadata());
        assertNotNull(resources);
    }

    @Test
    public void testResourcesWithSameLabelsAndDifferentTenants() {
        final Map<String, String> labels = new HashMap<>();
        labels.put("key", "value");

        ResourceCreate create = podamFactory.manufacturePojo(ResourceCreate.class);
        create.setLabels(labels);
        String resourceId = RandomStringUtils.randomAlphanumeric(10);
        create.setResourceId(resourceId);
        String tenantId = RandomStringUtils.randomAlphanumeric(10);
        String tenantId2 = RandomStringUtils.randomAlphanumeric(10);

        ResourceInfo info = new ResourceInfo()
            .setEnvoyId("e-1")
            .setTenantId(tenantId)
            .setResourceId(resourceId);

        when(envoyResourceManagement.getOne(any(), any()))
            .thenReturn(CompletableFuture.completedFuture(info));

        resourceManagement.createResource(tenantId, create);
        resourceManagement.createResource(tenantId2, create);

        Page<Resource> resources = resourceManagement.getResourcesFromLabels(labels, tenantId, LabelSelectorMethod.AND, Pageable.unpaged());
        assertEquals(1L, resources.getTotalElements()); //make sure we only returned the one value
        assertEquals(tenantId, resources.getContent().get(0).getTenantId());
        assertEquals(create.getResourceId(), resources.getContent().get(0).getResourceId());
    }

    @Test
    public void testResourcesWithSameLabelsAndDifferentTenantsUsingOr() {
        final Map<String, String> labels = new HashMap<>();
        labels.put("key", "value");

        ResourceCreate create = podamFactory.manufacturePojo(ResourceCreate.class);
        create.setLabels(labels);
        String tenantId = RandomStringUtils.randomAlphanumeric(10);
        String tenantId2 = RandomStringUtils.randomAlphanumeric(10);
        String resourceId = RandomStringUtils.randomAlphanumeric(10);
        create.setResourceId(resourceId);
        ResourceInfo info = new ResourceInfo()
            .setEnvoyId("e-1")
            .setTenantId(tenantId)
            .setResourceId(resourceId);

        when(envoyResourceManagement.getOne(any(), any()))
            .thenReturn(CompletableFuture.completedFuture(info));
        resourceManagement.createResource(tenantId, create);
        resourceManagement.createResource(tenantId2, create);

        Page<Resource> resources = resourceManagement.getResourcesFromLabels(labels, tenantId, LabelSelectorMethod.OR, Pageable.unpaged());
        assertEquals(1L, resources.getTotalElements()); //make sure we only returned the one value
        assertEquals(tenantId, resources.getContent().get(0).getTenantId());
        assertEquals(create.getResourceId(), resources.getContent().get(0).getResourceId());
    }

    @Test
    public void testMatchResourceWithMultipleLabels() {
        final Map<String, String> labels = new HashMap<>();
        labels.put("os", "DARWIN");
        labels.put("env", "test");

        ResourceCreate create = podamFactory.manufacturePojo(ResourceCreate.class);
        create.setLabels(labels);
        String resourceId = RandomStringUtils.randomAlphanumeric(10);
        create.setResourceId(resourceId);
        String tenantId = RandomStringUtils.randomAlphanumeric(10);

        ResourceInfo info = new ResourceInfo()
            .setEnvoyId("e-1")
            .setTenantId(tenantId)
            .setResourceId(resourceId);

        when(envoyResourceManagement.getOne(any(), any()))
            .thenReturn(CompletableFuture.completedFuture(info));

        resourceManagement.createResource(tenantId, create);
        entityManager.flush();

        Page<Resource> resources = resourceManagement.getResourcesFromLabels(labels, tenantId, LabelSelectorMethod.AND, Pageable.unpaged());
        assertEquals(1L, resources.getTotalElements()); //make sure we only returned the one value
        assertEquals(tenantId, resources.getContent().get(0).getTenantId());
        assertEquals(create.getResourceId(), resources.getContent().get(0).getResourceId());
        assertEquals(labels, resources.getContent().get(0).getLabels());
    }

    @Test
    public void testMatchResourceWithSingleOrLabel() {
        final Map<String, String> labels = new HashMap<>();
        labels.put("os", "DARWIN");
        labels.put("env", "test");

        final Map<String, String> matchLabels = new HashMap<>();
        labels.put("os", "DARWIN");

        ResourceCreate create = podamFactory.manufacturePojo(ResourceCreate.class);
        create.setLabels(labels);
        String resourceId = RandomStringUtils.randomAlphanumeric(10);
        create.setResourceId(resourceId);
        String tenantId = RandomStringUtils.randomAlphanumeric(10);

        ResourceInfo info = new ResourceInfo()
            .setEnvoyId("e-1")
            .setTenantId(tenantId)
            .setResourceId(resourceId);

        when(envoyResourceManagement.getOne(any(), any()))
            .thenReturn(CompletableFuture.completedFuture(info));

        resourceManagement.createResource(tenantId, create);
        entityManager.flush();

        Page<Resource> resources = resourceManagement.getResourcesFromLabels(matchLabels, tenantId, LabelSelectorMethod.OR, Pageable.unpaged());
        assertEquals(1L, resources.getTotalElements()); //make sure we only returned the one value
        assertEquals(tenantId, resources.getContent().get(0).getTenantId());
        assertEquals(create.getResourceId(), resources.getContent().get(0).getResourceId());
        assertEquals(labels, resources.getContent().get(0).getLabels());
    }

    /**
     * Make sure that when supplied with a emptyMap collection that we return resources with no labels
     * as well as any resources for that tenant
     */
    @Test
    public void testMatchResourcesWithEmptyLabels() {

        ResourceCreate create = podamFactory.manufacturePojo(ResourceCreate.class);
        create.setLabels(Collections.emptyMap());
        String tenantId = RandomStringUtils.randomAlphanumeric(10);
        String resourceId = RandomStringUtils.randomAlphanumeric(10);
        create.setResourceId(resourceId);

        ResourceInfo info = new ResourceInfo()
            .setEnvoyId("e-1")
            .setTenantId(tenantId)
            .setResourceId(resourceId);

        when(envoyResourceManagement.getOne(any(), any()))
            .thenReturn(CompletableFuture.completedFuture(info));

        resourceManagement.createResource(tenantId, create);



        final Map<String, String> labels = new HashMap<>();
        labels.put("os", "DARWIN");
        labels.put("env", "test");

        ResourceCreate create2 = podamFactory.manufacturePojo(ResourceCreate.class);
        create2.setLabels(labels);
        resourceId = RandomStringUtils.randomAlphanumeric(10);
        create2.setResourceId(resourceId);
        resourceManagement.createResource(tenantId, create2);


        entityManager.flush();

        Page<Resource> resources = resourceManagement.getResourcesFromLabels(Collections.emptyMap(), tenantId, LabelSelectorMethod.OR, Pageable.unpaged());
        List<Map<String, String>> resourceLabels = resources.get().map(Resource::getLabels).collect(Collectors.toList());
        List<String> resourceIds = resources.get().map(Resource::getResourceId).collect(Collectors.toList());

        assertEquals(2L, resources.getTotalElements());
        assertEquals(tenantId, resources.getContent().get(0).getTenantId());
        assertThat(resourceLabels, containsInAnyOrder(create.getLabels(), create2.getLabels()));
        assertThat(resourceIds, containsInAnyOrder(create.getResourceId(), create2.getResourceId()));
    }

    //This test is supposed to make sure that when matching resources we are still only returning the tenants requested
    public void testMatchResourceWithNoLabelsAsOrRequestOnlyReturnsTenant() {
        final Map<String, String> labels = new HashMap<>();
        labels.put("os", "DARWIN");
        labels.put("env", "test");

        ResourceCreate create = podamFactory.manufacturePojo(ResourceCreate.class);
        create.setLabels(Collections.emptyMap());
        String tenantId = RandomStringUtils.randomAlphanumeric(10);
        resourceManagement.createResource(tenantId, create);
        resourceManagement.createResource(RandomStringUtils.randomAlphanumeric(10), create);
        entityManager.flush();

        Page<Resource> resources = resourceManagement.getResourcesFromLabels(Collections.emptyMap(), tenantId, LabelSelectorMethod.OR, Pageable.unpaged());
        assertEquals(1L, resources.getTotalElements()); //make sure we only returned the one value
        assertEquals(tenantId, resources.getContent().get(0).getTenantId());
        assertEquals(create.getResourceId(), resources.getContent().get(0).getResourceId());
        assertEquals(labels, resources.getContent().get(0).getLabels());
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
        String resourceId = RandomStringUtils.randomAlphanumeric(10);
        create.setResourceId(resourceId);
        String tenantId = RandomStringUtils.randomAlphanumeric(10);

        ResourceInfo info = new ResourceInfo()
            .setEnvoyId("e-1")
            .setTenantId(tenantId)
            .setResourceId(resourceId);

        when(envoyResourceManagement.getOne(any(), any()))
            .thenReturn(CompletableFuture.completedFuture(info));

        resourceManagement.createResource(tenantId, create);
        entityManager.flush();

        Page<Resource> resources = resourceManagement.getResourcesFromLabels(labels, tenantId, LabelSelectorMethod.AND, Pageable.unpaged());
        assertEquals(0L, resources.getTotalElements());
    }

    @Test
    public void testFailedMatchResourceWithMultipleLabelsUsingOr() {
        final Map<String, String> resourceLabels = new HashMap<>();
        resourceLabels.put("os", "DARWIN");
        resourceLabels.put("env", "test");

        final Map<String, String> labels = new HashMap<>();
        labels.put("os", "Windows");
        labels.put("env", "prod");

        ResourceCreate create = podamFactory.manufacturePojo(ResourceCreate.class);
        String resourceId = RandomStringUtils.randomAlphanumeric(10);
        create.setResourceId(resourceId);
        create.setLabels(resourceLabels);
        String tenantId = RandomStringUtils.randomAlphanumeric(10);

        ResourceInfo info = new ResourceInfo()
            .setEnvoyId("e-1")
            .setTenantId(tenantId)
            .setResourceId(resourceId);

        when(envoyResourceManagement.getOne(any(), any()))
            .thenReturn(CompletableFuture.completedFuture(info));

        resourceManagement.createResource(tenantId, create);
        entityManager.flush();



        Page<Resource> resources = resourceManagement.getResourcesFromLabels(labels, tenantId, LabelSelectorMethod.OR, Pageable.unpaged());
        assertEquals(0L, resources.getTotalElements());
    }

    @Test
    public void testMatchResourceWithSupersetOfLabels() {
        final Map<String, String> resourceLabels = new HashMap<>();
        resourceLabels.put("os", "DARWIN");
        resourceLabels.put("env", "test");
        resourceLabels.put("architecture", "x86");
        final Map<String, String> labels = new HashMap<>();
        labels.put("os", "DARWIN");
        labels.put("env", "test");

        ResourceCreate create = podamFactory.manufacturePojo(ResourceCreate.class);
        create.setLabels(resourceLabels);
        String resourceId = RandomStringUtils.randomAlphanumeric(10);
        create.setResourceId(resourceId);
        String tenantId = RandomStringUtils.randomAlphanumeric(10);

        ResourceInfo info = new ResourceInfo()
            .setEnvoyId("e-1")
            .setTenantId(tenantId)
            .setResourceId(resourceId);

        when(envoyResourceManagement.getOne(any(), any()))
            .thenReturn(CompletableFuture.completedFuture(info));

        resourceManagement.createResource(tenantId, create);
        entityManager.flush();

        Page<Resource> resources = resourceManagement.getResourcesFromLabels(labels, tenantId, LabelSelectorMethod.AND, Pageable.unpaged());
        assertEquals(1L, resources.getTotalElements()); //make sure we only returned the one value
        assertEquals(tenantId, resources.getContent().get(0).getTenantId());
        assertEquals(create.getResourceId(), resources.getContent().get(0).getResourceId());
        assertEquals(resourceLabels, resources.getContent().get(0).getLabels());
    }

    @Test
    public void testMatchResourceWithSupersetOfLabelsDoingOr() {
        final Map<String, String> resourceLabels = new HashMap<>();
        resourceLabels.put("os", "DARWIN");
        resourceLabels.put("env", "test");
        resourceLabels.put("architecture", "x86");
        final Map<String, String> labels = new HashMap<>();
        labels.put("os", "DARWIN");
        labels.put("env", "prod");

        ResourceCreate create = podamFactory.manufacturePojo(ResourceCreate.class);
        create.setLabels(resourceLabels);
        String tenantId = RandomStringUtils.randomAlphanumeric(10);
        String resourceId = RandomStringUtils.randomAlphanumeric(10);
        create.setResourceId(resourceId);

        ResourceInfo info = new ResourceInfo()
            .setEnvoyId("e-1")
            .setTenantId(tenantId)
            .setResourceId(resourceId);

        when(envoyResourceManagement.getOne(any(), any()))
            .thenReturn(CompletableFuture.completedFuture(info));

        resourceManagement.createResource(tenantId, create);
        entityManager.flush();

        Page<Resource> resources = resourceManagement.getResourcesFromLabels(labels, tenantId, LabelSelectorMethod.OR, Pageable.unpaged());
        assertEquals(1L, resources.getTotalElements()); //make sure we only returned the one value
        assertEquals(tenantId, resources.getContent().get(0).getTenantId());
        assertEquals(create.getResourceId(), resources.getContent().get(0).getResourceId());
        assertEquals(resourceLabels, resources.getContent().get(0).getLabels());
    }

    @Test
    public void testFailMatchResourceWithSupersetOfDifferentLabels() {
        final Map<String, String> resourceLabels = new HashMap<>();
        resourceLabels.put("os", "DARWIN");
        resourceLabels.put("env", "test");
        resourceLabels.put("architecture", "x86");
        final Map<String, String> labels = new HashMap<>();
        labels.put("os", "DARWIN");
        labels.put("env", "prod");

        ResourceCreate create = podamFactory.manufacturePojo(ResourceCreate.class);
        create.setLabels(resourceLabels);
        String resourceId = RandomStringUtils.randomAlphanumeric(10);
        create.setResourceId(resourceId);
        String tenantId = RandomStringUtils.randomAlphanumeric(10);
        ResourceInfo info = new ResourceInfo()
            .setEnvoyId("e-1")
            .setTenantId(tenantId)
            .setResourceId(resourceId);

        when(envoyResourceManagement.getOne(any(), any()))
            .thenReturn(CompletableFuture.completedFuture(info));

        resourceManagement.createResource(tenantId, create);
        entityManager.flush();

        Page<Resource> resources = resourceManagement.getResourcesFromLabels(labels, tenantId, LabelSelectorMethod.AND, Pageable.unpaged());
        assertEquals(0L, resources.getTotalElements());
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


        ResourceCreate create = podamFactory.manufacturePojo(ResourceCreate.class);
        String resourceId = RandomStringUtils.randomAlphanumeric(10);
        create.setResourceId(resourceId);
        create.setLabels(resourceLabels);
        String tenantId = RandomStringUtils.randomAlphanumeric(10);

        ResourceInfo info = new ResourceInfo()
            .setEnvoyId("e-1")
            .setTenantId(tenantId)
            .setResourceId(resourceId);

        when(envoyResourceManagement.getOne(any(), any()))
            .thenReturn(CompletableFuture.completedFuture(info));

        resourceManagement.createResource(tenantId, create);
        entityManager.flush();

        Page<Resource> resources = resourceManagement.getResourcesFromLabels(labels, tenantId, LabelSelectorMethod.AND, Pageable.unpaged());
        assertEquals(0L, resources.getTotalElements());
    }

    @Test
    public void testGetResourcesFromLabels_noLabels() {
        final Map<String, String> resourceLabels = new HashMap<>();
        resourceLabels.put("os", "DARWIN");
        resourceLabels.put("env", "test");

        ResourceCreate create = podamFactory.manufacturePojo(ResourceCreate.class);
        create.setLabels(resourceLabels);
        String tenantId = RandomStringUtils.randomAlphanumeric(10);
        String resourceId = RandomStringUtils.randomAlphanumeric(10);
        create.setResourceId(resourceId);

        ResourceInfo info = new ResourceInfo()
            .setEnvoyId("e-1")
            .setTenantId(tenantId)
            .setResourceId(resourceId);

        when(envoyResourceManagement.getOne(any(), any()))
            .thenReturn(CompletableFuture.completedFuture(info));

        resourceManagement.createResource(tenantId, create);
        entityManager.flush();

        Page<Resource> resources = resourceManagement.getResourcesFromLabels(Collections.emptyMap(), tenantId, LabelSelectorMethod.AND, Pageable.unpaged());
        assertThat(resources.getTotalElements(), equalTo(1L));
        assertThat(resources.getContent().get(0).getTenantId(), equalTo(tenantId));
        assertThat(resources.getContent().get(0).getResourceId(), equalTo(create.getResourceId()));
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

        Page<Resource> resources = resourceManagement.getResourcesFromLabels(
            Collections.singletonMap("os", "linux"),
            "testGetResourcesFromLabels_multipleMatches",
            LabelSelectorMethod.AND,
            Pageable.unpaged());
        assertThat(resources.getTotalElements(), equalTo(10L));
        assertThat(resources.getContent().get(0).getTenantId(), equalTo("testGetResourcesFromLabels_multipleMatches"));
    }

  @Test(expected = IllegalArgumentException.class)
  public void testUserLabelConflictsWithSystemNamespace() {
    final ResourceUpdate update = new ResourceUpdate()
        .setLabels(Collections.singletonMap(LabelNamespaces.EVENT_ENGINE_TAGS + "_account", "HackedAccount"));
    resourceManagement.updateResource(TENANT, RESOURCE_ID, update);
  }

    @Test
    public void testGetTenantResourceLabels() {
        Map<String, String> labels1 = new HashMap<>();
        labels1.put("key1", "value-1-1");
        labels1.put("key2", "value-2-1");
        labels1.put("key3", "value-3-1");
        persistResource("t-1", "r-1", labels1, Collections.emptyMap());
        Map<String, String> labels2 = new HashMap<>();
        labels2.put("key1", "value-1-1");
        labels2.put("key2", "value-2-2");
        labels2.put("key3", "value-3-2");
        persistResource("t-1", "r-2", labels2, Collections.emptyMap());
        Map<String, String> labels3 = new HashMap<>();
        labels3.put("key1", "value-1-2");
        labels3.put("key2", "value-2-2");
        labels3.put("key3", "value-3-2");
        persistResource("t-1", "r-3", labels3, Collections.emptyMap());
        Map<String, String> labels4 = new HashMap<>();
        labels4.put("key1", "value-1-x");
        labels4.put("key2", "value-2-x");
        labels4.put("key3", "value-3-x");
        persistResource("t-2", "r-4", labels4, Collections.emptyMap());

        entityManager.flush();

        final MultiValueMap<String, String> results = resourceManagement
            .getTenantResourceLabels("t-1");

        final MultiValueMap<String, String> expected = new LinkedMultiValueMap<>();
        expected.put("key1", Arrays.asList("value-1-1", "value-1-2"));
        expected.put("key2", Arrays.asList("value-2-1", "value-2-2"));
        expected.put("key3", Arrays.asList("value-3-1", "value-3-2"));
        assertThat(results, equalTo(expected));
    }

    @Test
    public void testGetTenantResourceMetadataKeys() {
        Map<String, String> metadata1 = new HashMap<>();
        metadata1.put("key1", "value-1-1");
        persistResource("t-1", "r-1", Collections.emptyMap(), metadata1);
        Map<String, String> metadata2 = new HashMap<>();
        metadata2.put("key2", "value-2-2");
        persistResource("t-1", "r-2", Collections.emptyMap(), metadata2);
        Map<String, String> metadata3 = new HashMap<>();
        metadata3.put("key2", "value-2-2");
        metadata3.put("key3", "value-3-2");
        persistResource("t-1", "r-3", Collections.emptyMap(), metadata3);
        Map<String, String> metadata4 = new HashMap<>();
        metadata4.put("key1", "value-1-x");
        metadata4.put("key2", "value-2-x");
        persistResource("t-2", "r-4", Collections.emptyMap(), metadata4);

        entityManager.flush();

        final List<String> results = resourceManagement
            .getTenantResourceMetadataKeys("t-1");

        assertThat(results, equalTo(Arrays.asList("key1", "key2", "key3")));
    }

    @Test
    public void testGetTenantResourceMetadataKeys_whenEmpty() {
        persistResource("t-1", "r-1", Collections.emptyMap(), Collections.emptyMap());

        entityManager.flush();

        final List<String> results = resourceManagement
            .getTenantResourceMetadataKeys("t-1");

        assertThat(results, hasSize(0));
    }

    @Test
    public void testSearchResource() {
      persistResource("t-1", "ping", Collections.emptyMap(), Collections.emptyMap());
      persistResource("t-1", "CPU", Collections.emptyMap(), Collections.emptyMap());
      persistResource("t-1", "databasingEverything", Collections.emptyMap(), Collections.emptyMap());
      persistResource("t-2", "ping", Collections.emptyMap(), Collections.emptyMap());

      Optional<Resource> resource = resourceManagement.getResource("t-1", "ping");

      Pageable page = PageRequest.of(0, 1, Sort.by("resourceId").descending());
      ResourceInfo info = new ResourceInfo()
          .setEnvoyId("e-1")
          .setTenantId("t-1")
          .setResourceId("resourceIdDoesntMatter");

      when(envoyResourceManagement.getOne(any(), any()))
          .thenReturn(CompletableFuture.completedFuture(info));

      Page<ResourceDTO> resources = resourceManagement.getResourcesBySearchString("t-1", "in", page);
      //Need to make sure we test the paging query so make sure the total number of elements is what we expect to find.
      assertThat(resources.getTotalElements(), equalTo(2L));
      assertThat(resources.getTotalPages(), equalTo(2));
      assertThat(resources.getNumberOfElements(), equalTo(1));
      assertThat(resources.get().findFirst().get().getResourceId(), equalTo(resource.get().getResourceId()));
    }

    @Test
    public void testRemoveAllTenantResources() {
      persistResource("t-1", "ping", Collections.emptyMap(), Collections.emptyMap());
      persistResource("t-1", "CPU", Collections.emptyMap(), Collections.emptyMap());
      persistResource("t-1", "databasingEverything", Collections.emptyMap(), Collections.emptyMap());
      persistResource("t-2", "ping", Collections.emptyMap(), Collections.emptyMap());

      resourceManagement.removeAllTenantResources("t-1", true);

      assertThat(resourceManagement.getResources("t-1", true, Pageable.unpaged()).getNumberOfElements(), equalTo(0));
      assertThat(resourceManagement.getResources("t-2", true, Pageable.unpaged()).getNumberOfElements(), equalTo(1));
      verify(kafkaEgress, times(3)).sendResourceEvent(any());
    }

    @Test
    public void testRemoveAllTenantResources_noEvents() {
        persistResource("t-1", "ping", Collections.emptyMap(), Collections.emptyMap());
        persistResource("t-1", "CPU", Collections.emptyMap(), Collections.emptyMap());
        persistResource("t-1", "databasingEverything", Collections.emptyMap(), Collections.emptyMap());
        persistResource("t-2", "ping", Collections.emptyMap(), Collections.emptyMap());

        resourceManagement.removeAllTenantResources("t-1", false);

        assertThat(resourceManagement.getResources("t-1", true, Pageable.unpaged()).getNumberOfElements(), equalTo(0));
        assertThat(resourceManagement.getResources("t-2", true, Pageable.unpaged()).getNumberOfElements(), equalTo(1));
    }

    private void persistResource(String tenantId, String resourceId, Map<String, String> labels,
                                 Map<String, String> metadata) {
        entityManager.persist(
            new Resource()
                .setTenantId(tenantId)
                .setResourceId(resourceId)
                .setLabels(labels)
                .setMetadata(metadata)
                .setPresenceMonitoringEnabled(true)
        );
    }
}