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

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import com.rackspace.salus.resource_management.services.KafkaEgress;
import com.rackspace.salus.resource_management.services.ResourceManagement;
import com.rackspace.salus.resource_management.web.model.ResourceUpdate;
import com.rackspace.salus.telemetry.errors.ResourceAlreadyExists;
import com.rackspace.salus.telemetry.messaging.AttachEvent;
import com.rackspace.salus.telemetry.model.Resource;
import com.rackspace.salus.telemetry.repositories.ResourceRepository;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Import;
import org.springframework.data.domain.PageRequest;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import uk.co.jemos.podam.api.PodamFactory;
import uk.co.jemos.podam.api.PodamFactoryImpl;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

@RunWith(SpringRunner.class)

@DataJpaTest
@Import({ResourceManagement.class})
//@Transactional(propagation = Propagation.NOT_SUPPORTED)
public class ResourceManagementTest {

    @Autowired
    ResourceManagement resourceManagement;

    @Autowired
    ResourceRepository resourceRepository;

    PodamFactory podamFactory = new PodamFactoryImpl();

    @MockBean
    KafkaEgress kafkaEgress;

    @Before
    public void setUp() {
        Resource resource = new Resource()
                .setTenantId("abcde")
                .setResourceId("host:test")
                .setLabels(Collections.singletonMap("key", "value"))
                .setPresenceMonitoringEnabled(false);

        resourceRepository.save(resource);
    }

    @Test
    public void testRegisterNewResource() {
    }

    @Test
    //@Transactional(propagation = Propagation.REQUIRED)
    public void testGetResource() {
        Resource r = resourceManagement.getResource("abcde", "host:test");

        assertThat(r.getId(), notNullValue());
        assertThat(r.getLabels(), hasEntry("key", "value"));
    }

    @Test
    public void testGetAll() {
        //assertThat(resourceManagement.getAllResources().size(), equalTo(1));
    }

    @Test
    //@Transactional(propagation = Propagation.REQUIRED)
    public void testNewEnvoyAttach() {
        AttachEvent attachEvent = podamFactory.manufacturePojo(AttachEvent.class);
        System.out.println(attachEvent.toString());
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
}