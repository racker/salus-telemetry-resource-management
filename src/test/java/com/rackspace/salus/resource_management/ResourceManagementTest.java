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

package com.rackspace.salus.resource_management.services;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.rackspace.salus.resource_management.services.services.ResourceManagement;
import com.rackspace.salus.telemetry.model.Resource;
import com.rackspace.salus.telemetry.model.ResourceIdentifier;
import com.rackspace.salus.telemetry.repositories.ResourceRepository;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.boot.test.autoconfigure.data.jdbc.DataJdbcTest;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import javax.persistence.EntityManager;
import java.util.Collections;

@RunWith(SpringRunner.class)
//@SpringBootTest
@EnableJpaRepositories("com.rackspace.salus.telemetry")
@EntityScan("com.rackspace.salus.telemetry")
@DataJdbcTest
@Transactional(propagation = Propagation.NOT_SUPPORTED)
public class ResourceManagementTest {

    @Autowired
    ResourceManagement resourceManagement;

    @Autowired
    ResourceRepository resourceRepository;

    @Before
    public void setUp() {
        Resource resource = new Resource()
                .setTenantId("abcde")
                .setResourceIdentifier(new ResourceIdentifier()
                        .setIdentifierName("host")
                        .setIdentifierValue("this"))
                .setLabels(Collections.singletonMap("key", "value"))
                .setPresenceMonitoringEnabled(false);

        resourceRepository.save(resource);
    }

    @Test
    public void testRegisterNewResource() {
    }

    @Test
    public void testGetResource() {
        Resource r = resourceManagement.getResource("abcde", "host", "this");

        assertThat(r.getId(), notNullValue());
        assertThat(r.getLabels(), hasEntry("key", "value"));
    }

}

