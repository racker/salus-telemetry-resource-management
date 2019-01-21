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

import com.rackspace.salus.resource_management.web.model.ResourceCreate;
import com.rackspace.salus.resource_management.web.model.ResourceUpdate;
import com.rackspace.salus.telemetry.errors.ResourceAlreadyExists;
import com.rackspace.salus.telemetry.messaging.ResourceEvent;
import com.rackspace.salus.telemetry.model.*;
import com.rackspace.salus.telemetry.messaging.*;
import com.rackspace.salus.telemetry.repositories.ResourceRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.PropertyMapper;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;

import javax.persistence.EntityManager;
import javax.persistence.NoResultException;
import javax.persistence.PersistenceContext;
import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.CriteriaQuery;
import javax.persistence.criteria.Root;
import javax.validation.Valid;
import java.lang.reflect.InvocationTargetException;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;

@Slf4j
@Service
public class ResourceManagement {
    private final ResourceRepository resourceRepository;
    private final KafkaEgress kafkaEgress;

    @PersistenceContext
    private final EntityManager entityManager;

    @Autowired
    public ResourceManagement(ResourceRepository resourceRepository, KafkaEgress kafkaEgress, EntityManager entityManager) {
        this.resourceRepository = resourceRepository;
        this.kafkaEgress = kafkaEgress;
        this.entityManager = entityManager;
    }

    /**
     * Creates or updates the resource depending on whether the ID already exists.
     * Also sends a resource event to kafka for consumption by other services.
     *
     * @param resource The resource object to create/update in the database.
     * @param oldLabels The labels of the resource prior to any modifications.
     * @param presenceMonitoringStateChanged Whether the presence monitoring flag has been switched.
     * @param operation
     * @return
     */
    public Resource saveAndPublishResource(Resource resource, Map<String, String> oldLabels,
                                           boolean presenceMonitoringStateChanged, OperationType operation) {
        resourceRepository.save(resource);
        publishResourceEvent(resource, oldLabels, presenceMonitoringStateChanged, operation);
        return resource;
    }

    public Resource getResource(String tenantId, String resourceId) {
        CriteriaBuilder cb = entityManager.getCriteriaBuilder();
        CriteriaQuery<Resource> cr = cb.createQuery(Resource.class);
        Root<Resource> root = cr.from(Resource.class);
        cr.select(root).where(cb.and(
                cb.equal(root.get(Resource_.tenantId), tenantId),
                cb.equal(root.get(Resource_.resourceId), resourceId)));

        Resource result;
        try {
            result = entityManager.createQuery(cr).getSingleResult();
        } catch (NoResultException e) {
            result = null;
        }

        return result;
    }

    public Page<Resource> getAllResources(Pageable page) {
        return resourceRepository.findAll(page);
    }

    public Page<Resource> getResources(String tenantId, Pageable page) {
        CriteriaBuilder cb = entityManager.getCriteriaBuilder();
        CriteriaQuery<Resource> cr = cb.createQuery(Resource.class);
        Root<Resource> root = cr.from(Resource.class);
        cr.select(root).where(
                cb.equal(root.get(Resource_.tenantId), tenantId));

        List<Resource> resources = entityManager.createQuery(cr).getResultList();

        return new PageImpl<>(resources, page, resources.size());
    }

    /**
    public List<Resource> getResources(String tenantId, Map<String, String> labels) {
        // use geoff's label search query
    }*/

    public Stream<Resource> getResources(boolean presenceMonitoringEnabled) {
        CriteriaBuilder cb = entityManager.getCriteriaBuilder();
        CriteriaQuery<Resource> cr = cb.createQuery(Resource.class);
        Root<Resource> root = cr.from(Resource.class);

        cr.select(root).where(
                cb.equal(root.get(Resource_.presenceMonitoringEnabled), presenceMonitoringEnabled));

        return entityManager.createQuery(cr).getResultStream();
    }

    public List<Resource> getResources(String tenantId, boolean presenceMonitoringEnabled) {
        CriteriaBuilder cb = entityManager.getCriteriaBuilder();
        CriteriaQuery<Resource> cr = cb.createQuery(Resource.class);
        Root<Resource> root = cr.from(Resource.class);

        cr.select(root).where(cb.and(
                cb.equal(root.get(Resource_.tenantId), tenantId),
                cb.equal(root.get(Resource_.presenceMonitoringEnabled), presenceMonitoringEnabled)));

        return entityManager.createQuery(cr).getResultList();
    }

    public Resource createResource(String tenantId, @Valid ResourceCreate newResource) throws IllegalArgumentException, ResourceAlreadyExists {
        Resource existing = getResource(tenantId, newResource.getResourceId());
        if (existing != null) {
            throw new ResourceAlreadyExists(String.format("Resource already exists with identifier %s on tenant %s",
                    newResource.getResourceId(), tenantId));
        }

        Resource resource = new Resource()
                .setTenantId(tenantId)
                .setResourceId(newResource.getResourceId())
                .setLabels(newResource.getLabels())
                .setPresenceMonitoringEnabled(newResource.getPresenceMonitoringEnabled());

        resource = saveAndPublishResource(resource, null, resource.getPresenceMonitoringEnabled(), OperationType.CREATE);

        return resource;
    }

    public Resource updateResource(String tenantId, String resourceId, @Valid ResourceUpdate updatedValues) {
        Resource resource = getResource(tenantId, resourceId);
        if (resource == null) {
            throw new NotFoundException(String.format("No resource found for %s on tenant %s",
                    resourceId, tenantId));
        }
        Map<String, String> oldLabels = new HashMap<>(resource.getLabels());
        boolean presenceMonitoringStateChange = false;
        if (updatedValues.getPresenceMonitoringEnabled() != null) {
            presenceMonitoringStateChange = resource.getPresenceMonitoringEnabled().booleanValue()
                    != updatedValues.getPresenceMonitoringEnabled().booleanValue();
        }

        PropertyMapper map = PropertyMapper.get();
        map.from(updatedValues.getLabels())
                .whenNonNull()
                .to(resource::setLabels);
        map.from(updatedValues.getPresenceMonitoringEnabled())
                .whenNonNull()
                .to(resource::setPresenceMonitoringEnabled);

        saveAndPublishResource(resource, oldLabels, presenceMonitoringStateChange, OperationType.UPDATE);

        return resource;
    }

    public void removeResource(String tenantId, String resourceId) {
        Resource resource = getResource(tenantId, resourceId);
        if (resource != null) {
            resourceRepository.deleteById(resource.getId());
        } else {
            throw new NotFoundException(String.format("No resource found for %s on tenant %s",
                    resourceId, tenantId));
        }

    }

    public void handleEnvoyAttach(AttachEvent attachEvent) {
        String tenantId = attachEvent.getTenantId();
        String resourceId = attachEvent.getResourceId();
        Map<String, String> labels = attachEvent.getLabels();
        labels = applyNamespaceToKeys(labels, "envoy");

        Resource existing = getResource(tenantId, resourceId);

        if (existing == null) {
            log.debug("No resource found for new envoy attach");
            Resource newResource = new Resource()
                    .setTenantId(tenantId)
                    .setResourceId(resourceId)
                    .setLabels(labels)
                    .setPresenceMonitoringEnabled(true);
            saveAndPublishResource(newResource, null, true, OperationType.CREATE);
        } else {
            log.debug("Found existing resource related to envoy: {}", existing.toString());

            Map<String,String> oldLabels = new HashMap<>(existing.getLabels());

            updateEnvoyLabels(existing, labels);
            saveAndPublishResource(existing, oldLabels, false, OperationType.UPDATE);
        }
    }

    private void updateEnvoyLabels(Resource resource, Map<String, String> envoyLabels) {
        AtomicBoolean updated = new AtomicBoolean(false);
        Map<String, String> resourceLabels = resource.getLabels();
        Map<String, String> oldLabels = new HashMap<>(resourceLabels);

        resource.getLabels().forEach((name, value) -> {
            if (envoyLabels.containsKey(name)) {
                if (envoyLabels.get(name) != value) {
                    updated.set(true);
                    resourceLabels.put(name, value);
                }
            } else {
                updated.set(true);
                resourceLabels.put(name, value);
            }
        });
        if (updated.get()) {
            resource.setLabels(resourceLabels);
            saveAndPublishResource(resource, oldLabels, false, OperationType.UPDATE);
        }
    }

    private void publishResourceEvent(Resource resource, Map<String, String> oldLabels, boolean envoyChanged, OperationType operation) {
        ResourceEvent event = new ResourceEvent();
        event.setResource(resource);
        event.setOldLabels(oldLabels);
        event.setPresenceMonitorChange(envoyChanged);
        event.setOperation(operation);

        kafkaEgress.sendResourceEvent(event);
    }

    /**
     * Receives a map of strings and adds the given namespace as a prefix to the key.
     * @param map The map to modify.
     * @param namespace Prefix to apply to map's keys.
     * @return Original map but with the namespace prefix applied to all keys.
     */
    private Map<String, String> applyNamespaceToKeys(Map<String, String> map, String namespace) {
        Map<String, String> prefixedMap = new HashMap<>();
        map.forEach((name, value) -> {
            prefixedMap.put(namespace + "." + name, value);
        });
        return prefixedMap;
    }

    /**
     * This can be used to force a presence monitoring change if it is currently running but should not be.
     *
     * If the resource no longer exists, we will still send an event in case presence monitoring is still active.
     * This case will also ensure the monitor mgmt service removed any active monitors.
     *
     * @param tenantId The tenant associated to the resource.
     * @param resourceId THe id of the resource we need to disable monitoring of.
     */
    private void removePresenceMonitoring(String tenantId, String resourceId) {
        Resource resource = getResource(tenantId, resourceId);
        if (resource == null) {
            log.debug("No resource found to remove presence monitoring");
            resource = new Resource()
                    .setTenantId(tenantId)
                    .setResourceId(resourceId)
                    .setPresenceMonitoringEnabled(false);
        } else {
            resource.setPresenceMonitoringEnabled(false);
        }

        saveAndPublishResource(resource, resource.getLabels(), true, OperationType.UPDATE);
    }

    //public Resource migrateResourceToTenant(String oldTenantId, String newTenantId, String identifierName, String identifierValue) {}
}
