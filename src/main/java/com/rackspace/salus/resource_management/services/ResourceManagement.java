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
import org.hibernate.Session;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.PropertyMapper;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;

import javax.persistence.EntityManager;
import javax.persistence.NoResultException;
import javax.persistence.PersistenceContext;
import javax.persistence.Query;
import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.CriteriaQuery;
import javax.persistence.criteria.Root;
import javax.validation.Valid;
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

    private static final String ENVOY_NAMESPACE = "envoy.";

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
     * @param operation The type of event that occurred. e.g. create, update, or delete.
     * @return
     */
    public Resource saveAndPublishResource(Resource resource, Map<String, String> oldLabels,
                                           boolean presenceMonitoringStateChanged, OperationType operation) {
        resourceRepository.save(resource);
        publishResourceEvent(resource, oldLabels, presenceMonitoringStateChanged, operation);
        return resource;
    }

    /**
     * Gets an individual resource object by the public facing id.
     * @param tenantId The tenant owning the resource.
     * @param resourceId The unique value representing the resource.
     * @return The resource object.
     */
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

    /**
     * Get a selection of resource objects across all accounts.
     * @param page The slice of results to be returned.
     * @return The resources found that match the page criteria.
     */
    public Page<Resource> getAllResources(Pageable page) {
        return resourceRepository.findAll(page);
    }

    /**
     * Same as {@link #getAllResources(Pageable page) getAllResources} except restricted to a single tenant.
     * @param tenantId The tenant to select resources from.
     * @param page The slice of results to be returned.
     * @return The resources found for the tenant that match the page criteria.
     */
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

    /**
     * Get all resources where the presence monitoring field matches the parameter provided.
     * @param presenceMonitoringEnabled Whether presence monitoring is enabled or not.
     * @return Stream of resources.
     */
    public Stream<Resource> getResources(boolean presenceMonitoringEnabled) {
        CriteriaBuilder cb = entityManager.getCriteriaBuilder();
        CriteriaQuery<Resource> cr = cb.createQuery(Resource.class);
        Root<Resource> root = cr.from(Resource.class);

        cr.select(root).where(
                cb.equal(root.get(Resource_.presenceMonitoringEnabled), presenceMonitoringEnabled));

        return entityManager.createQuery(cr).getResultStream();
    }

    /**
     * Similar to {@link #getResources(boolean presenceMonitoringEnabled) getResources} except restricted to a
     * single tenant, and returns a list.
     * @param tenantId The tenant to select resources from.
     * @param presenceMonitoringEnabled Whether presence monitoring is enabled or not.
     * @param page The slice of results to be returned.
     * @return A page or resources matching the given criteria.
     */
    public Page<Resource> getResources(String tenantId, boolean presenceMonitoringEnabled, Pageable page) {
        CriteriaBuilder cb = entityManager.getCriteriaBuilder();
        CriteriaQuery<Resource> cr = cb.createQuery(Resource.class);
        Root<Resource> root = cr.from(Resource.class);

        cr.select(root).where(cb.and(
                cb.equal(root.get(Resource_.tenantId), tenantId),
                cb.equal(root.get(Resource_.presenceMonitoringEnabled), presenceMonitoringEnabled)));

        List<Resource> resources = entityManager.createQuery(cr).getResultList();

        return new PageImpl<>(resources, page, resources.size());
    }

    /**
     * Create a new resource in the database and publish an event to kafka.
     * @param tenantId The tenant to create the entity for.
     * @param newResource The resource parameters to store.
     * @return The newly created resource.
     * @throws IllegalArgumentException
     * @throws ResourceAlreadyExists
     */
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

    /**
     * Update an existing resource and publish an event to kafka.
     * @param tenantId The tenant to create the entity for.
     * @param resourceId The id of the existing resource.
     * @param updatedValues The new resource parameters to store.
     * @return The newly updated resource.
     */
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

    /**
     * Delete a resource and publish an event to kafka.
     * @param tenantId The tenant the resource belongs to.
     * @param resourceId The id of the resource.
     */
    public void removeResource(String tenantId, String resourceId) {
        Resource resource = getResource(tenantId, resourceId);
        if (resource != null) {
            resourceRepository.deleteById(resource.getId());
            publishResourceEvent(resource, null, resource.getPresenceMonitoringEnabled(), OperationType.DELETE);
        } else {
            throw new NotFoundException(String.format("No resource found for %s on tenant %s",
                    resourceId, tenantId));
        }
    }

    /**
     * Registers or updates resources in the datastore.
     * Prefixes the labels received from the envoy so they do not clash with any api specified values.
     *
     * @param attachEvent The event triggered from the Ambassador by any envoy attachment.
     */
    public void handleEnvoyAttach(AttachEvent attachEvent) {
        String tenantId = attachEvent.getTenantId();
        String resourceId = attachEvent.getResourceId();
        Map<String, String> labels = attachEvent.getLabels();
        labels = applyNamespaceToKeys(labels, ENVOY_NAMESPACE);

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

    /**
     * When provided with a list of envoy labels determine which ones need to be modified and perform an update.
     * @param resource The resource to update.
     * @param envoyLabels The list of labels received from a newly connected envoy.
     */
    private void updateEnvoyLabels(Resource resource, Map<String, String> envoyLabels) {
        AtomicBoolean updated = new AtomicBoolean(false);
        Map<String, String> resourceLabels = resource.getLabels();
        Map<String, String> oldLabels = new HashMap<>(resourceLabels);

        oldLabels.entrySet().stream()
            .filter(entry -> entry.getKey().startsWith(ENVOY_NAMESPACE))
            .forEach(entry -> {
                if (envoyLabels.containsKey(entry.getKey())) {
                    if (envoyLabels.get(entry.getKey()) != entry.getValue()) {
                        updated.set(true);
                        resourceLabels.put(entry.getKey(), entry.getValue());
                    }
                } else {
                    updated.set(true);
                    resourceLabels.remove(entry.getKey());
                }
            });
        if (updated.get()) {
            saveAndPublishResource(resource, oldLabels, false, OperationType.UPDATE);
        }
    }

    /**
     * Publish a resource event to kafka for consumption by other services.
     * @param resource The updated resource the operation was performed on.
     * @param oldLabels The resource labels prior to any update.
     * @param presenceMonitoringStateChanged Whether the presence monitoring flag has been switched.
     * @param operation The type of event that occurred. e.g. create, update, or delete.
     */
    private void publishResourceEvent(Resource resource, Map<String, String> oldLabels, boolean presenceMonitoringStateChanged, OperationType operation) {
        ResourceEvent event = new ResourceEvent();
        event.setResource(resource);
        event.setOldLabels(oldLabels);
        event.setPresenceMonitorChange(presenceMonitoringStateChanged);
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
            prefixedMap.put(namespace + name, value);
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

    public Query constructQuery(Map<String, String> labels, String tenantId) {
        /*
        SELECT * FROM resources where id IN (SELECT id from resource_labels WHERE id IN (select id from resources)
        AND ((labels = "windows" AND labels_key = "os") OR (labels = "prod" AND labels_key="env")) GROUP BY id
        HAVING COUNT(id) = 2) AND tenant_id = "aaaad";
        */
        StringBuilder builder = new StringBuilder("SELECT r FROM Resource as r WHERE id IN ");
        builder.append("(SELECT id from resource_labels WHERE id IN ( SELECT id FROM resources WHERE tenant_id = :tenant_id) AND ");

        int i = 0;
        labels.size();
        for(Map.Entry<String, String> entry : labels.entrySet()) {
            if(i > 0) {
                builder.append(" OR ");
            }
            builder.append("(labels = :label"+ i +" AND labels_key = :key" + i + ")");
            i++;
        }
        builder.append(" GROUP BY id HAVING COUNT(id) = :i)");
        //CriteriaQuery<Resource> query = session.getCriteriaBuilder().createQuery(Resource.class);

        Query actualQuery = entityManager.createQuery(builder.toString());
        //Query actualQuery = entityManager.createQuery("select r from Resource as r");
        /*actualQuery.setParameter("tenant_id", tenantId);
        actualQuery.setParameter("i", i);
        actualQuery.setParameter("star", "*");
        i = 0;
        for(Map.Entry<String, String> entry : labels.entrySet()) {
            actualQuery.setParameter("label"+i, entry.getValue());
            actualQuery.setParameter("key"+i, entry.getKey());
            i++;
        }*/

        return actualQuery;
    }

    //public Resource migrateResourceToTenant(String oldTenantId, String newTenantId, String identifierName, String identifierValue) {}
}
