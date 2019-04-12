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

import static com.rackspace.salus.telemetry.model.LabelNamespaces.labelHasNamespace;

import com.rackspace.salus.resource_management.repositories.ResourceRepository;
import com.rackspace.salus.resource_management.web.model.ResourceCreate;
import com.rackspace.salus.resource_management.web.model.ResourceUpdate;
import com.rackspace.salus.telemetry.errors.ResourceAlreadyExists;
import com.rackspace.salus.telemetry.messaging.AttachEvent;
import com.rackspace.salus.telemetry.messaging.OperationType;
import com.rackspace.salus.telemetry.messaging.ResourceEvent;
import com.rackspace.salus.telemetry.model.LabelNamespaces;
import com.rackspace.salus.telemetry.model.NotFoundException;
import com.rackspace.salus.telemetry.model.Resource;
import com.rackspace.salus.telemetry.model.Resource_;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;
import javax.persistence.EntityManager;
import javax.persistence.NoResultException;
import javax.persistence.PersistenceContext;
import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.CriteriaQuery;
import javax.persistence.criteria.Root;
import javax.validation.Valid;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.PropertyMapper;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class ResourceManagement {
    private final ResourceRepository resourceRepository;
    private final KafkaEgress kafkaEgress;

    @PersistenceContext
    private final EntityManager entityManager;

    JdbcTemplate jdbcTemplate;

    @Autowired
    public ResourceManagement(ResourceRepository resourceRepository,
                              KafkaEgress kafkaEgress,
                              EntityManager entityManager,
                              JdbcTemplate jdbcTemplate) {
        this.resourceRepository = resourceRepository;
        this.kafkaEgress = kafkaEgress;
        this.entityManager = entityManager;
        this.jdbcTemplate = jdbcTemplate;

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
        log.debug("Saving resource: {}", resource);
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

    public List<Resource> getAllTenantResources(String tenantid) {
        return resourceRepository.findAllByTenantId(tenantid);
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

        if (newResource.getLabels() != null) {
            checkLabels(newResource.getLabels());
        }

        Resource resource = new Resource()
                .setTenantId(tenantId)
                .setResourceId(newResource.getResourceId())
                .setLabels(newResource.getLabels())
                .setPresenceMonitoringEnabled(newResource.getPresenceMonitoringEnabled())
                .setRegion(newResource.getRegion());

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
        if (updatedValues.getLabels() != null) {
            checkLabels(updatedValues.getLabels());
        }

        PropertyMapper map = PropertyMapper.get();
        map.from(updatedValues.getLabels())
                .whenNonNull()
                .to(resource::setLabels);
        map.from(updatedValues.getPresenceMonitoringEnabled())
                .whenNonNull()
                .to(resource::setPresenceMonitoringEnabled);
        map.from(updatedValues.getRegion())
                .whenNonNull()
                .to(resource::setRegion);

        saveAndPublishResource(resource, oldLabels, presenceMonitoringStateChange, OperationType.UPDATE);

        return resource;
    }

    private void checkLabels(Map<String,String> labels) {
        for (Entry<String, String> labelEntry : labels.entrySet()) {
            final String labelName = labelEntry.getKey();
            if (!LabelNamespaces.validateUserLabel(labelName)) {
                throw new IllegalArgumentException(String
                        .format("The given label '%s' conflicts with a system namespace",
                                labelName
                        ));
            }
        }
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
        log.debug("Handling Envoy attach: {}", attachEvent);

        String tenantId = attachEvent.getTenantId();
        String resourceId = attachEvent.getResourceId();
        Map<String, String> labels = attachEvent.getLabels();

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

            updateEnvoyLabels(existing, labels);
        }
    }

    /**
     * When provided with a list of envoy labels determine which ones need to be modified and perform an update.
     * @param resource The resource to update.
     * @param envoyLabels The list of labels received from a newly connected envoy.
     */
    private void updateEnvoyLabels(Resource resource, Map<String, String> envoyLabels) {
        final AtomicBoolean updated = new AtomicBoolean(false);
        final Map<String, String> oldResourceLabels = resource.getLabels();
        // Work with a new map to avoid mutating the labels in-place
        final Map<String, String> resourceLabels = new HashMap<>(oldResourceLabels);
        final Set<String> newEnvoyLabelKeys = new HashSet<>(envoyLabels.keySet());

        // The goals are:
        // 1. don't touch non-agent labels
        // 2. update envoy labels that have a new value
        // 3. remove envoy labels not in the given envoy labels
        // 4. add any new envoy labels

        oldResourceLabels.forEach((key, value) -> {
            final String envoyLabelValue = envoyLabels.get(key);
            if (envoyLabelValue != null) {
                if (!envoyLabelValue.equals(value)) {
                    // goal 2
                    updated.set(true);
                    resourceLabels.put(key, envoyLabelValue);
                }
                newEnvoyLabelKeys.remove(key);
            } else if (labelHasNamespace(key, LabelNamespaces.AGENT)) { // goal 1
                // goal 3
                updated.set(true);
                resourceLabels.remove(key);
            }
        });

        if (!newEnvoyLabelKeys.isEmpty()) {
            // goal 4
            newEnvoyLabelKeys.forEach(key -> resourceLabels.put(key, envoyLabels.get(key)));
            updated.set(true);
        }

        if (updated.get()) {
            resource.setLabels(resourceLabels);
            saveAndPublishResource(resource, oldResourceLabels, false, OperationType.UPDATE);
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

        log.debug("Publishing resource event: {}", event);
        kafkaEgress.sendResourceEvent(event);
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

    /**
     * takes in a Mapping of labels for a tenant, builds and runs the query to match to those labels
     * @param labels the labels that we need to match to
     * @param tenantId The tenant associated to the resource
     * @return the list of Resource's that match the labels or all tenant resources if no labels
     * given
     */
    public List<Resource> getResourcesFromLabels(Map<String, String> labels, String tenantId) {
        /*
        SELECT * FROM resources where id IN (SELECT id from resource_labels WHERE id IN (select id from resources)
        AND ((labels = "windows" AND labels_key = "os") OR (labels = "prod" AND labels_key="env")) GROUP BY id
        HAVING COUNT(id) = 2) AND tenant_id = "aaaad";
        */

        if(labels == null || labels.isEmpty()) {
            return getAllTenantResources(tenantId);
        }

        MapSqlParameterSource paramSource = new MapSqlParameterSource();
        paramSource.addValue("tenantId", tenantId);//AS r JOIN resource_labels AS rl
        StringBuilder builder = new StringBuilder("SELECT * FROM resources JOIN resource_labels AS rl WHERE resources.id = rl.id AND resources.id IN ");
        builder.append("(SELECT id from resource_labels WHERE id IN ( SELECT id FROM resources WHERE tenant_id = :tenantId) AND resources.id IN ");
        builder.append(" (SELECT id FROM resource_labels WHERE ");
        int i = 0;
        for(Map.Entry<String, String> entry : labels.entrySet()) {
            if(i > 0) {
                builder.append(" OR ");
            }
            builder.append("(labels = :label"+ i +" AND labels_key = :labelKey" + i + ")");
            paramSource.addValue("label"+i, entry.getValue());
            paramSource.addValue("labelKey"+i, entry.getKey());
            i++;
        }
        builder.append(" GROUP BY id HAVING COUNT(*) = :i)) ORDER BY resources.id");
        paramSource.addValue("i", i);

        NamedParameterJdbcTemplate namedParameterTemplate = new NamedParameterJdbcTemplate(jdbcTemplate.getDataSource());
        List<Resource> resources = new ArrayList<>();
        namedParameterTemplate.query(builder.toString(), paramSource, (resultSet)->{

            long prevId = 0;
            Resource prevResource = null;
            boolean iterate = true;

            while(iterate){
                if(resultSet.getLong("id") == prevId) {
                    prevResource.getLabels().put(
                            resultSet.getString("labels_key"),
                            resultSet.getString("labels"));
                }else {
                    Map<String, String> theseLabels = new HashMap<String, String>();
                    theseLabels.put(
                            resultSet.getString("labels_key"),
                            resultSet.getString("labels"));
                    Resource r = new Resource()
                            .setId(resultSet.getLong("id"))
                            .setResourceId(resultSet.getString("resource_id"))
                            .setTenantId(resultSet.getString("tenant_id"))
                            .setPresenceMonitoringEnabled(resultSet.getBoolean("presence_monitoring_enabled"))
                            .setLabels(theseLabels);
                    prevId = resultSet.getLong("id");
                    prevResource = r;
                    resources.add(r);

                }
                iterate = resultSet.next();
            }
        });

        return resources;
    }

}
