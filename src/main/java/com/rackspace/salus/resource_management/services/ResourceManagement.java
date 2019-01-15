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

import com.rackspace.salus.telemetry.errors.ResourceAlreadyExists;
import com.rackspace.salus.telemetry.events.ResourceEvent;
import com.rackspace.salus.telemetry.messaging.AttachEvent;
import com.rackspace.salus.telemetry.model.NotFoundException;
import com.rackspace.salus.telemetry.model.Resource;
import com.rackspace.salus.telemetry.model.ResourceIdentifier;
import com.rackspace.salus.telemetry.model.ResourceIdentifier_;
import com.rackspace.salus.telemetry.model.Resource_;
import com.rackspace.salus.telemetry.repositories.ResourceRepository;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;
import javax.persistence.EntityManager;
import javax.persistence.NoResultException;
import javax.persistence.PersistenceContext;
import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.CriteriaQuery;
import javax.persistence.criteria.Join;
import javax.persistence.criteria.Root;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.beanutils.BeanUtilsBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

@Slf4j
@Service
public class ResourceManagement {
    private final ResourceRepository resourceRepository;

    @PersistenceContext
    private final EntityManager entityManager;

    @Autowired
    public ResourceManagement(ResourceRepository resourceRepository, EntityManager entityManager) {
        this.resourceRepository = resourceRepository;
        this.entityManager = entityManager;
    }

    public Resource saveAndPublishResource(Resource resource, Map<String, String> oldLabels, boolean envoyChanged, String operation) {
        resourceRepository.save(resource);
        publishResourceEvent(resource, oldLabels, envoyChanged, operation);
        return resource;
    }

    public Resource getResource(String tenantId, String identifierName, String identifierValue) {
        CriteriaBuilder cb = entityManager.getCriteriaBuilder();
        CriteriaQuery<Resource> cr = cb.createQuery(Resource.class);
        Root<Resource> root = cr.from(Resource.class);
        Join<Resource, ResourceIdentifier> identifier = root.join(Resource_.resourceIdentifier);
        cr.select(root).where(cb.and(
                cb.equal(root.get(Resource_.tenantId), tenantId),
                cb.equal(identifier.get(ResourceIdentifier_.identifierName), identifierName),
                cb.equal(identifier.get(ResourceIdentifier_.identifierValue), identifierValue)));

        Resource result;
        try {
            result = entityManager.createQuery(cr).getSingleResult();
        } catch (NoResultException e) {
            result = null;
        }

        return result;
    }

    public List<Resource> getAllResources() {
        List<Resource> result = new ArrayList<>();
        resourceRepository.findAll().forEach(result::add);
        return result;
    }

    public List<?> getResources(String tenantId) {
        CriteriaBuilder cb = entityManager.getCriteriaBuilder();
        CriteriaQuery<Resource> cr = cb.createQuery(Resource.class);
        Root<Resource> root = cr.from(Resource.class);
        cr.select(root).where(
                cb.equal(root.get(Resource_.tenantId), tenantId));

        return entityManager.createQuery(cr).getResultList();
    }

    /**
    public List<Resource> getResources(String tenantId, Map<String, String> labels) {
        // use geoff's label search query
    }*/

    public Stream<Resource> getResources(boolean hasEnvoy) {
        CriteriaBuilder cb = entityManager.getCriteriaBuilder();
        CriteriaQuery<Resource> cr = cb.createQuery(Resource.class);
        Root<Resource> root = cr.from(Resource.class);

        cr.select(root).where(
                cb.equal(root.get(Resource_.presenceMonitoringEnabled), hasEnvoy));

        return entityManager.createQuery(cr).getResultStream();
    }

    public List<Resource> getResources(String tenantId, boolean hasEnvoy) {
        CriteriaBuilder cb = entityManager.getCriteriaBuilder();
        CriteriaQuery<Resource> cr = cb.createQuery(Resource.class);
        Root<Resource> root = cr.from(Resource.class);

        cr.select(root).where(cb.and(
                cb.equal(root.get(Resource_.tenantId), tenantId),
                cb.equal(root.get(Resource_.presenceMonitoringEnabled), hasEnvoy)));

        return entityManager.createQuery(cr).getResultList();
    }

    public Resource updateResource(String tenantId, String identifierName, String identifierValue, Resource updatedValues) throws ResourceAlreadyExists {
        Resource resource = getResource(tenantId, identifierName, identifierValue);

        if (!resource.getResourceIdentifier().toString().equals(updatedValues.getResourceIdentifier().toString())) {
            String newIdentifierName = updatedValues.getResourceIdentifier().getIdentifierName();
            String newIdentifierValue = updatedValues.getResourceIdentifier().getIdentifierValue();
            Resource existingWithIdentifier = getResource(tenantId, newIdentifierName, newIdentifierValue);

            if (existingWithIdentifier != null) {
                throw new ResourceAlreadyExists(String.format("Resource already exists with identifier %s on tenant %s",
                        updatedValues.getResourceIdentifier().toString(), tenantId));
            }
        }

        Map<String, String> oldLabels = new HashMap<>(resource.getLabels());
        boolean envoyChanged = resource.isPresenceMonitoringEnabled() != updatedValues.isPresenceMonitoringEnabled();

        try {
            log.info("Copying {} to {}.", updatedValues.toString(), resource.toString());
            nullAwareBeanCopy(resource, updatedValues);
            log.info("End result is {}", resource.toString());
        } catch (Exception e) {
            log.error(e.toString());
        }

        saveAndPublishResource(resource, oldLabels, envoyChanged, "update");

        return resource;
    }

    public static void nullAwareBeanCopy(Object dest, Object source) throws IllegalAccessException, InvocationTargetException {
        new BeanUtilsBean() {
            @Override
            public void copyProperty(Object dest, String name, Object value)
                    throws IllegalAccessException, InvocationTargetException {
                if(value != null) {
                    super.copyProperty(dest, name, value);
                }
            }
        }.copyProperties(dest, source);
    }

    public void removeResource(String tenantId, String identifierName, String identifierValue) {
        Resource resource = getResource(tenantId, identifierName, identifierValue);
        if (resource != null) {
            resourceRepository.deleteById(resource.getId());
        } else {
            throw new NotFoundException(String.format("No resource found for %s:%s on tenant %s",
                    identifierName, identifierValue, tenantId));
        }

    }

    @Transactional(propagation = Propagation.REQUIRED)
    public void handleEnvoyAttach(AttachEvent attachEvent) {
        String tenantId = attachEvent.getTenantId();
        String identifierName = attachEvent.getIdentifierName();
        String identifierValue = attachEvent.getIdentifierValue();
        Map<String, String> labels = attachEvent.getLabels();
        labels = applyNamespaceToKeys(labels, "envoy");

        ResourceIdentifier identifier = new ResourceIdentifier()
                .setIdentifierName(identifierName)
                .setIdentifierValue(identifierValue);

        Resource existing = getResource(tenantId, identifierName, identifierValue);

        if (existing == null) {
            log.debug("No resource found for new envoy attach");
            Resource newResource = new Resource()
                    .setTenantId(tenantId)
                    .setResourceIdentifier(identifier)
                    .setLabels(labels)
                    .setPresenceMonitoringEnabled(true);
            saveAndPublishResource(newResource, null, true, "create");
        } else {
            log.debug("Found existing resource related to envoy: {}", existing.toString());
            updateEnvoyLabels(existing, labels);
        }
    }

    private void updateEnvoyLabels(Resource resource, Map<String, String> envoyLabels) {
        log.debug("Processing new envoy with {} labels", envoyLabels.size());
        log.debug("Existing resource has {} labels", resource.getLabels().size());
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
            saveAndPublishResource(resource, oldLabels, false, "update");
        }
    }

    private void publishResourceEvent(Resource resource, Map<String, String> oldLabels, boolean envoyChanged, String operation) {
        ResourceEvent event = new ResourceEvent();
        event.setResource(resource);
        event.setOldLabels(oldLabels);
        event.setPresenceMonitorChange(envoyChanged);
        event.setTenantId(resource.getTenantId());
        event.setOperation(operation);


    }

    //public Resource migrateResourceToTenant(String oldTenantId, String newTenantId, String identifierName, String identifierValue) {}

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

    private void removePresenceMonitoring(String tenantId, String identifierName, String identifierValue) {
        Resource resource = getResource(tenantId, identifierName, identifierValue);
        if (resource == null) {
            log.debug("No resource found to remove presence monitoring");
        } else {
            resource.setPresenceMonitoringEnabled(false);
            saveAndPublishResource(resource, resource.getLabels(), true, "update");
        }
    }
}
