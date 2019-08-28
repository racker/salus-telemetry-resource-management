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

import com.rackspace.salus.resource_management.config.ResourceManagementProperties;
import com.rackspace.salus.telemetry.entities.Resource;
import com.rackspace.salus.telemetry.repositories.ResourceRepository;
import com.rackspace.salus.resource_management.web.model.ResourceCreate;
import com.rackspace.salus.resource_management.web.model.ResourceUpdate;
import com.rackspace.salus.telemetry.errors.AlreadyExistsException;
import com.rackspace.salus.telemetry.messaging.AttachEvent;
import com.rackspace.salus.telemetry.messaging.ResourceEvent;
import com.rackspace.salus.telemetry.model.LabelNamespaces;
import com.rackspace.salus.telemetry.model.NotFoundException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.persistence.EntityManager;
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
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;

@Slf4j
@Service
public class ResourceManagement {
  private final ResourceRepository resourceRepository;
  private final KafkaEgress kafkaEgress;

  JdbcTemplate jdbcTemplate;
  private final EntityManager entityManager;
  private final ResourceManagementProperties resourceManagementProperties;

  @Autowired
  public ResourceManagement(ResourceRepository resourceRepository,
                            KafkaEgress kafkaEgress,
                            JdbcTemplate jdbcTemplate,
                            EntityManager entityManager,
                            ResourceManagementProperties resourceManagementProperties) {
    this.resourceRepository = resourceRepository;
    this.kafkaEgress = kafkaEgress;
    this.jdbcTemplate = jdbcTemplate;
    this.entityManager = entityManager;
    this.resourceManagementProperties = resourceManagementProperties;
  }

  private void publishResourceEvent(ResourceEvent event) {
    kafkaEgress.sendResourceEvent(event);
  }

  /**
   * Creates or updates the resource depending on whether the ID already exists.
   * Also sends a resource event to kafka for consumption by other services.
   *
   * @param resource The resource object to create/update in the database.
   * @param labelsChanged
   * @param reattachedEnvoyId
   * @return
   */
  public Resource saveAndPublishResource(Resource resource, boolean labelsChanged,
      String reattachedEnvoyId) {
    log.debug("Saving resource: {}", resource);
    resourceRepository.save(resource);
    publishResourceEvent(
        new ResourceEvent()
            .setTenantId(resource.getTenantId())
            .setResourceId(resource.getResourceId())
            .setLabelsChanged(labelsChanged)
            .setReattachedEnvoyId(reattachedEnvoyId)
    );
    return resource;
  }

  /**
   * Tests whether the resource exists on the given tenant.
   * @param tenantId The tenant owning the resource.
   * @param resourceId The unique value representing the resource.
   * @return True if the resource exists on the tenant, otherwise false.
   */
  public boolean exists(String tenantId, String resourceId) {
    return resourceRepository.existsByTenantIdAndResourceId(tenantId, resourceId);
  }

  /**
   * Gets an individual resource object by the public facing id.
   * @param tenantId The tenant owning the resource.
   * @param resourceId The unique value representing the resource.
   * @return The resource object.
   */
  public Optional<Resource> getResource(String tenantId, String resourceId) {
    return resourceRepository.findByTenantIdAndResourceId(tenantId, resourceId);
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
    return resourceRepository.findAllByTenantId(tenantId, page);
  }

  public List<Resource> getAllTenantResources(String tenantId) {
    return resourceRepository.findAllByTenantId(tenantId);
  }
  /**
   * Get all resources where the presence monitoring field matches the parameter provided.
   * @param presenceMonitoringEnabled Whether presence monitoring is enabled or not.
   * @return Stream of resources.
   */
  public Stream<Resource> getResources(boolean presenceMonitoringEnabled) {
    return resourceRepository.findAllByPresenceMonitoringEnabled(presenceMonitoringEnabled).stream();
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
    List<Resource> resources =
        resourceRepository.findAllByTenantIdAndPresenceMonitoringEnabled(tenantId, presenceMonitoringEnabled);

    return new PageImpl<>(resources, page, resources.size());
  }

  /**
   * Create a new resource in the database and publish an event to kafka.
   * @param tenantId The tenant to create the entity for.
   * @param newResource The resource parameters to store.
   * @return The newly created resource.
   * @throws IllegalArgumentException
   * @throws AlreadyExistsException
   */
  public Resource createResource(String tenantId, @Valid ResourceCreate newResource) throws IllegalArgumentException, AlreadyExistsException {
    if (exists(tenantId, newResource.getResourceId())) {
      throw new AlreadyExistsException(String.format("Resource already exists with identifier %s on tenant %s",
          newResource.getResourceId(), tenantId));
    }

    if (newResource.getLabels() != null) {
      checkLabels(newResource.getLabels());
    }

    Resource resource = new Resource()
        .setTenantId(tenantId)
        .setResourceId(newResource.getResourceId())
        .setLabels(newResource.getLabels())
        .setMetadata(newResource.getMetadata())
        .setPresenceMonitoringEnabled(newResource.getPresenceMonitoringEnabled());

    resource = saveAndPublishResource(resource, true, null);

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
    Resource resource = getResource(tenantId, resourceId)
        .orElseThrow(() -> new NotFoundException(String.format("No resource found for %s on tenant %s",
            resourceId, tenantId)));

    Map<String, String> oldLabels = new HashMap<>(resource.getLabels());
    if (updatedValues.getLabels() != null) {
      checkLabels(updatedValues.getLabels());

      final Map<String, String> mergedLabels = Stream
          .concat(
              updatedValues.getLabels().entrySet().stream(),
              oldLabels.entrySet().stream()
                  .filter(entry -> labelHasNamespace(entry.getKey(), LabelNamespaces.AGENT))
          )
          .collect(Collectors.toMap(Entry::getKey, Entry::getValue));

      resource.setLabels(mergedLabels);
    }

    PropertyMapper map = PropertyMapper.get();
    map.from(updatedValues.getMetadata())
        .whenNonNull()
        .to(resource::setMetadata);
    map.from(updatedValues.getPresenceMonitoringEnabled())
        .whenNonNull()
        .to(resource::setPresenceMonitoringEnabled);
    saveAndPublishResource(resource, true, null);

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
    Resource resource = getResource(tenantId, resourceId).orElseThrow(() ->
        new NotFoundException(
            String.format("No resource found for %s on tenant %s", resourceId, tenantId)));

    resourceRepository.deleteById(resource.getId());
    publishResourceEvent(
        new ResourceEvent()
            .setTenantId(tenantId)
            .setResourceId(resourceId)
            .setDeleted(true)
    );
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

    Optional<Resource> existing = getResource(tenantId, resourceId);

    if (existing.isPresent()) {
      log.debug("Found existing resource related to envoy: {}", existing.get());

      updateEnvoyLabels(existing.get(), labels, attachEvent.getEnvoyId());
      log.debug("Found existing resource related to envoy: {}", existing.get());
    } else {
      log.debug("No resource found for new envoy attach");
      Resource newResource = new Resource()
          .setTenantId(tenantId)
          .setResourceId(resourceId)
          .setLabels(labels)
          .setPresenceMonitoringEnabled(true)
          .setAssociatedWithEnvoy(true);
      saveAndPublishResource(newResource, true, null);
    }
  }

  /**
   * When provided with a list of envoy labels determine which ones need to be modified and perform an update.
   * @param existingResource The resource to update.
   * @param envoyLabels The list of labels received from a newly connected envoy.
   * @param envoyId
   * @return the saved resource, if modified, or the given resource otherwise
   */
  private void updateEnvoyLabels(Resource existingResource, Map<String, String> envoyLabels,
      String envoyId) {
    final Map<String, String> oldResourceLabels = existingResource.getLabels();
    // Work with a new map to avoid mutating the labels in-place
    final Map<String, String> resourceLabels = new HashMap<>(oldResourceLabels);
    final Set<String> newEnvoyLabelKeys = new HashSet<>(envoyLabels.keySet());

    // The goals are:
    // 1. don't touch non-agent labels
    // 2. update envoy labels that have a new value
    // 3. remove envoy labels not in the given envoy labels
    // 4. add any new envoy labels

    boolean labelsChanged = false;
    for (Entry<String, String> entry : oldResourceLabels.entrySet()) {
      String k = entry.getKey();
      String value = entry.getValue();
      final String envoyLabelValue = envoyLabels.get(k);
      if (envoyLabelValue != null) {
        if (!envoyLabelValue.equals(value)) {
          // goal 2
          labelsChanged = true;
          resourceLabels.put(k, envoyLabelValue);
        }
        newEnvoyLabelKeys.remove(k);
      } else if (labelHasNamespace(k, LabelNamespaces.AGENT)) { // goal 1
        // goal 3
        labelsChanged = true;
        resourceLabels.remove(k);
      }
    }

    if (!newEnvoyLabelKeys.isEmpty()) {
      // goal 4
      newEnvoyLabelKeys.forEach(key -> resourceLabels.put(key, envoyLabels.get(key)));
      labelsChanged = true;
    }

    // This is a re-attachment if the existing resource was already associated with an Envoy before
    final boolean reattached = existingResource.isAssociatedWithEnvoy();

    // If the labels changed above or this is first association with Envoy
    if (labelsChanged || !reattached) {
      // ...then save it

      existingResource.setAssociatedWithEnvoy(true);
      existingResource.setLabels(resourceLabels);

      log.debug("Saving resource due to Envoy attachment: {}", existingResource);
      resourceRepository.save(existingResource);
    }

    // If labels changed or this is a re-attachment
    if (labelsChanged || reattached) {
      // ...then send a resource changed event

      publishResourceEvent(
          new ResourceEvent()
              .setTenantId(existingResource.getTenantId())
              .setResourceId(existingResource.getResourceId())
              .setLabelsChanged(labelsChanged)
              .setReattachedEnvoyId(reattached ? envoyId : null)
      );
    }
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
    Resource resource = getResource(tenantId, resourceId).orElse(
        new Resource().setTenantId(tenantId).setResourceId(resourceId)
    );
    resource.setPresenceMonitoringEnabled(false);
    saveAndPublishResource(resource, false, null);
  }

  /**
   * takes in a Mapping of labels for a tenant, builds and runs the query to match to those labels
   * @param labels the labels that we need to match to
   * @param tenantId The tenant associated to the resource
   * @return the list of Resource's that match the labels or all tenant resources if no labels
   * given
   */
  public Page<Resource> getResourcesFromLabels(Map<String, String> labels, String tenantId, Pageable page) {
        /*
        SELECT * FROM resources where id IN (SELECT id from resource_labels WHERE id IN (select id from resources)
        AND ((labels = "windows" AND labels_key = "os") OR (labels = "prod" AND labels_key="env")) GROUP BY id
        HAVING COUNT(id) = 2) AND tenant_id = "aaaad";
        */

    if(labels == null || labels.isEmpty()) {
      return getPagedResults(getAllTenantResources(tenantId), page);
    }

    MapSqlParameterSource paramSource = new MapSqlParameterSource();
    paramSource.addValue("tenantId", tenantId);//AS r JOIN resource_labels AS rl
    StringBuilder builder = new StringBuilder("SELECT resources.id as id FROM resources JOIN resource_labels AS rl WHERE resources.id = rl.id AND resources.id IN ");
    builder.append("(SELECT id from resource_labels WHERE id IN ( SELECT id FROM resources WHERE tenant_id = :tenantId) AND resources.id IN ");
    builder.append(" (SELECT id FROM resource_labels WHERE ");
    int i = 0;
    for(Map.Entry<String, String> entry : labels.entrySet()) {
      if(i > 0) {
        builder.append(" OR ");
      }
      builder.append("(labels = :label").append(i)
          .append(" AND labels_key = :labelKey").append(i)
          .append(")");
      paramSource.addValue("label"+i, entry.getValue());
      paramSource.addValue("labelKey"+i, entry.getKey());
      i++;
    }
    builder.append(" GROUP BY id HAVING COUNT(*) = :i)) ORDER BY resources.id");
    paramSource.addValue("i", i);

    NamedParameterJdbcTemplate namedParameterTemplate = new NamedParameterJdbcTemplate(jdbcTemplate.getDataSource());
    final List<Long> resourceIds = namedParameterTemplate.query(builder.toString(), paramSource,
        (resultSet, rowIndex) -> resultSet.getLong(1)
    );

    final List<Resource> resources = new ArrayList<>();
    for (Resource resource : resourceRepository.findAllById(resourceIds)) {
      resources.add(resource);
    }

    return getPagedResults(resources, page);
  }

  public MultiValueMap<String, String> getTenantResourceLabels(String tenantId) {
    final List<Map.Entry> distinctLabelTuples = entityManager.createNamedQuery(
        "Resource.getDistinctLabels", Map.Entry.class)
        .setParameter("tenantId", tenantId)
        .getResultList();

    final MultiValueMap<String,String> combined = new LinkedMultiValueMap<>();
    for (Entry entry : distinctLabelTuples) {
      combined.add((String)entry.getKey(), (String)entry.getValue());
    }
    return combined;
  }

  public List<String> getTenantResourceMetadataKeys(String tenantId) {
    /*
    NOTE: since metadata is stored as a json text column there's not really an efficient way
    to pull out just the metadata keys. As a result, the following iterates over just the
    metadata of the tenant's resources and picks out the distinct keys.
     */
    @SuppressWarnings("unchecked")
    final Stream<Map<String,String>> resultStream = entityManager
        .createNamedQuery("Resource.getMetadata")
        .setParameter("tenantId", tenantId)
        .setHint("org.hibernate.fetchSize", resourceManagementProperties.getResourceMetadataFetchSize())
        .getResultStream();

    return resultStream
        .flatMap(map -> map.keySet().stream()
            .map(k -> (String) k)
        )
        .distinct()
        .collect(Collectors.toList());
  }

  /**
   * Converts a list of resources into a Page containing only the requested elements.
   * @param resources The full list of resources found.
   * @param page The page details that were requested.
   * @return A Page object containing only the resources that were requested.
   */
  @SuppressWarnings("Duplicates")
  private Page<Resource> getPagedResults(List<Resource> resources, Pageable page) {
    if (page.isPaged()) {
      int start = page.getPageSize() * page.getPageNumber();
      int end = start + page.getPageSize();
      if (end > resources.size()) {
        end = resources.size();
      }
      List<Resource> results;
      try {
        results = resources.subList(start, end);
      } catch (IndexOutOfBoundsException | IllegalArgumentException e) {
        results = Collections.emptyList();
      }
      return new PageImpl<>(results, page, resources.size());
    } else {
      return new PageImpl<>(resources, page, resources.size());
    }
  }

  public Collection<String> getLabelNamespaces() {
    return LabelNamespaces.getNamespaces();
  }
}
