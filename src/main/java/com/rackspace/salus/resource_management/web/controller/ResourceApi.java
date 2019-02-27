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

package com.rackspace.salus.resource_management.web.controller;

import com.rackspace.salus.resource_management.services.ResourceManagement;
import com.rackspace.salus.resource_management.web.model.ResourceCreate;
import com.rackspace.salus.resource_management.web.model.ResourceUpdate;
import com.rackspace.salus.telemetry.errors.ResourceAlreadyExists;
import com.rackspace.salus.telemetry.model.NotFoundException;
import com.rackspace.salus.telemetry.model.Resource;
import lombok.extern.slf4j.Slf4j;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.query.NativeQuery;
import org.hibernate.query.Query;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.task.TaskExecutor;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import javax.persistence.criteria.CriteriaQuery;
import javax.validation.Valid;
import java.io.IOException;
import java.util.Map;
import java.util.stream.Stream;

@Slf4j
@RestController
@RequestMapping("/api")
public class ResourceApi {
    private static SessionFactory factory;
    private ResourceManagement resourceManagement;
    private TaskExecutor taskExecutor;

    @Autowired
    public ResourceApi(ResourceManagement resourceManagement, TaskExecutor taskExecutor) {
        this.resourceManagement = resourceManagement;
        this.taskExecutor = taskExecutor;
    }

    @GetMapping("/resources")
    public Page<Resource> getAll(@RequestParam(defaultValue = "100") int size,
                                 @RequestParam(defaultValue = "0") int page) {

        return resourceManagement.getAllResources(PageRequest.of(page, size));

    }

    @GetMapping("/envoys")
    public SseEmitter getAllWithPresenceMonitoringAsStream() {
        SseEmitter emitter = new SseEmitter();
        Stream<Resource> resourcesWithEnvoys = resourceManagement.getResources(true);
        taskExecutor.execute(() -> {
            resourcesWithEnvoys.forEach(r -> {
                try {
                    emitter.send(r);
                } catch (IOException e) {
                    emitter.completeWithError(e);
                }
            });
            emitter.complete();
        });
        return emitter;
    }

    @GetMapping("/tenant/{tenantId}/resources/{resourceId}")
    public Resource getByResourceId(@PathVariable String tenantId,
                                    @PathVariable String resourceId) throws NotFoundException {

        Resource resource = resourceManagement.getResource(tenantId, resourceId);
        if (resource == null) {
            throw new NotFoundException(String.format("No resource found for %s on tenant %s",
                    resourceId, tenantId));
        }
        return resource;
    }

    @GetMapping("/tenant/{tenantId}/resources")
    public Page<Resource>  getAllForTenant(@PathVariable String tenantId,
                                   @RequestParam(defaultValue = "100") int size,
                                   @RequestParam(defaultValue = "0") int page) {

        return resourceManagement.getResources(tenantId, PageRequest.of(page, size));
    }

    @PostMapping("/tenant/{tenantId}/resources")
    @ResponseStatus(HttpStatus.CREATED)
    public Resource create(@PathVariable String tenantId,
                           @Valid @RequestBody final ResourceCreate input)
            throws IllegalArgumentException, ResourceAlreadyExists {
        return resourceManagement.createResource(tenantId, input);
    }

    @PutMapping("/tenant/{tenantId}/resources/{resourceId}")
    public Resource update(@PathVariable String tenantId,
                           @PathVariable String resourceId,
                           @Valid @RequestBody final ResourceUpdate input) throws IllegalArgumentException {
        return resourceManagement.updateResource(tenantId, resourceId, input);
    }

    @DeleteMapping("/tenant/{tenantId}/resources/{resourceId}")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void delete(@PathVariable String tenantId,
                       @PathVariable String resourceId) {
        resourceManagement.removeResource(tenantId, resourceId);
    }

    @GetMapping("/tenant/{tenantId}/resources/{labels}")
    public Resource getResourcesWithLabels(@PathVariable String tenantId,
                                                @PathVariable Map<String, String> labels) {

        Query query = constructQuery(labels, tenantId);
        query.getResultList();
        return null;
    }

    private Query constructQuery(Map<String, String> labels, String tenantId) {
        //SELECT * FROM resources where id IN (SELECT id from resource_labels WHERE id IN (select id from resources)
        // AND ((labels = "windows" AND labels_key = "os") OR (labels = "prod" AND labels_key="env")) GROUP BY id
        // HAVING COUNT(id) = 2) AND tenant_id = "aaaad";

        Session session = factory.getCurrentSession();


        String query = "SELECT * FROM resources WHERE id IN (SELECT id FROM resources id IN ";
        StringBuilder builder = new StringBuilder(query);
        builder.append("SELECT id from resource_labels WHERE id IN ( SELECT id FROM resources WHERE tenant_id = :tenant_id) AND ");

        int i = 0;
        for(Map.Entry<String, String> entry : labels.entrySet()) {
            if(i > 0) {
                builder.append(" OR ");
            }
            builder.append("labels = :label"+ i +" AND labels_key = :key" + i);
            i++;
        }
        builder.append(") GROUP BY id HAVING COUNT(id) = :i");
        //CriteriaQuery<Resource> query = session.getCriteriaBuilder().createQuery(Resource.class);

        Query actualQuery = session.createSQLQuery(query);
        actualQuery.setParameter("tenant_id", tenantId);
        actualQuery.setParameter("i", i);
        i = 0;
        for(Map.Entry<String, String> entry : labels.entrySet()) {
            actualQuery.setParameter("label"+i, entry.getValue());
            actualQuery.setParameter("key"+i, entry.getKey());
            i++;
        }

        return actualQuery;
    }
}