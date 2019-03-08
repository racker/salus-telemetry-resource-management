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
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import javax.validation.Valid;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.task.TaskExecutor;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import javax.validation.Valid;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

@Slf4j
@RestController
@RequestMapping("/api")
public class ResourceApi {
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

    @GetMapping("/tenant/{tenantId}/resourceLabels")
    public List<Resource> getResourcesWithLabels(@PathVariable String tenantId,
                                                 @RequestBody Map<String, String> labels) {
        return resourceManagement.getResourcesFromLabels(labels, tenantId);
    }

    @GetMapping("/tenant/{tenantId}/resourceIds/withEnvoyLabels")
    public List<String> getResourceIdsWithLabels(@PathVariable String tenantId,
                                                 @RequestParam Map<String, String> labels) {

        return resourceManagement.getResourceIdsWithEnvoyLabels(labels, tenantId);
    }
}