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
import com.rackspace.salus.resource_management.web.event.PaginatedResultsRetrievedEvent;
import com.rackspace.salus.resource_management.web.event.ResourceCreatedEvent;
import com.rackspace.salus.resource_management.web.event.SingleResourceRetrievedEvent;
import com.rackspace.salus.telemetry.errors.ResourceAlreadyExists;
import com.rackspace.salus.telemetry.model.NotFoundException;
import com.rackspace.salus.telemetry.model.Resource;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;
import org.springframework.web.util.UriComponentsBuilder;

import javax.servlet.http.HttpServletResponse;
import javax.validation.Valid;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Stream;

@Slf4j
@RestController
@RequestMapping("/api")
public class ResourceApi {

    private ResourceManagement resourceManagement;
    private ApplicationEventPublisher eventPublisher;

    @Autowired
    public ResourceApi(ResourceManagement resourceManagement, ApplicationEventPublisher eventPublisher) {
        this.resourceManagement = resourceManagement;
        this.eventPublisher = eventPublisher;
    }

    @GetMapping("/resources")
    public List<Resource> getAll(@RequestParam(defaultValue = "100") int size,
                                 @RequestParam(defaultValue = "0") int page,
                                 UriComponentsBuilder uriBuilder,
                                 final HttpServletResponse response) {

        Page<Resource> result = resourceManagement.getAllResources(PageRequest.of(page, size));
        eventPublisher.publishEvent(new PaginatedResultsRetrievedEvent<>(
                Resource.class, uriBuilder, response, page, result.getTotalPages(), size));
        return result.getContent();
    }

    @GetMapping("/envoys")
    public SseEmitter getAllWithPresenceMonitoringAsStream() {
        SseEmitter emitter = new SseEmitter();
        ExecutorService nonBlockingService = Executors.newCachedThreadPool();
        Stream<Resource> resourcesWithEnvoys = resourceManagement.getResources(true);
        nonBlockingService.execute(() -> {
            resourcesWithEnvoys.forEach(r -> {
                try {
                    emitter.send(r);
                } catch (IOException e) {
                    emitter.completeWithError(e);
                }
            });
            emitter.complete();
        });
        nonBlockingService.shutdown();
        return emitter;
    }

    @GetMapping("/tenant/{tenantId}/resources/{resourceId}")
    public Resource getByResourceId(@PathVariable String tenantId,
                                    @PathVariable String resourceId,
                                    final HttpServletResponse response) throws NotFoundException {

        Resource resource = resourceManagement.getResource(tenantId, resourceId);
        if (resource == null) {
            throw new NotFoundException(String.format("No resource found for %s on tenant %s",
                    resourceId, tenantId));
        }
        eventPublisher.publishEvent(new SingleResourceRetrievedEvent(this, response));
        return resource;
    }

    @GetMapping("/tenant/{tenantId}")
    public List<?> getAllForTenant(@PathVariable String tenantId,
                                   @RequestParam(defaultValue = "100") int size,
                                   @RequestParam(defaultValue = "0") int page,
                                   UriComponentsBuilder uriBuilder,
                                   final HttpServletResponse response) {

        Page<Resource> result = resourceManagement.getResources(tenantId, PageRequest.of(page, size));
        eventPublisher.publishEvent(new PaginatedResultsRetrievedEvent<>(
                Resource.class, uriBuilder, response, page, result.getTotalPages(), size));
        return result.getContent();
    }

    @PostMapping("/tenant/{tenantId}/resources")
    @ResponseStatus(HttpStatus.CREATED)
    public Resource create(@PathVariable String tenantId,
                           @Valid @RequestBody final ResourceCreate input,
                           final HttpServletResponse response) throws IllegalArgumentException, ResourceAlreadyExists {

        Resource created = resourceManagement.createResource(tenantId, input);
        eventPublisher.publishEvent(new ResourceCreatedEvent(this, response, created.getResourceId()));

        return created;
    }

    @PutMapping("/tenant/{tenantId}/resources/{resourceId}")
    public Resource update(@PathVariable String tenantId,
                           @PathVariable String resourceId,
                           @Valid @RequestBody final ResourceUpdate input,
                           final HttpServletResponse response) throws IllegalArgumentException, IllegalAccessException, InvocationTargetException {

        Resource updated = resourceManagement.updateResource(tenantId, resourceId, input);
        eventPublisher.publishEvent(new SingleResourceRetrievedEvent(this, response));

        return updated;
    }

    @DeleteMapping("/tenant/{tenantId}/resources/{resourceId}")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void delete(@PathVariable String tenantId,
                       @PathVariable String resourceId) {
        resourceManagement.removeResource(tenantId, resourceId);
    }
}