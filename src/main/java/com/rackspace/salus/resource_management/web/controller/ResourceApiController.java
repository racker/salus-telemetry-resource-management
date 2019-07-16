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

import com.fasterxml.jackson.annotation.JsonView;
import com.rackspace.salus.resource_management.services.ResourceManagement;
import com.rackspace.salus.resource_management.web.model.ResourceCreate;
import com.rackspace.salus.resource_management.web.model.ResourceDTO;
import com.rackspace.salus.resource_management.web.model.ResourceUpdate;
import com.rackspace.salus.telemetry.errors.AlreadyExistsException;
import com.rackspace.salus.telemetry.model.LabelNamespaces;
import com.rackspace.salus.telemetry.model.NotFoundException;
import com.rackspace.salus.resource_management.entities.Resource;
import com.rackspace.salus.telemetry.model.PagedContent;
import com.rackspace.salus.telemetry.model.View;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;
import javax.validation.Valid;


import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.task.TaskExecutor;
import org.springframework.data.domain.Pageable;
import org.springframework.http.HttpStatus;
import org.springframework.util.MultiValueMap;
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
import io.swagger.annotations.*;


@Slf4j
@RestController
@Api(description = "Resource operations", authorizations = {
    @Authorization(value = "repose_auth",
        scopes = {
            @AuthorizationScope(scope = "write:resource", description = "modify resources in your account"),
            @AuthorizationScope(scope = "read:resource", description = "read your resource"),
            @AuthorizationScope(scope = "delete:resource", description = "delete your resource")
        })
})
@RequestMapping("/api")
public class ResourceApiController {
  private ResourceManagement resourceManagement;
  private TaskExecutor taskExecutor;

  @Autowired
  public ResourceApiController(ResourceManagement resourceManagement, TaskExecutor taskExecutor) {
    this.resourceManagement = resourceManagement;
    this.taskExecutor = taskExecutor;
  }

  @GetMapping("/admin/resources")
  @JsonView(View.Admin.class)
  @ApiOperation(value = "Gets all Resources irrespective of Tenant")
  public PagedContent<ResourceDTO> getAll(Pageable pageable) {

    return PagedContent.fromPage(resourceManagement.getAllResources(pageable)
        .map(Resource::toDTO));
  }

  @GetMapping("/envoys")
  @JsonView(View.Admin.class)
  public SseEmitter getAllWithPresenceMonitoringAsStream() {
    SseEmitter emitter = new SseEmitter();
    Stream<Resource> resourcesWithEnvoys = resourceManagement.getResources(true);
    taskExecutor.execute(() -> {
      resourcesWithEnvoys.forEach(r -> {
        try {
          emitter.send(r.toDTO());
        } catch (IOException e) {
          emitter.completeWithError(e);
        }
      });
      emitter.complete();
    });
    return emitter;
  }

  @GetMapping("/tenant/{tenantId}/resources/{resourceId}")
  @ApiOperation(value = "Gets specific Resource for specific Tenant")
  @JsonView(View.Public.class)
  public ResourceDTO getByResourceId(@PathVariable String tenantId,
      @PathVariable String resourceId) throws NotFoundException {

    Optional<Resource> resource = resourceManagement.getResource(tenantId, resourceId);
    return resource.map(Resource::toDTO).orElseThrow(() -> new NotFoundException(String.format("No resource found for %s on tenant %s",
        resourceId, tenantId)));
  }

  @GetMapping("/tenant/{tenantId}/resources")
  @ApiOperation(value = "Gets all Resources for authenticated tenant")
  @JsonView(View.Public.class)
  public PagedContent<ResourceDTO>  getAllForTenant(@PathVariable String tenantId, Pageable pageable) {

    return PagedContent.fromPage(resourceManagement.getResources(tenantId, pageable)
        .map(Resource::toDTO));
  }

  @PostMapping("/tenant/{tenantId}/resources")
  @ResponseStatus(HttpStatus.CREATED)
  @ApiOperation(value = "Create one Resource for Tenant")
  @ApiResponses(value = { @ApiResponse(code = 201, message = "Successfully Created Resource")})
  @JsonView(View.Public.class)
  public ResourceDTO create(@PathVariable String tenantId,
      @Valid @RequestBody final ResourceCreate input)
      throws IllegalArgumentException, AlreadyExistsException {
    return resourceManagement.createResource(tenantId, input).toDTO();
  }

  @PutMapping("/tenant/{tenantId}/resources/{resourceId}")
  @ApiOperation(value = "Updates specific Resource for Tenant")
  @JsonView(View.Public.class)
  public ResourceDTO update(@PathVariable String tenantId,
      @PathVariable String resourceId,
      @Valid @RequestBody final ResourceUpdate input) throws IllegalArgumentException {
    return resourceManagement.updateResource(tenantId, resourceId, input).toDTO();
  }

  @DeleteMapping("/tenant/{tenantId}/resources/{resourceId}")
  @ResponseStatus(HttpStatus.NO_CONTENT)
  @ApiOperation(value = "Gets all Resources for authenticated tenant")
  @ApiResponses(value = { @ApiResponse(code = 204, message = "Resource Deleted")})
  @JsonView(View.Public.class)
  public void delete(@PathVariable String tenantId,
      @PathVariable String resourceId) {
    resourceManagement.removeResource(tenantId, resourceId);
  }

  @GetMapping("/tenant/{tenantId}/resources-by-label")
  @JsonView(View.Public.class)
  public PagedContent<ResourceDTO> getResourcesWithLabels(@PathVariable String tenantId,
      @RequestParam Map<String, String> labels, Pageable pageable) {
    return PagedContent.fromPage(resourceManagement.getResourcesFromLabels(labels, tenantId, pageable)
        .map(Resource::toDTO));
  }

  @GetMapping("/tenant/{tenantId}/resource-labels")
  @JsonView(View.Public.class)
  public MultiValueMap<String,String> getResourceLabels(@PathVariable String tenantId) {
    return resourceManagement.getTenantResourceLabels(tenantId);
  }

  @GetMapping("/tenant/{tenantId}/resource-metadata-keys")
  @JsonView(View.Public.class)
  public List<String> getResourceMetadataKeys(@PathVariable String tenantId) {
    return resourceManagement.getTenantResourceMetadataKeys(tenantId);
  }

  @GetMapping("/tenant/{tenantId}/resource-label-namespaces")
  public Collection<String> getLabelNamespaces(@PathVariable String tenantId) {
    return resourceManagement.getLabelNamespaces();
  }
}
