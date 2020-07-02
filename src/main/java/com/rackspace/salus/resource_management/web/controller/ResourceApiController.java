/*
 * Copyright 2020 Rackspace US, Inc.
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
 *
 */

package com.rackspace.salus.resource_management.web.controller;

import com.rackspace.salus.resource_management.services.ResourceManagement;
import com.rackspace.salus.resource_management.web.model.ResourceCreate;
import com.rackspace.salus.resource_management.web.model.ResourceDTO;
import com.rackspace.salus.resource_management.web.model.ResourceUpdate;
import com.rackspace.salus.telemetry.entities.Resource;
import com.rackspace.salus.telemetry.errors.AlreadyExistsException;
import com.rackspace.salus.telemetry.model.LabelSelectorMethod;
import com.rackspace.salus.telemetry.model.NotFoundException;
import com.rackspace.salus.telemetry.model.PagedContent;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import io.swagger.annotations.Authorization;
import io.swagger.annotations.AuthorizationScope;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import javax.validation.Valid;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.data.web.SpringDataWebProperties;
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
  private final SpringDataWebProperties springDataWebProperties;

  @Autowired
  public ResourceApiController(ResourceManagement resourceManagement, TaskExecutor taskExecutor,
                               SpringDataWebProperties springDataWebProperties) {
    this.resourceManagement = resourceManagement;
    this.taskExecutor = taskExecutor;
    this.springDataWebProperties = springDataWebProperties;
  }

  @GetMapping("/admin/resources")
  @ApiOperation(value = "Gets all Resources irrespective of Tenant")
  public PagedContent<ResourceDTO> getAll(Pageable pageable) {
    return PagedContent.fromPage(resourceManagement.getAllResourceDTOs(pageable));
  }

  @GetMapping("/envoys")
  public SseEmitter getAllWithPresenceMonitoringAsStream() {
    SseEmitter emitter = new SseEmitter();
    Stream<Resource> resourcesWithEnvoys = resourceManagement.getResources(true);
    taskExecutor.execute(() -> {
      resourcesWithEnvoys.forEach(r -> {
        try {
          emitter.send(new ResourceDTO(r, null));
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
  public ResourceDTO getByResourceId(@PathVariable String tenantId,
      @PathVariable String resourceId) throws NotFoundException {
    return resourceManagement.getResourceDTO(tenantId, resourceId);
  }

  @GetMapping("/tenant/{tenantId}/resources")
  @ApiOperation(value = "Gets all Resources for authenticated tenant")
  public PagedContent<ResourceDTO>  getAllForTenant(@PathVariable String tenantId, Pageable pageable) {

    return PagedContent.fromPage(resourceManagement.getResourceDTOs(tenantId, pageable));
  }

  @PostMapping("/tenant/{tenantId}/resources")
  @ResponseStatus(HttpStatus.CREATED)
  @ApiOperation(value = "Create one Resource for Tenant")
  @ApiResponses(value = { @ApiResponse(code = 201, message = "Successfully Created Resource")})
  public ResourceDTO create(@PathVariable String tenantId,
      @Valid @RequestBody final ResourceCreate input)
      throws IllegalArgumentException, AlreadyExistsException {
    return resourceManagement.createResource(tenantId, input);
  }

  @PutMapping("/tenant/{tenantId}/resources/{resourceId}")
  @ApiOperation(value = "Updates specific Resource for Tenant")
  public ResourceDTO update(@PathVariable String tenantId,
      @PathVariable String resourceId,
      @Valid @RequestBody final ResourceUpdate input) throws IllegalArgumentException {
    return resourceManagement.updateResource(tenantId, resourceId, input);
  }

  @DeleteMapping("/tenant/{tenantId}/resources/{resourceId}")
  @ResponseStatus(HttpStatus.NO_CONTENT)
  @ApiOperation(value = "Gets all Resources for authenticated tenant")
  @ApiResponses(value = { @ApiResponse(code = 204, message = "Resource Deleted")})
  public void delete(@PathVariable String tenantId,
      @PathVariable String resourceId) {
    resourceManagement.removeResource(tenantId, resourceId);
  }

  @GetMapping("/admin/resources-by-label/{tenantId}/{logicalOperator}")
  public List<ResourceDTO> getAllTenantResourcesWithLabels(@PathVariable String tenantId,
                                                           @RequestParam Map<String, String> labels,
                                                           @PathVariable LabelSelectorMethod logicalOperator) {
    return resourceManagement
        .getResourceDTOsFromLabels(labels, tenantId, logicalOperator, Pageable.unpaged()).getContent();
  }

  @GetMapping("/tenant/{tenantId}/resources-by-label/{logicalOperator}")
  public PagedContent<ResourceDTO> getPagedResourcesWithLabels(@PathVariable String tenantId,
      @RequestParam Map<String, String> labels, @PathVariable LabelSelectorMethod logicalOperator,
                                                          Pageable pageable) {

    // labels is a catch-all for request/query parameters, so need to strip out the pageable parameters
    final Map<String, String> resourceLabels = new HashMap<>(labels);
    resourceLabels.remove(springDataWebProperties.getPageable().getSizeParameter());
    resourceLabels.remove(springDataWebProperties.getPageable().getPageParameter());

    return PagedContent.fromPage(
        resourceManagement
            .getResourceDTOsFromLabels(resourceLabels, tenantId, logicalOperator, pageable));
  }

  @GetMapping("/tenant/{tenantId}/resource-labels")
  @ApiOperation("Lists the label keys and the values for each that are currently in use on resources")
  public MultiValueMap<String,String> getResourceLabels(@PathVariable String tenantId) {
    return resourceManagement.getTenantResourceLabels(tenantId);
  }

  @GetMapping("/tenant/{tenantId}/resource-metadata-keys")
  @ApiOperation("Lists the metadata keys current in use on resources")
  public List<String> getResourceMetadataKeys(@PathVariable String tenantId) {
    return resourceManagement.getTenantResourceMetadataKeys(tenantId);
  }

  @GetMapping("/tenant/{tenantId}/resource-label-namespaces")
  @ApiOperation("Lists the label namespaces that are reserved for use by the system")
  public Collection<String> getLabelNamespaces(@PathVariable String tenantId) {
    return resourceManagement.getLabelNamespaces();
  }

  @GetMapping("/tenant/{tenantId}/search/{searchCriteria}")
  @ApiOperation("Find all resources that match the searchCriteria in the ResourceId")
  public PagedContent<Resource> getResourcesSearch(@PathVariable String tenantId, @PathVariable String searchCriteria, Pageable page) {
    return PagedContent.fromPage(resourceManagement.resourceSearch(tenantId, searchCriteria, page));
  }
}
