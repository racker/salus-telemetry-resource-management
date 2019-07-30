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

package com.rackspace.salus.resource_management.repositories;

import com.rackspace.salus.resource_management.entities.Resource;
import java.util.List;
import java.util.Optional;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.PagingAndSortingRepository;


public interface ResourceRepository extends PagingAndSortingRepository<Resource, Long> {

  List<Resource> findAllByTenantId(String tenantId);

  boolean existsByTenantIdAndResourceId(String tenantId, String resourceId);

  List<Resource> findAllByPresenceMonitoringEnabled(boolean value);

  Page<Resource> findAllByTenantId(String tenantId, Pageable pageable);

  Optional<Resource> findByTenantIdAndResourceId(String tenantId, String resourceId);

  List<Resource> findAllByTenantIdAndPresenceMonitoringEnabled(String tenantId, boolean presenceMonitoringEnabled);
}
