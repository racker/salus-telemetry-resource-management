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

package com.rackspace.salus.resource_management.web.listener;

import java.net.URI;

import javax.servlet.http.HttpServletResponse;

import com.google.common.net.HttpHeaders;
import com.rackspace.salus.resource_management.web.event.ResourceCreatedEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.stereotype.Component;
import org.springframework.web.servlet.support.ServletUriComponentsBuilder;

import com.google.common.base.Preconditions;

@Component
class ResourceCreatedDiscoverabilityListener implements ApplicationListener<ResourceCreatedEvent> {

    @Override
    public void onApplicationEvent(final ResourceCreatedEvent resourceCreatedEvent) {
        Preconditions.checkNotNull(resourceCreatedEvent);

        final HttpServletResponse response = resourceCreatedEvent.getResponse();
        final String idOfNewResource = resourceCreatedEvent.getIdOfNewResource();

        addLinkHeaderOnResourceCreation(response, idOfNewResource);
    }

    void addLinkHeaderOnResourceCreation(final HttpServletResponse response, final String idOfNewResource) {
        final URI uri = ServletUriComponentsBuilder.fromCurrentRequestUri().path("/{idOfNewResource}").buildAndExpand(idOfNewResource).toUri();
        response.setHeader(HttpHeaders.LOCATION, uri.toASCIIString());
    }

}