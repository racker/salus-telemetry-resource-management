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

package com.rackspace.salus.resource_management.web.event;

import javax.servlet.http.HttpServletResponse;

import org.springframework.context.ApplicationEvent;

public class ResourceCreatedEvent extends ApplicationEvent {
    private final HttpServletResponse response;
    private final String idOfNewResource;

    public ResourceCreatedEvent(final Object source, final HttpServletResponse response, final String idOfNewResource) {
        super(source);

        this.response = response;
        this.idOfNewResource = idOfNewResource;
    }

    // API

    public HttpServletResponse getResponse() {
        return response;
    }

    public String getIdOfNewResource() {
        return idOfNewResource;
    }

}