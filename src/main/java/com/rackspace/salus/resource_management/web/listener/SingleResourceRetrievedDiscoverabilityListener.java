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

import com.google.common.base.Preconditions;
import com.google.common.net.HttpHeaders;
import com.rackspace.salus.resource_management.web.event.SingleResourceRetrievedEvent;
import com.rackspace.salus.resource_management.web.util.LinkUtil;
import org.springframework.context.ApplicationListener;
import org.springframework.stereotype.Component;
import org.springframework.web.servlet.support.ServletUriComponentsBuilder;

import javax.servlet.http.HttpServletResponse;

@Component
class SingleResourceRetrievedDiscoverabilityListener
        implements ApplicationListener<SingleResourceRetrievedEvent> {

    @Override
    public void onApplicationEvent(SingleResourceRetrievedEvent resourceRetrievedEvent){
        Preconditions.checkNotNull(resourceRetrievedEvent);

        HttpServletResponse response = resourceRetrievedEvent.getResponse();
        addLinkHeaderOnSingleResourceRetrieval(response);
    }
    private void addLinkHeaderOnSingleResourceRetrieval(HttpServletResponse response){
        String requestURL = ServletUriComponentsBuilder.fromCurrentRequestUri().
                build().toUri().toASCIIString();
        int positionOfLastSlash = requestURL.lastIndexOf("/");
        String uriForResourceCreation = requestURL.substring(0, positionOfLastSlash);

        String linkHeaderValue = LinkUtil
                .createLinkHeader(uriForResourceCreation, "collection");
        response.addHeader(HttpHeaders.LINK, linkHeaderValue);
    }
}