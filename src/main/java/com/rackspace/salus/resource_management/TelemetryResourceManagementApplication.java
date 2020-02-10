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

package com.rackspace.salus.resource_management;

import com.rackspace.salus.common.messaging.EnableSalusKafkaMessaging;
import com.rackspace.salus.common.util.DumpConfigProperties;
import com.rackspace.salus.common.web.EnableExtendedErrorAttributes;
import com.rackspace.salus.common.web.EnableRoleBasedJsonViews;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.security.servlet.SecurityAutoConfiguration;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication(exclude = {SecurityAutoConfiguration.class})
@EnableScheduling
@EnableSalusKafkaMessaging
@EnableExtendedErrorAttributes
@EnableRoleBasedJsonViews
public class TelemetryResourceManagementApplication {

  public static void main(String[] args) {
    DumpConfigProperties.process(args);

    SpringApplication.run(TelemetryResourceManagementApplication.class, args);
  }

}