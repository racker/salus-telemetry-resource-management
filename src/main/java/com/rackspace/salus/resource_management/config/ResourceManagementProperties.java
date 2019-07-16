package com.rackspace.salus.resource_management.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@ConfigurationProperties("salus.resource-management")
@Component
@Data
public class ResourceManagementProperties {

  /**
   * Specifies the number of rows fetched per roundtrip when querying metadata keys.
   */
  int resourceMetadataFetchSize = 100;
}
