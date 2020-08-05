package com.rackspace.salus.resource_management.config;

import javax.validation.constraints.Min;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;
import org.springframework.validation.annotation.Validated;

@ConfigurationProperties("salus.resource-management")
@Component
@Data
@Validated
public class ResourceManagementProperties {

  /**
   * Specifies the number of rows fetched per roundtrip when querying metadata keys.
   */
  @Min(0)
  int resourceMetadataFetchSize = 100;
}
