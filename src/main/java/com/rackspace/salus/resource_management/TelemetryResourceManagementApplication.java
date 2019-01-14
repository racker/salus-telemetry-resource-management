package com.rackspace.salus.resource_management;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;

@SpringBootApplication
@EnableJpaRepositories("com.rackspace.salus.telemetry")
@EntityScan("com.rackspace.salus.telemetry")
public class TelemetryResourceManagementApplication {

    public static void main(String[] args) {
        SpringApplication.run(TelemetryResourceManagementApplication.class, args);
    }

}

