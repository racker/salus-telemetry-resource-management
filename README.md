# Dev Dependencies

The kafka and mysql containers from [salus-telemetry-bundle](https://github.com/racker/salus-telemetry-bundle#runningdeveloping-locally) must be running.

[salus-telemetry-model](https://github.com/racker/salus-telemetry-model) must be recent and have been built to generate the `Resource_` sources under `target/classes/generated-sources/annotations/com.rackspace.salus.telemetry.model`.  If building the `-model` module via IntelliJ does not create this, you should try a `mvn clean` in that project before building.


The `dev` profile should be set to ensure the properties from `application-dev.yml` get picked up.

# Dev Testing

To see events being posted to Kafka you can run this command:
```
docker exec -it telemetry-infra_kafka_1 \
kafka-console-consumer --bootstrap-server localhost:9093 --topic telemetry.resources.json
```

You can trigger these events to be posted by utilizing some of the API operations below.

# API Operations
Examples of a subset of the available API operations.

## Create a new resource
```
echo '{"resourceId": "host1", "presenceMonitoringEnabled": true}' | http POST 'localhost:8085/api/tenant/aaaaa/resources'
```

## Update an existing resource
```
echo '{"presenceMonitoringEnabled": false}' | http PUT 'localhost:8085/api/tenant/aaaaa/resources/host1'
```

## Get a stream of resources which have presence monitoring enabled
```
curl localhost:8085/api/envoys
```

> **Notes**:
>
> httpie will not receive the stream the same way curl does.  Currently unsure why.
>
> The name of this endpoint may change to be more specific to presence monitoring.

## Delete a resource
```
http DELETE localhost:8085/api/tenant/aaaaa/resources/host1
```