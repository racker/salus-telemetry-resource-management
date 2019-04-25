package com.rackspace.salus.resource_management;


import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.commons.lang3.StringUtils;
import org.codehaus.jackson.node.ArrayNode;

import java.io.File;
import java.util.Iterator;
import java.util.Map;
import java.util.Scanner;

public class swaggerJsonConverter {



    public static void main(String[] args) throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        String content = new Scanner(new File("/Users/adam9500/workspace/salus-telemetry-bundle/apps/resource-management/target/generated/swagger/swagger.json")).useDelimiter("\\Z").next();
        ObjectNode root = (ObjectNode)mapper.readTree(content);

        for (Iterator<Map.Entry<String, JsonNode>> it = root.get("paths").fields(); it.hasNext(); ) {
            Map.Entry<String, JsonNode> elt = it.next();
            if (elt.getKey().contains("tenant"))
            {
                ((ObjectNode)root.get("paths")).set(
                        elt.getKey().replace("tenant/{tenantId}/", ""),
                        elt.getValue());

                ((ObjectNode) root.get("paths")).remove(elt.getKey());
            } else {
                ((ObjectNode) root.get("paths")).remove(elt.getKey());
            }
        }

        mapper.writeValue(new java.io.File("convertedOutput.json"), (JsonNode)root);

    }
}
