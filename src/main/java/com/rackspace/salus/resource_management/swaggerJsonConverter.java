package com.rackspace.salus.resource_management;


import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.File;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Scanner;

public class swaggerJsonConverter {



    public static void main(String[] args) throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        String content = new Scanner(new File("/Users/adam9500/workspace/salus-telemetry-bundle/apps/resource-management/target/generated/swagger/swagger.json")).useDelimiter("\\Z").next();
        ObjectNode root = (ObjectNode)mapper.readTree(content);
        Map<String, JsonNode> temp = new HashMap();
        for (Iterator<Map.Entry<String, JsonNode>> it = root.get("paths").fields(); it.hasNext(); ) {
            Map.Entry<String, JsonNode> elt = it.next();
            if (elt.getKey().contains("tenant"))
            {
                String newKey = elt.getKey().replace("tenant/{tenantId}/", "");
                temp.put(newKey, elt.getValue());

                it.remove();

            } else {
                it.remove();
            }
        }
        ObjectNode pathNode = mapper.getNodeFactory().objectNode();

        temp.forEach((key, node)->{
            pathNode.set(key, node);
            //for some reason this works but placing it in the path doesnt...

           });
        root.set("paths", pathNode);
        mapper.writeValue(new java.io.File("/Users/adam9500/workspace/salus-telemetry-bundle/apps/resource-management/target/generated/swagger/convertedOutput.json"), (JsonNode)root);

    }
}
