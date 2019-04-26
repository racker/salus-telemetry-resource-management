package com.rackspace.salus.resource_management.converter;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.File;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Scanner;

public class SwaggerJsonConverter {

    public static void main(String[] args) throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        String content = new Scanner(new File(args[0]+"/swagger.json")).useDelimiter("\\Z").next();
        ObjectNode root = (ObjectNode)mapper.readTree(content);
        Map<String, JsonNode> temp = new HashMap();
        for (Iterator<Map.Entry<String, JsonNode>> it = root.get("paths").fields(); it.hasNext(); ) {
            Map.Entry<String, JsonNode> elt = it.next();
            if (elt.getKey().contains("tenant"))
            {
                String newKey = elt.getKey().replace("tenant/{tenantId}/", "");
                temp.put(newKey, elt.getValue());
                /*
                attempting to remove the parameters but is failing on array index out of bounds exception

                elt.getValue().fields().forEachRemaining((webVerbs)->{
                    int i = 0;
                    for (Iterator<Map.Entry<String, JsonNode>> parameters = elt.getValue().get("parameters").fields(); parameters.hasNext(); ) {
                        Map.Entry<String, JsonNode> parameter = parameters.next();
                        if (parameter.getValue().get("name").asText().compareTo("tenantId") == 0) {
                            //parameters is an array
                            ((ArrayNode)temp.get(webVerbs.getKey()).get("parameters")).remove(i);
                            break;
                        }
                        i++;
                    }
                });*/

                it.remove();

            } else {
                it.remove();
            }
        }
        ObjectNode pathNode = mapper.getNodeFactory().objectNode();

        temp.forEach((key, node)->{
            pathNode.set(key, node);
        });
        root.set("paths", pathNode);
        mapper.writeValue(new java.io.File(args[0]+"/convertedOutput.json"), (JsonNode)root);

    }
}
