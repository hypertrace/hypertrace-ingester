package org.hypertrace.semantic.convention.utils.http;

import org.hypertrace.core.datamodel.AttributeValue;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.semantic.convention.utils.rpc.RpcSemanticConventionUtils;

import java.util.Map;

public class GrpcMigration {

    public static int getGrpcStatusCode(Event event) {

        Map<String, AttributeValue> attributeValueMap = event.getAttributes().getAttributeMap();
        for(String statuscode : RpcSemanticConventionUtils.getAttributeKeysForGrpcStatusCode()){
            if(attributeValueMap.get(statuscode)!=null){
                return Integer.parseInt(attributeValueMap.get(statuscode).getValue());

            }
        }
        return -1;
    }


}