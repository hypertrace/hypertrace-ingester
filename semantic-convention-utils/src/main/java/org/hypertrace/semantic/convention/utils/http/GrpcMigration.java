package org.hypertrace.semantic.convention.utils.http;

import org.hypertrace.core.datamodel.AttributeValue;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.span.constants.RawSpanConstants;
import org.hypertrace.semantic.convention.utils.rpc.RpcSemanticConventionUtils;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.hypertrace.core.span.constants.v1.CensusResponse.CENSUS_RESPONSE_STATUS_MESSAGE;
import static org.hypertrace.core.span.constants.v1.Envoy.ENVOY_GRPC_STATUS_MESSAGE;
import static org.hypertrace.core.span.constants.v1.Grpc.GRPC_ERROR_MESSAGE;
import static org.hypertrace.core.span.normalizer.constants.RpcSpanTag.RPC_REQUEST_METADATA_AUTHORITY;
import static org.hypertrace.core.span.normalizer.constants.RpcSpanTag.RPC_REQUEST_METADATA_USER_AGENT;

public class GrpcMigration {

    private static final List<String> Status_Msg_Att =
            List.of(
                    RawSpanConstants.getValue(CENSUS_RESPONSE_STATUS_MESSAGE),
                    RawSpanConstants.getValue(ENVOY_GRPC_STATUS_MESSAGE));

    public static int getGrpcStatusCode(Event event) {
        Map<String, AttributeValue> attributeValueMap = event.getAttributes().getAttributeMap();
        for(String statuscode : RpcSemanticConventionUtils.getAttributeKeysForGrpcStatusCode()){
            if(attributeValueMap.get(statuscode)!=null){
                return Integer.parseInt(attributeValueMap.get(statuscode).getValue());
            }
        }
        return -1;
    }

    public static String getGrpcStatusMsg(Event event) {
        Map<String, AttributeValue> attributeValueMap = event.getAttributes().getAttributeMap();
        for(String statusmsg : Status_Msg_Att) {
            if(attributeValueMap.get(statusmsg)!=null) {
                return attributeValueMap.get(statusmsg).getValue();
            }
        }
        return "";
    }

    public static String getGrpcErrorMsg(Event event) {
        Map<String, AttributeValue> attributeValueMap = event.getAttributes().getAttributeMap();
        if(attributeValueMap.get(RawSpanConstants.getValue(GRPC_ERROR_MESSAGE))!=null) {
            return attributeValueMap.get(RawSpanConstants.getValue(GRPC_ERROR_MESSAGE)).getValue();
        }
        return "";
    }

    public static Optional<String> getGrpcUserAgent(Event event) {
        if(event.getAttributes()==null || event.getAttributes().getAttributeMap()==null) {
            return Optional.empty();
        };
        Map<String, AttributeValue> attributeValueMap = event.getAttributes().getAttributeMap();
        if(attributeValueMap.get(RPC_REQUEST_METADATA_USER_AGENT.getValue())!=null) {
            return Optional.of(RPC_REQUEST_METADATA_USER_AGENT.getValue());
        }
        return Optional.empty();
    }

    public static Optional<String> getGrpcAuthority(Event event) {
        if(event.getAttributes()==null || event.getAttributes().getAttributeMap()==null) {
            return Optional.empty();
        };
        Map<String, AttributeValue> attributeValueMap = event.getAttributes().getAttributeMap();
        if(attributeValueMap.get(RPC_REQUEST_METADATA_AUTHORITY.getValue())!=null) {
            return Optional.of(RPC_REQUEST_METADATA_AUTHORITY.getValue());
        }
        return Optional.empty();
    }


}