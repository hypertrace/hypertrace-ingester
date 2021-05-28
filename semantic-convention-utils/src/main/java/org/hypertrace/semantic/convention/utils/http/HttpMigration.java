package org.hypertrace.semantic.convention.utils.http;

import org.hypertrace.core.datamodel.AttributeValue;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.semantic.convention.constants.http.OTelHttpSemanticConventions;
import org.hypertrace.core.span.constants.RawSpanConstants;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import static org.hypertrace.core.span.constants.v1.Http.*;

public class HttpMigration{

    private static final List<String> USER_AGENT_ATTRIBUTES =
            List.of(
                    RawSpanConstants.getValue(HTTP_USER_DOT_AGENT),
                    RawSpanConstants.getValue(HTTP_USER_AGENT_WITH_UNDERSCORE),
                    RawSpanConstants.getValue(HTTP_USER_AGENT_WITH_DASH),
                    RawSpanConstants.getValue(HTTP_USER_AGENT_REQUEST_HEADER),
                    RawSpanConstants.getValue(HTTP_USER_AGENT),
                    OTelHttpSemanticConventions.HTTP_USER_AGENT.getValue());

    public static Optional<String> getHttpUserAgent(Event event) {
        Map<String, AttributeValue> attributeValueMap = event.getAttributes().getAttributeMap();
        for(String useragent: USER_AGENT_ATTRIBUTES){
            if(attributeValueMap.get(useragent) !=null){
                return Optional.of(attributeValueMap.get(useragent).getValue());
            }
        }
        return Optional.empty();
    }
}