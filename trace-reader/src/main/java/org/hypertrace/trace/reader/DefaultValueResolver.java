package org.hypertrace.trace.reader;

import static io.reactivex.rxjava3.core.Single.zip;

import io.reactivex.rxjava3.core.Maybe;
import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.core.Single;
import java.util.List;
import java.util.stream.Collectors;
import org.hypertrace.core.attribute.service.cachingclient.CachingAttributeClient;
import org.hypertrace.core.attribute.service.projection.AttributeProjection;
import org.hypertrace.core.attribute.service.projection.AttributeProjectionRegistry;
import org.hypertrace.core.attribute.service.v1.AttributeKind;
import org.hypertrace.core.attribute.service.v1.AttributeMetadata;
import org.hypertrace.core.attribute.service.v1.AttributeType;
import org.hypertrace.core.attribute.service.v1.LiteralValue;
import org.hypertrace.core.attribute.service.v1.Projection;
import org.hypertrace.core.attribute.service.v1.ProjectionExpression;

class DefaultValueResolver implements ValueResolver {
  private final CachingAttributeClient attributeClient;
  private final AttributeProjectionRegistry attributeProjectionRegistry;

  DefaultValueResolver(
      CachingAttributeClient attributeClient,
      AttributeProjectionRegistry attributeProjectionRegistry) {
    this.attributeClient = attributeClient;
    this.attributeProjectionRegistry = attributeProjectionRegistry;
  }

  @Override
  public Single<LiteralValue> resolve(
      ValueSource valueSource, AttributeMetadata attributeMetadata) {
    if (!attributeMetadata.hasDefinition()) {
      return this.buildError("Attribute definition not set");
    }

    switch (attributeMetadata.getDefinition().getValueCase()) {
      case SOURCE_PATH:
        return this.resolveValue(
            valueSource,
            attributeMetadata.getScopeString(),
            attributeMetadata.getType(),
            attributeMetadata.getValueKind(),
            attributeMetadata.getDefinition().getSourcePath());
      case PROJECTION:
        return this.resolveProjection(
            valueSource, attributeMetadata.getDefinition().getProjection());
      case VALUE_NOT_SET:
      default:
        return this.buildError("Unrecognized attribute definition");
    }
  }

  private Single<LiteralValue> resolveValue(
      ValueSource contextValueSource,
      String attributeScope,
      AttributeType attributeType,
      AttributeKind attributeKind,
      String path) {
    Single<ValueSource> matchingValueSource =
        Maybe.fromOptional(contextValueSource.sourceForScope(attributeScope))
            .switchIfEmpty(
                this.buildError("No value source available supporting scope %s", attributeScope));
    switch (attributeType) {
      case ATTRIBUTE:
        return matchingValueSource
            .mapOptional(valueSource -> valueSource.getAttribute(path, attributeKind))
            .switchIfEmpty(
                this.buildError(
                    "Unable to extract attribute path %s with type %s", path, attributeKind));
      case METRIC:
        return matchingValueSource
            .mapOptional(valueSource -> valueSource.getMetric(path, attributeKind))
            .switchIfEmpty(
                this.buildError(
                    "Unable to extract metric path %s with type %s", path, attributeKind));
      case UNRECOGNIZED:
      case TYPE_UNDEFINED:
      default:
        return this.buildError("Unrecognized projection type");
    }
  }

  private Single<LiteralValue> resolveProjection(ValueSource valueSource, Projection projection) {
    switch (projection.getValueCase()) {
      case ATTRIBUTE_ID:
        return this.attributeClient
            .get(projection.getAttributeId())
            .flatMap(attributeMetadata -> this.resolve(valueSource, attributeMetadata));
      case LITERAL:
        return Single.just(projection.getLiteral());
      case EXPRESSION:
        return this.resolveExpression(valueSource, projection.getExpression());
      case VALUE_NOT_SET:
      default:
        return this.buildError("Unrecognized projection type");
    }
  }

  private Single<LiteralValue> resolveExpression(
      ValueSource valueSource, ProjectionExpression expression) {

    Single<AttributeProjection> projectionSingle =
        Maybe.fromOptional(this.attributeProjectionRegistry.getProjection(expression.getOperator()))
            .switchIfEmpty(
                buildError("Unregistered projection operator: %s", expression.getOperator()));
    Single<List<LiteralValue>> argumentsSingle =
        this.resolveArgumentList(valueSource, expression.getArgumentsList());

    return zip(projectionSingle, argumentsSingle, AttributeProjection::project);
  }

  private Single<List<LiteralValue>> resolveArgumentList(
      ValueSource valueSource, List<Projection> arguments) {
    return Observable.fromIterable(arguments)
        .flatMapSingle(argument -> this.resolveProjection(valueSource, argument))
        .collect(Collectors.toList());
  }

  private <T> Single<T> buildError(String message, Object... args) {
    return Single.error(new UnsupportedOperationException(String.format(message, args)));
  }
}
