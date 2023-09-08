package org.hypertrace.trace.reader.attributes;

import static io.reactivex.rxjava3.core.Single.zip;

import com.google.common.util.concurrent.RateLimiter;
import io.reactivex.rxjava3.core.Maybe;
import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.core.Single;
import java.util.List;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.attribute.service.cachingclient.CachingAttributeClient;
import org.hypertrace.core.attribute.service.projection.AttributeProjection;
import org.hypertrace.core.attribute.service.projection.AttributeProjectionRegistry;
import org.hypertrace.core.attribute.service.v1.AttributeDefinition;
import org.hypertrace.core.attribute.service.v1.AttributeDefinition.AttributeDefinitions;
import org.hypertrace.core.attribute.service.v1.AttributeDefinition.SourceField;
import org.hypertrace.core.attribute.service.v1.AttributeKind;
import org.hypertrace.core.attribute.service.v1.AttributeMetadata;
import org.hypertrace.core.attribute.service.v1.AttributeType;
import org.hypertrace.core.attribute.service.v1.LiteralValue;
import org.hypertrace.core.attribute.service.v1.LiteralValue.ValueCase;
import org.hypertrace.core.attribute.service.v1.Projection;
import org.hypertrace.core.attribute.service.v1.ProjectionExpression;

@Slf4j
class DefaultValueResolver implements ValueResolver {
  // One log a minute
  private static final RateLimiter LOGGING_LIMITER = RateLimiter.create(1 / 60d);
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
      return this.buildAndLogErrorLazily(
          "Attribute definition not set for attribute: " + attributeMetadata.getDisplayName());
    }

    return this.resolveDefinition(
        valueSource, attributeMetadata, attributeMetadata.getDefinition());
  }

  private Single<LiteralValue> resolveDefinition(
      ValueSource valueSource,
      AttributeMetadata attributeMetadata,
      AttributeDefinition definition) {

    switch (definition.getValueCase()) {
      case SOURCE_PATH:
        return this.resolveValue(
            valueSource,
            attributeMetadata.getScopeString(),
            attributeMetadata.getType(),
            attributeMetadata.getValueKind(),
            definition.getSourcePath());
      case PROJECTION:
        return this.resolveProjection(valueSource, definition.getProjection());
      case SOURCE_FIELD:
        return this.resolveField(
            valueSource, definition.getSourceField(), attributeMetadata.getValueKind());
      case FIRST_VALUE_PRESENT:
        return this.resolveFirstValuePresent(
            valueSource, attributeMetadata, definition.getFirstValuePresent());
      case VALUE_NOT_SET:
      default:
        return this.buildAndLogErrorLazily("Unrecognized attribute definition");
    }
  }

  private Maybe<LiteralValue> maybeResolveDefinition(
      ValueSource valueSource,
      AttributeMetadata attributeMetadata,
      AttributeDefinition definition) {
    return this.resolveDefinition(valueSource, attributeMetadata, definition)
        .filter(literalValue -> !literalValue.getValueCase().equals(ValueCase.VALUE_NOT_SET))
        .onErrorComplete();
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
                this.buildAndLogErrorLazily(
                    "No value source available supporting scope %s", attributeScope));
    switch (attributeType) {
      case ATTRIBUTE:
        return matchingValueSource
            .mapOptional(valueSource -> valueSource.getAttribute(path, attributeKind))
            .defaultIfEmpty(LiteralValue.getDefaultInstance());
      case METRIC:
        return matchingValueSource
            .mapOptional(valueSource -> valueSource.getMetric(path, attributeKind))
            .defaultIfEmpty(LiteralValue.getDefaultInstance());
      case UNRECOGNIZED:
      case TYPE_UNDEFINED:
      default:
        return this.buildAndLogErrorLazily("Unrecognized projection type");
    }
  }

  private Single<LiteralValue> resolveProjection(ValueSource valueSource, Projection projection) {
    switch (projection.getValueCase()) {
      case ATTRIBUTE_ID:
        return valueSource
            .executionContext()
            .wrapSingle(() -> this.attributeClient.get(projection.getAttributeId()))
            .flatMap(attributeMetadata -> this.resolve(valueSource, attributeMetadata));
      case LITERAL:
        return Single.just(projection.getLiteral());
      case EXPRESSION:
        return this.resolveExpression(valueSource, projection.getExpression());
      case VALUE_NOT_SET:
      default:
        return this.buildAndLogErrorLazily("Unrecognized projection type");
    }
  }

  private Single<LiteralValue> resolveField(
      ValueSource valueSource, SourceField sourceField, AttributeKind attributeKind) {
    return Maybe.fromOptional(valueSource.getSourceField(sourceField, attributeKind))
        .defaultIfEmpty(LiteralValue.getDefaultInstance());
  }

  private Single<LiteralValue> resolveFirstValuePresent(
      ValueSource valueSource,
      AttributeMetadata attributeMetadata,
      AttributeDefinitions definitions) {

    return Observable.fromIterable(definitions.getDefinitionsList())
        .concatMapMaybe(
            definition -> this.maybeResolveDefinition(valueSource, attributeMetadata, definition))
        .first(LiteralValue.getDefaultInstance());
  }

  private Single<LiteralValue> resolveExpression(
      ValueSource valueSource, ProjectionExpression expression) {

    Single<AttributeProjection> projectionSingle =
        Maybe.fromOptional(this.attributeProjectionRegistry.getProjection(expression.getOperator()))
            .switchIfEmpty(
                buildAndLogErrorLazily(
                    "Unregistered projection operator: %s", expression.getOperator()));
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

  private <T> Single<T> buildAndLogErrorLazily(String message, Object... args) {
    return Single.error(
        () -> {
          if (LOGGING_LIMITER.tryAcquire()) {
            log.error(String.format(message, args));
          }
          return new UnsupportedOperationException(String.format(message, args));
        });
  }
}
