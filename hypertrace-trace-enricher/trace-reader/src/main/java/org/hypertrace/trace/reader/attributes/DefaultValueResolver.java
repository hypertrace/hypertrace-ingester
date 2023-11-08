package org.hypertrace.trace.reader.attributes;

import static io.reactivex.rxjava3.core.Single.zip;

import com.google.common.util.concurrent.RateLimiter;
import io.reactivex.rxjava3.core.Maybe;
import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.core.Single;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
import org.hypertrace.trace.provider.AttributeProvider;

@Slf4j
class DefaultValueResolver implements ValueResolver {
  // One log a minute
  private static final RateLimiter LOGGING_LIMITER = RateLimiter.create(1 / 60d);
  private final AttributeProvider attributeClient;
  private final AttributeProjectionRegistry attributeProjectionRegistry;

  DefaultValueResolver(
    AttributeProvider attributeClient,
      AttributeProjectionRegistry attributeProjectionRegistry) {
    this.attributeClient = attributeClient;
    this.attributeProjectionRegistry = attributeProjectionRegistry;
  }

  @Override
  public Optional<LiteralValue> resolve(
      ValueSource valueSource, AttributeMetadata attributeMetadata) {
    if (!attributeMetadata.hasDefinition()) {
      if(LOGGING_LIMITER.tryAcquire()) {
          log.error("Attribute definition not set for attribute: " + attributeMetadata.getId());
      }
      return Optional.empty();
    }

    return this.resolveDefinition(
        valueSource, attributeMetadata, attributeMetadata.getDefinition());
  }

  private Optional<LiteralValue> resolveDefinition(
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
        return Optional.empty();
    }
  }

  private Optional<LiteralValue> resolveValue(
      ValueSource contextValueSource,
      String attributeScope,
      AttributeType attributeType,
      AttributeKind attributeKind,
      String path) {
    Optional<ValueSource> matchingValueSource = contextValueSource.sourceForScope(attributeScope);
    if(matchingValueSource.isEmpty()) {
      if(LOGGING_LIMITER.tryAcquire()) {
        log.error("No value source available supporting scope %s", attributeScope);
      }
      return Optional.empty();
    }
    switch (attributeType) {
      case ATTRIBUTE:
        return matchingValueSource
            .flatMap(valueSource -> valueSource.getAttribute(path, attributeKind));
      case METRIC:
        return matchingValueSource
            .flatMap(valueSource -> valueSource.getMetric(path, attributeKind));
      case UNRECOGNIZED:
      case TYPE_UNDEFINED:
      default:
        return Optional.empty();
    }
  }

  private Optional<LiteralValue> resolveProjection(ValueSource valueSource, Projection projection) {
    switch (projection.getValueCase()) {
      case ATTRIBUTE_ID:
        return this.attributeClient.getById(valueSource.tenantId(), projection.getAttributeId())
            .flatMap(attributeMetadata -> this.resolve(valueSource, attributeMetadata));
      case LITERAL:
        return Optional.of(projection.getLiteral());
      case EXPRESSION:
        return this.resolveExpression(valueSource, projection.getExpression());
      case VALUE_NOT_SET:
      default:
        if(LOGGING_LIMITER.tryAcquire()) {
          log.error("Unrecognized projection type");
        }
        return Optional.empty();
    }
  }

  private Optional<LiteralValue> resolveField(
      ValueSource valueSource, SourceField sourceField, AttributeKind attributeKind) {
    return valueSource.getSourceField(sourceField, attributeKind);
  }

  private Optional<LiteralValue> resolveFirstValuePresent(
      ValueSource valueSource,
      AttributeMetadata attributeMetadata,
      AttributeDefinitions definitions) {

    return definitions.getDefinitionsList().stream()
        .map(
            definition -> this.resolveDefinition(valueSource, attributeMetadata, definition))
      .filter(Optional::isPresent)
      .findFirst()
      .flatMap(Function.identity());
  }

  private Optional<LiteralValue> resolveExpression(
      ValueSource valueSource, ProjectionExpression expression) {
    Optional<AttributeProjection> maybeProjection = this.attributeProjectionRegistry.getProjection(expression.getOperator());
    if(maybeProjection.isEmpty()) {
      if(LOGGING_LIMITER.tryAcquire()) {
        log.error("Unregistered projection operator: {}", expression.getOperator());
      }
      return Optional.empty();
    }
    Optional<List<LiteralValue>> maybeArguments =
        this.resolveArgumentList(valueSource, expression.getArgumentsList());

    return maybeArguments.map(arguments -> maybeProjection.get().project(arguments));
  }

  private Optional<List<LiteralValue>> resolveArgumentList(
      ValueSource valueSource, List<Projection> arguments) {
     Stream<Optional<LiteralValue>> resolvedArguments = arguments.stream().map(argument -> this.resolveProjection(valueSource, argument));
     try {
       return Optional.of(resolvedArguments.map(Optional::orElseThrow).collect(Collectors.toUnmodifiableList()));
     } catch (NoSuchElementException elementException) {
       // if any of the arguments don't resolve, fail argument resolution
       return Optional.empty();
     }
  }
}
