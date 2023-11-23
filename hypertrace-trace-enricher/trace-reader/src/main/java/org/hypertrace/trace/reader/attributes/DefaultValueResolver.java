package org.hypertrace.trace.reader.attributes;

import com.google.common.util.concurrent.RateLimiter;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.attribute.service.client.AttributeServiceCachedClient;
import org.hypertrace.core.attribute.service.projection.AttributeProjection;
import org.hypertrace.core.attribute.service.projection.AttributeProjectionRegistry;
import org.hypertrace.core.attribute.service.v1.AttributeDefinition;
import org.hypertrace.core.attribute.service.v1.AttributeDefinition.AttributeDefinitions;
import org.hypertrace.core.attribute.service.v1.AttributeDefinition.SourceField;
import org.hypertrace.core.attribute.service.v1.AttributeKind;
import org.hypertrace.core.attribute.service.v1.AttributeMetadata;
import org.hypertrace.core.attribute.service.v1.AttributeType;
import org.hypertrace.core.attribute.service.v1.LiteralValue;
import org.hypertrace.core.attribute.service.v1.Projection;
import org.hypertrace.core.attribute.service.v1.ProjectionExpression;

@Slf4j
class DefaultValueResolver implements ValueResolver {
  // One log a minute
  private static final RateLimiter LOGGING_LIMITER = RateLimiter.create(1 / 60d);
  private final AttributeServiceCachedClient attributeClient;
  private final AttributeProjectionRegistry attributeProjectionRegistry;

  DefaultValueResolver(
      AttributeServiceCachedClient attributeClient,
      AttributeProjectionRegistry attributeProjectionRegistry) {
    this.attributeClient = attributeClient;
    this.attributeProjectionRegistry = attributeProjectionRegistry;
  }

  @Override
  public Optional<LiteralValue> resolve(
      ValueSource valueSource, AttributeMetadata attributeMetadata) {
    if (!attributeMetadata.hasDefinition()) {
      logError("Attribute definition not set for attribute: {}", attributeMetadata.getId());
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
    if (matchingValueSource.isEmpty()) {
      logError("No value source available supporting scope {}", attributeScope);
      return Optional.empty();
    }
    switch (attributeType) {
      case ATTRIBUTE:
        return matchingValueSource.flatMap(
            valueSource -> valueSource.getAttribute(path, attributeKind));
      case METRIC:
        return matchingValueSource.flatMap(
            valueSource -> valueSource.getMetric(path, attributeKind));
      case UNRECOGNIZED:
      case TYPE_UNDEFINED:
      default:
        return Optional.empty();
    }
  }

  private Optional<LiteralValue> resolveProjection(ValueSource valueSource, Projection projection) {
    switch (projection.getValueCase()) {
      case ATTRIBUTE_ID:
        return this.attributeClient
            .getById(valueSource.requestContext(), projection.getAttributeId())
            .flatMap(attributeMetadata -> this.resolve(valueSource, attributeMetadata));
      case LITERAL:
        return Optional.of(projection.getLiteral());
      case EXPRESSION:
        return this.resolveExpression(valueSource, projection.getExpression());
      case VALUE_NOT_SET:
      default:
        logError("Unrecognized projection type");
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
        .map(definition -> this.resolveDefinition(valueSource, attributeMetadata, definition))
        .flatMap(Optional::stream)
        .findFirst();
  }

  private Optional<LiteralValue> resolveExpression(
      ValueSource valueSource, ProjectionExpression expression) {
    Optional<AttributeProjection> maybeProjection =
        this.attributeProjectionRegistry.getProjection(expression.getOperator());
    if (maybeProjection.isEmpty()) {
      logError("Unregistered projection operator: {}", expression.getOperator());
      return Optional.empty();
    }
    List<LiteralValue> argumentList =
        this.resolveArgumentList(valueSource, expression.getArgumentsList());

    if (argumentList.isEmpty()) {
      logError("Failed to resolve argument list for expression with operator: {}", expression);
      return Optional.empty();
    }
    return maybeProjection.map(projection -> projection.project(argumentList));
  }

  private void logError(String format, Object... args) {
    if (LOGGING_LIMITER.tryAcquire()) {
      log.error(format, args);
    }
  }

  private List<LiteralValue> resolveArgumentList(
      ValueSource valueSource, List<Projection> arguments) {
    List<LiteralValue> resolvedArguments =
        arguments.stream()
            .map(argument -> this.resolveProjection(valueSource, argument))
            .flatMap(Optional::stream)
            .collect(Collectors.toUnmodifiableList());
    if (resolvedArguments.size() != arguments.size()) {
      // if any of the arguments don't resolve, return empty list to don't support partial
      // resolution
      return Collections.emptyList();
    }
    return resolvedArguments;
  }
}
