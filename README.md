# Span Normalizer

Converts the incoming spans from jaeger or any other format to a raw span format which is understood by the rest of the Hypertrace platform.

## How do we use Span normalizer?

| ![space-1.jpg](https://hypertrace-docs.s3.amazonaws.com/ingestion-pipeline.png) | 
|:--:| 
| *Hypertrace Ingestion Pipeline* |

Different tracers can have different span formats and as we support tracers like zipkin and jaeger the spans coming via `hypertrace-oc-collector` to kafka can have different fields in them. `span-normalizer` reads spans from kafka and adds more first class fields to span object like http url, http method, http status code, grpc method, grpc status message etc. so that platform downstream can access the values from the span. We call this normalized span `raw-span` which will be further processed by [raw-spans-grouper.](https://github.com/hypertrace/raw-spans-grouper)

You can find first class fields [here.](https://github.com/hypertrace/data-model/tree/main/data-model/src/main/avro/eventfields)

## Building locally
The Span normalizer uses gradlew to compile/install/distribute. Gradle wrapper is already part of the source code. To build Span normalizer, run:

```
./gradlew dockerBuildImages
```

## Docker Image Source:
- [DockerHub > Span normalizer](https://hub.docker.com/r/hypertrace/span-normalizer)
