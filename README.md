# Raw Spans Grouper

Streaming Job that converts a group of raw spans into traces.

## How do we use Raw spans grouper?

| ![space-1.jpg](https://hypertrace-docs.s3.amazonaws.com/ingestion-pipeline.png) | 
|:--:| 
| *Hypertrace Ingestion Pipeline* |

`hypertrace-oc-collector` collects spans from tracers like Jaeger, Zipkin and writes them to kafka which will be then converted to `raw-spans` by [span-normalizer](https://github.com/hypertrace/span-normalizer) Job. `raw-spans-grouper` fetches this `raw-spans` from kafka and  creates structured traces out of it based on the trace id and the parent span id in the spans.

## Building locally
Raw spans grouper uses gradlew to compile/install/distribute. Gradle wrapper is already part of the source code. To build Raw spans grouper, run:

```
./gradlew dockerBuildImages
```

## Docker Image Source:
- [DockerHub > Raw Spans Grouper](https://hub.docker.com/r/hypertrace/raw-spans-grouper)
