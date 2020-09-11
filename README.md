# Raw Spans Grouper

Streaming Job that converts a group of raw spans into traces.

## Description

| ![space-1.jpg](https://hypertrace-docs.s3.amazonaws.com/ingestion-pipeline.png) | 
|:--:| 
| *Hypertrace Ingestion Pipeline* |

`hypertrace-oc-collector` collects spans from tracers like Jaeger, Zipkin and writes them to kafka which will be then converted to `raw-spans` by [span-normalizer](https://github.com/hypertrace/span-normalizer) Job. `raw-spans-grouper` fetches this `raw-spans` from kafka and  creates structured traces out of it based on the trace id and the parent span id in the spans.

## Building locally
Raw spans grouper uses gradlew to compile/install/distribute. Gradle wrapper is already part of the source code. To build Raw spans grouper, run:

```
./gradlew dockerBuildImages
```
## Testing

### Running unit tests
Run `./gradlew test` to execute unit tests. 

### Testing image

You can test the image you built after modification by running docker-compose or helm setup. 

#### docker-compose
Change the tag for `raw-spans-grouper` from `:main` to `:test` in [docker-compose file](https://github.com/hypertrace/hypertrace/blob/main/docker/docker-compose.yml) like this.

```yaml
  raw-spans-grouper:
    image: hypertrace/raw-spans-grouper:test
    container_name: raw-spans-grouper
    ...
```

and then run `docker-compose up` to test the setup.

## Docker Image Source:
- [DockerHub > Raw Spans Grouper](https://hub.docker.com/r/hypertrace/raw-spans-grouper)
