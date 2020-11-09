# Hypertrace View Generator

###### org.hypertrace.viewgenerator

[![CircleCI](https://circleci.com/gh/hypertrace/hypertrace-view-generator.svg?style=svg)](https://circleci.com/gh/hypertrace/hypertrace-view-generator)

This repository contains Hypertrace view creator bootstrap job and Hypertrace view generation streaming job.

## Description

| ![space-1.jpg](https://hypertrace-docs.s3.amazonaws.com/ingestion-pipeline.png) | 
|:--:| 
| *Hypertrace Ingestion Pipeline* |

Hypertrace view creator is a bootstrap job that runs once and creates required views in pinot -  `spanEventView`, `serviceCallView`, `backendEntityView`, `rawServiceView`, `rawTraceView`.

Once these views are there in pinot, Hypertrace view generator consumes(from kafka) enriched traces produced by [Hypertrace trace enricher](https://github.com/hypertrace/hypertrace-trace-enricher), applies required filters, transformations and then materializes them into pinot views. 


## Building locally
`hypertrace-view-generator` uses gradlew to compile/install/distribute. Gradle wrapper is already part of the source code. To build `hypertrace-view-generator`, run:

```
./gradlew dockerBuildImages
```

## Testing

### Running unit tests
Run `./gradlew test` to execute unit tests. 


### Testing image

You can test the image you built after modification by running docker-compose or helm setup. 

#### docker-compose
Change the tag for `hypertrace-view-generator ` from `:main` to `:test` in [docker-compose file](https://github.com/hypertrace/hypertrace/blob/main/docker/docker-compose.yml) like this.

```yaml
  hypertrace-view-generator:
    image: hypertrace/hypertrace-view-generator:test
    container_name: hypertrace-view-generator
    ...
```

and then run `docker-compose up` to test the setup.

## Docker Image Source:
- [DockerHub > Hypertrace view generator](https://hub.docker.com/r/hypertrace/hypertrace-view-generator)
