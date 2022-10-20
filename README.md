# Hypertrace Ingester
 Hypertrace ingester is comprised of 4 streaming jobs on kafka
- Span-normalizer
- Raw Spans Grouper
- Hypertrace Trace Enricher
- Hypertrace View Generator

## Description
| ![space-1.jpg](https://imagizer.imageshack.com/v2/xq90/923/UZhzhg.png) | 
|:--:| 
| *Hypertrace Ingester* |

Working of Hypertrace starts with developers instrumenting their applications with tracing libraries (For ex., Zipkin, Jaeger). So the Hypertrace pipeline starts with Hypertrace OC Collector which is implementation based on OpenCensus Service. The Hypertrace OC Collector is a component that runs “nearby” (e.g. in the same VPC, AZ, etc.) a user’s application components and receives trace spans and metrics emitted by the tasks instrumented with Tracing libraries. 

Hypertrace OC Collector writes spans from this processes/services to Kafka and then will be processed further by Hypertrace ingestion pipeline. Different tracers can have different span formats and as we support tracers like zipkin and jaeger the spans coming via [hypertrace-oc-collector](https://github.com/hypertrace/opencensus-service) to kafka can have different fields in them. [span-normalizer](https://github.com/hypertrace/span-normalizer) reads spans from kafka and adds more first class fields to span object like http url, http method, http status code, grpc method, grpc status message etc. so that platform downstream can access the values from the span. We call this normalized span `raw-span` which will be further processed by [raw-spans-grouper](https://github.com/hypertrace/raw-spans-grouper). You can find the first class fields [here](https://github.com/hypertrace/data-model/tree/main/data-model/src/main/avro/eventfields).

[raw-spans-grouper](https://github.com/hypertrace/raw-spans-grouper) fetches this `raw-spans` from kafka and creates structured traces out of it based on the trace id and the parent span id in the spans. So from this point onward we have traces and not spans!

So suppose we have two spans with `traceID:1234` then we will group them into a single trace. 

Trace-enrichers are being used to enrich traces with entity information. [hypertrace-trace-enricher](https://github.com/hypertrace/hypertrace-trace-enricher) service talks to entity-service which fetches entity information from Mongo as required. For example, Let's say we got a span which has http method related attribute method: `/api/v1/user?name`. So, in this case, if we already have Endpoint entity which refers to `/api/v1/user`, we fetch the id of that entity and add it to span. Now, span will have one more attribute like this method:`/api/v1/user?name, api_id:1234`. 

In parallel we have `Hypertrace view creator` running which is a bootstrap job that runs once and creates required views in pinot - `spanEventView`, `serviceCallView`, `backendEntityView`, `rawServiceView`, `rawTraceView`.

Once these views are there in pinot, [hypertrace-view-generator](ttps://github.com/hypertrace/hypertrace-view-generator) consumes enriched traces produced by Hypertrace trace enricher from kafka, applies required filters, transformations and then materializes them into pinot views.

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
Change the tag for `hypertrace-ingester` from `:main` to `:test` in [docker-compose file](https://github.com/hypertrace/hypertrace/blob/main/docker/docker-compose.yml) like this.

```yaml
  hypertrace-ingester:
    image: hypertrace/hypertrace-ingester:test
    container_name: hypertrace-ingester
    ...
```

and then run `docker-compose up` to test the setup.

## Docker Image Source:
- [DockerHub > Hypertrace Ingester](https://hub.docker.com/r/hypertrace/hypertrace-ingester)
