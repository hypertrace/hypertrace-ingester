package org.hypertrace.traceenricher.enrichment.enrichers.space;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import io.reactivex.rxjava3.core.Maybe;
import java.io.IOException;
import java.util.List;
import org.hypertrace.spaces.config.service.v1.GetRulesResponse;
import org.hypertrace.spaces.config.service.v1.SpaceConfigRule;
import org.hypertrace.spaces.config.service.v1.SpacesConfigServiceGrpc.SpacesConfigServiceImplBase;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SpaceRulesCachingClientTest {

  @Mock SpacesConfigServiceImplBase mockSpaceConfigService;

  SpaceRulesCachingClient spaceRulesCachingClient;

  Server grpcServer;
  ManagedChannel grpcChannel;

  @BeforeEach
  void beforeEach() throws IOException {
    String uniqueName = InProcessServerBuilder.generateName();
    this.grpcServer =
        InProcessServerBuilder.forName(uniqueName)
            .directExecutor() // directExecutor is fine for unit tests
            .addService(mockSpaceConfigService)
            .build()
            .start();
    this.grpcChannel = InProcessChannelBuilder.forName(uniqueName).directExecutor().build();
    this.spaceRulesCachingClient = new SpaceRulesCachingClient(this.grpcChannel);
  }

  @AfterEach
  void afterEach() {
    this.grpcServer.shutdownNow();
    this.grpcChannel.shutdownNow();
  }

  @Test
  void hitsCacheOnSecondRequest() {
    List<SpaceConfigRule> rules1 = List.of(SpaceConfigRule.newBuilder().setId("1").build());
    List<SpaceConfigRule> rules2 = List.of(SpaceConfigRule.newBuilder().setId("2").build());
    mockRuleResponse(Maybe.just(rules1));
    assertEquals(rules1, this.spaceRulesCachingClient.getRulesForTenant("tenant-id"));

    mockRuleResponse(Maybe.just(rules2));
    assertEquals(rules2, this.spaceRulesCachingClient.getRulesForTenant("tenant-id-2"));

    verify(this.mockSpaceConfigService, times(2)).getRules(any(), any());
    verifyNoMoreInteractions(this.mockSpaceConfigService);

    assertEquals(rules1, this.spaceRulesCachingClient.getRulesForTenant("tenant-id"));
    assertEquals(rules2, this.spaceRulesCachingClient.getRulesForTenant("tenant-id-2"));
  }

  @Test
  void retriesOnError() {
    mockRuleResponse(Maybe.error(new IllegalArgumentException()));
    assertEquals(List.of(), this.spaceRulesCachingClient.getRulesForTenant("tenant-id"));

    List<SpaceConfigRule> rules = List.of(SpaceConfigRule.getDefaultInstance());
    mockRuleResponse(Maybe.just(rules));
    assertEquals(rules, this.spaceRulesCachingClient.getRulesForTenant("tenant-id"));
  }

  void mockRuleResponse(Maybe<List<SpaceConfigRule>> configResponse) {
    doAnswer(
            invocation -> {
              StreamObserver<GetRulesResponse> observer =
                  invocation.getArgument(1, StreamObserver.class);

              configResponse.blockingSubscribe(
                  rules -> {
                    observer.onNext(GetRulesResponse.newBuilder().addAllRules(rules).build());
                    observer.onCompleted();
                  },
                  observer::onError);
              return null;
            })
        .when(this.mockSpaceConfigService)
        .getRules(any(), any());
  }
}
