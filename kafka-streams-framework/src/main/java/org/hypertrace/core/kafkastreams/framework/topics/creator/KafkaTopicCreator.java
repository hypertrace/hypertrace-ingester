package org.hypertrace.core.kafkastreams.framework.topics.creator;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.errors.TopicExistsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Helper class for creating required topics before starting streaming application
 */
public class KafkaTopicCreator {
  private static final Logger LOGGER = LoggerFactory.getLogger(KafkaTopicCreator.class);


  private static final int numPartitions = 1;
  private static final short numReplications = 1;

  public static void createTopics(String bootstrapServers, List<String> topics) {
    LOGGER.info("Creating topics : {}", topics);

    Properties adminProps = new Properties();
    adminProps.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    adminProps.put(CommonClientConfigs.REQUEST_TIMEOUT_MS_CONFIG, 5000);

    AdminClient client = AdminClient.create(adminProps);

    List<NewTopic> newTopics = topics.stream().
            map(topic -> new NewTopic(topic, numPartitions, numReplications))
            .collect(Collectors.toList());

    CreateTopicsResult result = client.createTopics(newTopics);

    try {
      result.values().forEach((topic, response) -> {
        try {
          response.get();
        } catch (InterruptedException | ExecutionException e) {
          if (!(e.getCause() instanceof TopicExistsException)) {
            throw new IllegalStateException(e);
          }
          LOGGER.info("Topic already exists : {}", topic);
        }
      });
      LOGGER.info("Successfully created all topics : {}", topics);
    } finally {
      client.close();
    }
  }

}
