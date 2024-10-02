package org.example.consumerApi.dao;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.TopicConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ExecutionException;

public class KafkaAdminDAO {
    private static final Logger log = LoggerFactory.getLogger(KafkaAdminDAO.class);

    // create topic
    public static void createTopic(NewTopic newTopic, String cleanupPolicy) throws ExecutionException, InterruptedException {

        log.info("Creating topic "+ newTopic.name());

        Properties propsAdminClient = new Properties();
        propsAdminClient.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConfig.KAFKA_SERVERS);
        try (AdminClient adminClient = AdminClient.create(propsAdminClient)) {

            log.info("Topic List:");
            adminClient.listTopics().names().get().forEach(log::info);

            Map<String, String> newTopicConfig = new HashMap<>();
            newTopicConfig.put(TopicConfig.CLEANUP_POLICY_CONFIG, cleanupPolicy);
            newTopic.configs(newTopicConfig);
            adminClient.createTopics(Collections.singletonList(newTopic));
        }

        log.info("Created topic "+ newTopic.name());
    }

    // for a given topic get a list of topic partition info
    public static List<TopicPartition> getPartitions(String topicName) {

        log.info("Finding partitions for topic "+ topicName);

        Properties propsAdminClient = new Properties();
        propsAdminClient.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConfig.KAFKA_SERVERS);

        try (AdminClient adminClient = AdminClient.create(propsAdminClient)) {

            DescribeTopicsResult describeTopics = adminClient.describeTopics(List.of(topicName));
            KafkaFuture<Map<String, TopicDescription>> future  = describeTopics.allTopicNames();

            Map<String, TopicDescription> map = null;
            try {
                map = future.get();
            } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException(e);
            }
            TopicDescription topicDescription = map.get(topicName);

            List<TopicPartition> partitionsList = new ArrayList<>();
            topicDescription.partitions().forEach(partition -> partitionsList.add(new TopicPartition(topicName, partition.partition())));

            log.info("Found "+ partitionsList.size() +" partitions for topic "+ topicName);

            return partitionsList;
        }
    }

    // true if a topic exists
    public static boolean checkTopicExists(String topicName) {

        log.info("Finding topic "+ topicName);

        Properties propsAdminClient = new Properties();
        propsAdminClient.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConfig.KAFKA_SERVERS);

        try (AdminClient adminClient = AdminClient.create(propsAdminClient)) {

            ListTopicsResult listTopicsResult = adminClient.listTopics();
            KafkaFuture<Set<String>> futureTopicNames = listTopicsResult.names();

            Set<String> set = null;
            try {
                set = futureTopicNames.get();
            } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException(e);
            }

            return set.contains(topicName);
        }
    }
}

