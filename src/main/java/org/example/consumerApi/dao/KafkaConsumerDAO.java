package org.example.consumerApi.dao;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;

public class KafkaConsumerDAO {
    private static final Logger log = LoggerFactory.getLogger(KafkaConsumerDAO.class);


    private final KafkaConsumer<String, String> kafkaConsumer;

    public KafkaConsumerDAO(KafkaConsumer<String, String> consumer){
        this.kafkaConsumer = consumer;
    }

    // assign partitions to consumer
    public void assignPartitions(List<TopicPartition> partitionList) {
        kafkaConsumer.assign(partitionList);
    }

    // true if offset is greater than any partition provided
    public boolean isOffsetGreaterThanMax( List<TopicPartition> topicPartitionList, Integer offset) {
        Map<TopicPartition, Long> endOffsets = kafkaConsumer.endOffsets(topicPartitionList);

        log.info("Max Offsets: ");
        endOffsets.forEach((key, value) -> log.info(key.topic() + "-" + key.partition() + " " + value));

        return endOffsets.values().stream().anyMatch(maxOffset -> offset > maxOffset );
    }

    // set offset for each partition
    public void setOffsetForPartitions(List<TopicPartition> partitionList, Integer offset) {
        for (TopicPartition topicPartition : partitionList) {
            kafkaConsumer.seek(topicPartition, offset);
        }
    }

    // poll for messages
    public ConsumerRecords<String, String> poll() {
        return kafkaConsumer.poll(Duration.of(KafkaConfig.CONSUMER_POLL_MS, ChronoUnit.MILLIS));
    }

    public static KafkaConsumer<String, String> consumerFactory(){
        return new KafkaConsumer<>(KafkaConfig.getConsumerConfig());
    }
}
