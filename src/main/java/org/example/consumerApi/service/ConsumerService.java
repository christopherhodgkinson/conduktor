package org.example.consumerApi.service;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.example.consumerApi.dao.KafkaAdminDAO;
import org.example.consumerApi.dao.KafkaConsumerDAO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class ConsumerService {

    private static final Logger log = LoggerFactory.getLogger(ConsumerService.class);
    private final KafkaConsumerDAO kafkaConsumerDAO;

    public ConsumerService(KafkaConsumerDAO kafkaConsumerDAO){
        this.kafkaConsumerDAO = kafkaConsumerDAO;
    }

    public List<ConsumerRecord<String, String>> retrieveRecords(String topic, Integer offset, Integer amount) {

        if(!KafkaAdminDAO.checkTopicExists(topic)){
            log.error("Topic does not exist: "+ topic);
            throw new UnknownTopicOrPartitionException("Topic does not exist: "+ topic);
        }
        log.info("Topic does exist: "+ topic);


        // assign partitions to consumer
        List<TopicPartition> partitionList = KafkaAdminDAO.getPartitions(topic);
        kafkaConsumerDAO.assignPartitions(partitionList);

        if(kafkaConsumerDAO.isOffsetGreaterThanMax(partitionList, offset)){
            log.error("Offset Greater than max - Offset "+ offset );
            throw new IllegalArgumentException("Offset provided "+offset+" greater than max for "+ topic);
        }

        // set the offset for all partitions
        kafkaConsumerDAO.setOffsetForPartitions(partitionList, offset);

        List<ConsumerRecord<String, String>> output = new ArrayList<>();

        int totalCount = 0;
        int maxRetries = 3;
        int retriesCount = 0;


        log.info("Polling START");
        // consume while it has less than amount to return or there are no messages left
        while (totalCount < amount && retriesCount < maxRetries){

            log.debug("Total Count:" + totalCount + " Retries:" + retriesCount + " Max Retries:" + maxRetries);

            // consume messages
            ConsumerRecords<String, String> consumerRecords = kafkaConsumerDAO.poll();
            if (consumerRecords.count() == 0){
                retriesCount++;
            }
            else{
                retriesCount = 0;
                totalCount += consumerRecords.count();
                consumerRecords.records(topic).forEach(output::add);
            }
        }

        if(retriesCount >= maxRetries){
            log.info("Max retries reached: " + maxRetries);
        }

        int actualAmount = Math.min(amount, output.size());

        log.info("Polling FINISH");
        log.info("Number of records consumed: "+ totalCount);
        log.info("Number of records expected: "+ amount);
        log.info("Number of records output: "+ actualAmount);

        return output.subList(0, actualAmount);

    }


}
