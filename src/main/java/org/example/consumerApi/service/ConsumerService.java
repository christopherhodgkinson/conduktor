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
    private final KafkaAdminDAO kafkaAdminDAO;


    public ConsumerService(KafkaConsumerDAO kafkaConsumerDAO, KafkaAdminDAO kafkaAdminDAO){
        this.kafkaConsumerDAO = kafkaConsumerDAO;
        this.kafkaAdminDAO = kafkaAdminDAO;
    }

    public List<ConsumerRecord<String, String>> retrieveRecords(String topic, Integer offset, Integer amount) {

        if(!kafkaAdminDAO.checkTopicExists(topic)){
            log.error("Topic does not exist: "+ topic);
            throw new UnknownTopicOrPartitionException("Topic does not exist: "+ topic);
        }
        log.info("Topic does exist: "+ topic);

        // assign partitions to consumer
        List<TopicPartition> partitionList = kafkaAdminDAO.getPartitions(topic);
        kafkaConsumerDAO.assignPartitions(partitionList);

        // exception when offset is larger than what is available for at least one partition
        if(kafkaConsumerDAO.isOffsetGreaterThanMax(partitionList, offset)){
            log.error("Offset Greater than max - Offset "+ offset );
            throw new IllegalArgumentException("Offset provided "+offset+" greater than max for "+ topic);
        }

        // set the offset for all partitions
        kafkaConsumerDAO.setOffsetForPartitions(partitionList, offset);

        List<ConsumerRecord<String, String>> output = new ArrayList<>();
        // get records
        consumeTopic(output, topic, amount);

        int actualAmount = Math.min(amount, output.size());

        log.info("Number of records expected: "+ amount);
        log.info("Number of records output: "+ actualAmount);

        return output.subList(0, actualAmount);
    }

    // consume for a given topic and amount
    // output to given list
    public void consumeTopic(List<ConsumerRecord<String, String>> output, String topic, int amount){

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
        log.info("Polling FINISH");
        log.info("Number of records consumed: "+ totalCount);

    }


}
