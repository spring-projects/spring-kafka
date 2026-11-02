/*
 * Copyright 2021-present the original author or authors.
 */

package org.springframework.kafka.jdocs.topics;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.TopicConfig;

import org.springframework.context.annotation.Bean;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaAdmin.NewTopics;

/**
 * Snippet for Configuring Topics section.
 *
 * @author Gary Russell
 * @since 2.7
 *
 */
public class Config {

    // tag::topicBeans[]
    @Bean
    public KafkaAdmin admin() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        return new KafkaAdmin(configs);
    }

    @Bean
    public NewTopic topic1() {
        return TopicBuilder.name("thing1")
                .partitions(10)
                .replicas(3)
                .compact()
                .build();
    }

    @Bean
    public NewTopic topic2() {
        return TopicBuilder.name("thing2")
                .partitions(10)
                .replicas(3)
                .config(TopicConfig.COMPRESSION_TYPE_CONFIG, "zstd")
                .build();
    }

    @Bean
    public NewTopic topic3() {
        return TopicBuilder.name("thing3")
                .assignReplicas(0, List.of(0, 1))
                .assignReplicas(1, List.of(1, 2))
                .assignReplicas(2, List.of(2, 0))
                .config(TopicConfig.COMPRESSION_TYPE_CONFIG, "zstd")
                .build();
    }
    // end::topicBeans[]
    // tag::brokerProps[]
    @Bean
    public NewTopic topic4() {
        return TopicBuilder.name("defaultBoth")
                .build();
    }

    @Bean
    public NewTopic topic5() {
        return TopicBuilder.name("defaultPart")
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic topic6() {
        return TopicBuilder.name("defaultRepl")
                .partitions(3)
                .build();
    }
    // end::brokerProps[]
    // tag::newTopicsBean[]
    @Bean
    public KafkaAdmin.NewTopics topics456() {
        return new NewTopics(
                TopicBuilder.name("defaultBoth")
                    .build(),
                TopicBuilder.name("defaultPart")
                    .replicas(1)
                    .build(),
                TopicBuilder.name("defaultRepl")
                    .partitions(3)
                    .build());
    }
    // end::newTopicsBean[]

}
