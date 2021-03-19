/*
 * Copyright 2021 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.kafka.kdocs.topics

import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.common.config.TopicConfig
import org.springframework.context.annotation.Bean
import org.springframework.kafka.config.TopicBuilder
import org.springframework.kafka.core.KafkaAdmin
import java.util.*

/**
 * Snippet for Configuring Topics section.
 *
 * @author Gary Russell
 * @author Artem Bilan
 * @since 2.7
 */
class Config {

    // tag::topicBeans[]
    @Bean
    fun admin() = KafkaAdmin(mapOf(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG to "localhost:9092"))

    @Bean
    fun topic1() =
        TopicBuilder.name("thing1")
            .partitions(10)
            .replicas(3)
            .compact()
            .build()

    @Bean
    fun topic2() =
        TopicBuilder.name("thing2")
            .partitions(10)
            .replicas(3)
            .config(TopicConfig.COMPRESSION_TYPE_CONFIG, "zstd")
            .build()

    @Bean
    fun topic3() =
        TopicBuilder.name("thing3")
            .assignReplicas(0, Arrays.asList(0, 1))
            .assignReplicas(1, Arrays.asList(1, 2))
            .assignReplicas(2, Arrays.asList(2, 0))
            .config(TopicConfig.COMPRESSION_TYPE_CONFIG, "zstd")
            .build()

    // end::topicBeans[]
    // tag::brokerProps[]
    @Bean
    fun topic4() = TopicBuilder.name("defaultBoth").build()

    @Bean
    fun topic5() = TopicBuilder.name("defaultPart").replicas(1).build()

    @Bean
    fun topic6() = TopicBuilder.name("defaultRepl").partitions(3).build()
    // end::brokerProps[]
    // tag::newTopicsBean[]
    @Bean
    fun topics456() = KafkaAdmin.NewTopics(
        TopicBuilder.name("defaultBoth")
            .build(),
        TopicBuilder.name("defaultPart")
            .replicas(1)
            .build(),
        TopicBuilder.name("defaultRepl")
            .partitions(3)
            .build()
    )
    // end::newTopicsBean[]

}