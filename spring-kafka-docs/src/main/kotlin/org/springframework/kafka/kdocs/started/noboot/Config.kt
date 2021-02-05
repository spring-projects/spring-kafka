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
package org.springframework.kafka.kdocs.started.noboot

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.IntegerDeserializer
import org.apache.kafka.common.serialization.IntegerSerializer
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.annotation.EnableKafka
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory
import org.springframework.kafka.core.*

/**
 * Code snippet for quick start.
 *
 * @author Gary Russell
 * @author Artem Bilan
 * @since 2.7
 */
// tag::startedNoBootConfig[]
@Configuration
@EnableKafka
class Config {

    @Bean
    fun kafkaListenerContainerFactory(consumerFactory: ConsumerFactory<Int, String>) =
        ConcurrentKafkaListenerContainerFactory<Int, String>().also { it.consumerFactory = consumerFactory }


    @Bean
    fun consumerFactory() = DefaultKafkaConsumerFactory<Int, String>(consumerProps)

    val consumerProps = mapOf(
        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to "localhost:9092",
        ConsumerConfig.GROUP_ID_CONFIG to "group",
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to IntegerDeserializer::class.java,
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java,
        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG to "earliest"
    )

    @Bean
    fun sender(template: KafkaTemplate<Int, String>) = Sender(template)

    @Bean
    fun listener() = Listener()

    @Bean
    fun producerFactory() = DefaultKafkaProducerFactory<Int, String>(senderProps)

    val senderProps = mapOf(
        ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to "localhost:9092",
        ProducerConfig.LINGER_MS_CONFIG to 10,
        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG to IntegerSerializer::class.java,
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java
    )

    @Bean
    fun kafkaTemplate(producerFactory: ProducerFactory<Int, String>) = KafkaTemplate(producerFactory)

}
// end::startedNoBootConfig[]
