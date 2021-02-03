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
import org.springframework.kafka.core.ConsumerFactory
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory
import org.springframework.kafka.core.DefaultKafkaConsumerFactory
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.core.ProducerFactory
import org.springframework.kafka.core.DefaultKafkaProducerFactory
import java.util.HashMap

/**
 * Code snippet for quick start.
 *
 * @author Gary Russell
 * @since 2.7
 */
// tag::startedNoBootConfig[]
@Configuration
@EnableKafka
class Config {
    @Bean
    fun kafkaListenerContainerFactory(consumerFactory: ConsumerFactory<Int, String>):
            ConcurrentKafkaListenerContainerFactory<Int, String> {

        val factory = ConcurrentKafkaListenerContainerFactory<Int, String>()
        factory.consumerFactory = consumerFactory
        return factory
    }

    @Bean
    fun consumerFactory(): ConsumerFactory<Int, String> {
        return DefaultKafkaConsumerFactory(consumerProps())
    }

    private fun consumerProps(): Map<String, Any> {
        val props: MutableMap<String, Any> = HashMap()
        props[ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG] = "localhost:9092"
        props[ConsumerConfig.GROUP_ID_CONFIG] = "group"
        props[ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG] = IntegerDeserializer::class.java
        props[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::class.java
        props[ConsumerConfig.AUTO_OFFSET_RESET_CONFIG] = "earliest"
        // ...
        return props
    }

    @Bean
    fun sender(template: KafkaTemplate<Int, String>): Sender {
        return Sender(template)
    }

    @Bean
    fun listener(): Listener {
        return Listener()
    }

    @Bean
    fun producerFactory(): ProducerFactory<Int, String> {
        return DefaultKafkaProducerFactory(senderProps())
    }

    private fun senderProps(): Map<String, Any> {
        val props: MutableMap<String, Any> = HashMap()
        props[ProducerConfig.BOOTSTRAP_SERVERS_CONFIG] = "localhost:9092"
        props[ProducerConfig.LINGER_MS_CONFIG] = 10
        props[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] = IntegerSerializer::class.java
        props[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.java
        //...
        return props
    }

    @Bean
    fun kafkaTemplate(producerFactory: ProducerFactory<Int, String>): KafkaTemplate<Int, String> {
        return KafkaTemplate(producerFactory)
    }

}
// end::startedNoBootConfig[]
