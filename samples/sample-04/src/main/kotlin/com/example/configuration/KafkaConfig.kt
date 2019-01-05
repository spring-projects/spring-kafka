/*
 * Copyright 2019 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.example.configuration

import com.fasterxml.jackson.databind.ObjectMapper
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.autoconfigure.kafka.KafkaProperties
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.http.converter.json.Jackson2ObjectMapperBuilder
import org.springframework.kafka.core.ConsumerFactory
import org.springframework.kafka.core.DefaultKafkaConsumerFactory
import org.springframework.kafka.core.DefaultKafkaProducerFactory
import org.springframework.kafka.core.ProducerFactory
import org.springframework.kafka.support.serializer.JsonDeserializer
import org.springframework.kafka.support.serializer.JsonSerializer

@Configuration
class KafkaConfig {
    @Autowired
    lateinit var jackson2ObjectMapperBuilder: Jackson2ObjectMapperBuilder
    @Autowired
    lateinit var kafkaProperties: KafkaProperties


    @Bean
    fun defaultKafkaConsumerFactory(): ConsumerFactory<Any, Any> {
        val objectMapper = jackson2ObjectMapperBuilder.build() as ObjectMapper
        val jsonDeserializer = JsonDeserializer<Any>(objectMapper)
        jsonDeserializer.configure(kafkaProperties.buildConsumerProperties(), false)
        val kafkaConsumerFactory = DefaultKafkaConsumerFactory<Any, Any>(
                kafkaProperties.buildConsumerProperties(),
                jsonDeserializer,
                jsonDeserializer
        )
        kafkaConsumerFactory.setValueDeserializer(jsonDeserializer)
        return kafkaConsumerFactory
    }

    @Bean
    fun defaultKafkaProducerFactory(): ProducerFactory<Any, Any> {
        val jsonSerializer = JsonSerializer<Any>(jackson2ObjectMapperBuilder.build())
        jsonSerializer.configure(kafkaProperties.buildProducerProperties(), false)
        val factory = DefaultKafkaProducerFactory<Any, Any>(
                kafkaProperties.buildProducerProperties(),
                jsonSerializer,
                jsonSerializer
        )
        val transactionIdPrefix = kafkaProperties.producer
                .transactionIdPrefix
        if (transactionIdPrefix != null) {
            factory.setTransactionIdPrefix(transactionIdPrefix)
        }
        return factory
    }
}
