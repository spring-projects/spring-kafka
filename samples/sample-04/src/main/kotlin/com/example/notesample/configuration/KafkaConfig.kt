package com.example.notesample.configuration

import com.fasterxml.jackson.databind.ObjectMapper
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.autoconfigure.kafka.KafkaProperties
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.http.converter.json.Jackson2ObjectMapperBuilder
import org.springframework.kafka.core.*
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
