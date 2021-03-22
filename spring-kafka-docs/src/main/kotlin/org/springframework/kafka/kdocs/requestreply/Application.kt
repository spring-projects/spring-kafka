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
package org.springframework.kafka.kdocs.requestreply

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.kafka.core.KafkaAdmin.NewTopics
import org.springframework.kafka.config.TopicBuilder
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory
import org.springframework.kafka.requestreply.ReplyingKafkaTemplate
import org.springframework.kafka.support.converter.ByteArrayJsonMessageConverter
import org.springframework.boot.ApplicationRunner
import org.springframework.boot.ApplicationArguments
import org.springframework.kafka.requestreply.RequestReplyTypedMessageFuture
import org.springframework.messaging.support.MessageBuilder
import org.springframework.core.ParameterizedTypeReference
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.messaging.handler.annotation.SendTo
import kotlin.jvm.JvmStatic
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.slf4j.LoggerFactory
import org.springframework.boot.SpringApplication
import org.springframework.context.annotation.Bean
import org.springframework.kafka.core.ProducerFactory
import java.util.concurrent.TimeUnit
import java.util.function.Consumer

/**
 * Code snippets for request/reply messaging.
 *
 * @author Gary Russell
 * @since 2.7
 */
@SpringBootApplication
class Application {

    private val log = LoggerFactory.getLogger(Application::class.java)

    @Bean
    fun topics(): NewTopics {
        return NewTopics(
            TopicBuilder.name("requests")
                .partitions(10)
                .replicas(1)
                .build(),
            TopicBuilder.name("replies")
                .partitions(10)
                .replicas(1)
                .build()
        )
    }

    @Bean
    fun kafkaTemplate(pf: ProducerFactory<Any, Any>?) = KafkaTemplate(pf)

// tag::beans[]
    @Bean
    fun template(
        pf: ProducerFactory<String?, String>?,
        factory: ConcurrentKafkaListenerContainerFactory<String?, String?>
    ): ReplyingKafkaTemplate<String?, String, String?> {
        val replyContainer = factory.createContainer("replies")
        replyContainer.containerProperties.groupId = "request.replies"
        val template = ReplyingKafkaTemplate(pf, replyContainer)
        template.messageConverter = ByteArrayJsonMessageConverter()
        template.defaultTopic = "requests"
        return template
    }
// end::beans[]

    @Bean
    fun runner(template: ReplyingKafkaTemplate<String?, String?, String?>): ApplicationRunner {
        return ApplicationRunner { _ ->
// tag::sendReceive[]
            val future1: RequestReplyTypedMessageFuture<String?, String?, Thing?>? =
                template.sendAndReceive(MessageBuilder.withPayload("getAThing").build(),
                    object : ParameterizedTypeReference<Thing?>() {})
            log.info(future1?.sendFuture?.get(10, TimeUnit.SECONDS)?.recordMetadata?.toString())
            val thing = future1?.get(10, TimeUnit.SECONDS)?.payload
            log.info(thing.toString())

            val future2: RequestReplyTypedMessageFuture<String?, String?, List<Thing?>?>? =
                template.sendAndReceive(MessageBuilder.withPayload("getThings").build(),
                    object : ParameterizedTypeReference<List<Thing?>?>() {})
            log.info(future2?.sendFuture?.get(10, TimeUnit.SECONDS)?.recordMetadata.toString())
            val things = future2?.get(10, TimeUnit.SECONDS)?.payload
            things?.forEach(Consumer { thing1: Thing? -> log.info(thing1.toString()) })
// end::sendReceive[]
        }
    }

    @KafkaListener(id = "myId", topics = ["requests"], properties = ["auto.offset.reset:earliest"])
    @SendTo
    fun listen(`in`: String): ByteArray {
        log.info(`in`)
        if (`in` == "\"getAThing\"") {
            return "{\"thingProp\":\"someValue\"}".toByteArray()
        }
        return if (`in` == "\"getThings\"") {
            "[{\"thingProp\":\"someValue1\"},{\"thingProp\":\"someValue2\"}]".toByteArray()
        } else `in`.toUpperCase().toByteArray()
    }

    class Thing {
        var thingProp: String? = null
        override fun toString(): String {
            return "Thing [thingProp=" + thingProp + "]"
        }
    }

}

fun main(args: Array<String>) {
    System.setProperty("spring.kafka.producer.value-serializer", ByteArraySerializer::class.java.name)
    System.setProperty("spring.kafka.consumer.value-deserializer", ByteArrayDeserializer::class.java.name)
    SpringApplication.run(Application::class.java, *args).close()
}
