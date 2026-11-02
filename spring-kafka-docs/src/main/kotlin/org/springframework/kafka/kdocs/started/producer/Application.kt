/*
 * Copyright 2021-present the original author or authors.
 */

package org.springframework.kafka.kdocs.started.producer

import org.apache.kafka.clients.admin.NewTopic
import org.springframework.boot.ApplicationRunner
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.context.annotation.Bean
import org.springframework.kafka.core.KafkaTemplate

/**
 * Code snippet for quick start.
 *
 * @author Gary Russell
 * @author Artem Bilan
 * @since 2.7
 */
// tag::startedProducer[]
@SpringBootApplication
class Application {

    @Bean
    fun topic() = NewTopic("topic1", 10, 1)

    @Bean
    fun runner(template: KafkaTemplate<String, String>) =
        ApplicationRunner { template.send("topic1", "test") }
    
    companion object {
        @JvmStatic
        fun main(args: Array<String>) = runApplication<Application>(*args)
    }

}
// end::startedProducer[]
