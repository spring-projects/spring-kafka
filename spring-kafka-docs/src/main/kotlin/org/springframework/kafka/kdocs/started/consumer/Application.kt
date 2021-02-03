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
package org.springframework.kafka.kdocs.started.consumer

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.apache.kafka.clients.admin.NewTopic
import org.springframework.kafka.config.TopicBuilder
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.boot.ApplicationRunner
import org.springframework.boot.ApplicationArguments
import kotlin.jvm.JvmStatic
import org.springframework.boot.SpringApplication
import org.springframework.context.annotation.Bean

/**
 * Code snippet for quick start.
 *
 * @author Gary Russell
 * @since 2.7
 */
// tag::startedConsumer[]
@SpringBootApplication
class Application {

    @Bean
    fun topic(): NewTopic {
        return TopicBuilder.name("topic1")
                .partitions(10)
                .replicas(1)
                .build()
    }

    @KafkaListener(id = "myId", topics = ["topic1"])
    fun listen(`in`: String?) {
        println(`in`)
    }

    @Bean
    fun runner(template: KafkaTemplate<String?, String?>): ApplicationRunner {
        return ApplicationRunner { _: ApplicationArguments? -> template.send("topic1", "test") }
    }

    companion object {
        @JvmStatic
        fun main(args: Array<String>) {
            SpringApplication.run(Application::class.java, *args)
        }
    }

}
// end::startedConsumer[]
