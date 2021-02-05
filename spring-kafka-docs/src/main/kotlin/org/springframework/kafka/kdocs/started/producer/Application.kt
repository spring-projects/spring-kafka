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
    fun runner(template: KafkaTemplate<String?, String?>) =
        ApplicationRunner { template.send("topic1", "test") }

    fun main(args: Array<String>) = runApplication<Application>(*args)

}
// end::startedProducer[]
