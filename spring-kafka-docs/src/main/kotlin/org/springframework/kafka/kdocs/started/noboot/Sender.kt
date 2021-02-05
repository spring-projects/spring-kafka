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

import org.springframework.boot.runApplication
import org.springframework.kafka.core.KafkaTemplate
import kotlin.jvm.JvmStatic
import org.springframework.context.annotation.AnnotationConfigApplicationContext
import org.springframework.kafka.kdocs.started.producer.Application

/**
 * Code snippet for quick start.
 *
 * @author Gary Russell
 * @author Artem Bilan
 * @since 2.7
 */
// tag::startedNoBootSender[]
class Sender(private val template: KafkaTemplate<Int, String>) {

    fun send(toSend: String, key: Int) {
        template.send("topic1", key, toSend)
    }

    fun main(args: Array<String>) {
        val context = runApplication<Sender>(*args)
        context.getBean(Sender::class.java).send("test", 42)
    }

}
// end::startedNoBootSender[]
