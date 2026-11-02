/*
 * Copyright 2021-present the original author or authors.
 */
package org.springframework.kafka.kdocs.started.noboot

import org.springframework.kafka.core.KafkaTemplate

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

}
// end::startedNoBootSender[]
