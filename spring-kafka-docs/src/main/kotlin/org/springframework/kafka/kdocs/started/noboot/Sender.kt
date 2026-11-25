/*
 * Copyright and licensing information for this source code can be found in
 * the NOTICE and LICENSE files distributed with this project.
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
