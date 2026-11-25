/*
 * Copyright and licensing information for this source code can be found in
 * the NOTICE and LICENSE files distributed with this project.
 */
package org.springframework.kafka.kdocs.started.noboot

import org.springframework.kafka.annotation.KafkaListener

/**
 * Code snippet for quick start.
 *
 * @author Gary Russell
 * @since 2.7
 */
// tag::startedNoBootListener[]
class Listener {

    @KafkaListener(id = "listen1", topics = ["topic1"])
    fun listen1(`in`: String) {
        println(`in`)
    }

}
// end::startedNoBootListener[]
