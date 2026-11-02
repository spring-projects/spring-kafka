/*
 * Copyright 2021-present the original author or authors.
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
