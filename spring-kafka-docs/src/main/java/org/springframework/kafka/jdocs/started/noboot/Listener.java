/*
 * Copyright 2021-present the original author or authors.
 */

package org.springframework.kafka.jdocs.started.noboot;

import org.springframework.kafka.annotation.KafkaListener;

/**
 * Code snippet for quick start.
 *
 * @author Gary Russell
 * @since 2.7
 *
 */
// tag::startedNoBootListener[]
public class Listener {

    @KafkaListener(id = "listen1", topics = "topic1")
    public void listen1(String in) {
        System.out.println(in);
    }

}
// end::startedNoBootListener[]
