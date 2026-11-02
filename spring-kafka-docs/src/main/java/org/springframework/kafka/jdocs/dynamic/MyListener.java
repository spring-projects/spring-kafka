/*
 * Copyright 2022-present the original author or authors.
 */

package org.springframework.kafka.jdocs.dynamic;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import org.springframework.kafka.listener.MessageListener;

/**
 * {@link MessageListener} for dynamic containers.
 * @author Gary Russell
 * @since 2.8.9
 *
 */
//tag::listener[]

public class MyListener implements MessageListener<String, String> {

    @Override
    public void onMessage(ConsumerRecord<String, String> data) {
        // ...
    }

}
// end::listener[]
