/*
 * Copyright 2026-present the original author or authors.
 */

package com.example.sample09;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.ShareAcknowledgment;
import org.springframework.stereotype.Component;

/**
 * Share consumer listener running in {@code ShareAckMode.MANUAL}, where every record is
 * delivered with a {@link ShareAcknowledgment} that the listener must terminate exactly once.
 * The record key selects the acknowledgment type to demonstrate: {@code ACCEPT} completes the
 * record, {@code RELEASE} returns it to the share group for another delivery attempt and
 * {@code REJECT} archives it without redelivery.
 *
 * @author Omar Morales Ortega
 */
@Component
public class Sample09ShareListener {

	private static final Log LOG = LogFactory.getLog(Sample09ShareListener.class);

	@KafkaListener(topics = "${sample09.topic}", groupId = "${sample09.group}",
			containerFactory = "manualShareKafkaListenerContainerFactory")
	public void process(ConsumerRecord<String, String> record, ShareAcknowledgment acknowledgment) {
		String key = record.key();
		short deliveryCount = record.deliveryCount().orElse((short) 1);

		if ("accept".equals(key)) {
			LOG.info("ACCEPT - " + record.value() + " [deliveryCount=" + deliveryCount + "]");
			acknowledgment.acknowledge();
		}
		else if ("release".equals(key)) {
			if (deliveryCount == 1) {
				LOG.info("RELEASE - " + record.value() + " [deliveryCount=" + deliveryCount
						+ "] - returning it to the share group");
				acknowledgment.release();
			}
			else {
				LOG.info("ACCEPT (redelivered) - " + record.value() + " [deliveryCount=" + deliveryCount + "]");
				acknowledgment.acknowledge();
			}
		}
		else if ("reject".equals(key)) {
			LOG.info("REJECT - " + record.value() + " [deliveryCount=" + deliveryCount + "] - archiving it");
			acknowledgment.reject();
		}
		else {
			LOG.info("ACCEPT (unknown key) - " + record.value() + " [deliveryCount=" + deliveryCount + "]");
			acknowledgment.acknowledge();
		}
	}

}
