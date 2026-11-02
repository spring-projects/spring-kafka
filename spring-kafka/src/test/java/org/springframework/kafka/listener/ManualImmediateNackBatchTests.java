/*
 * Copyright 2021-present the original author or authors.
 */

package org.springframework.kafka.listener;

import org.springframework.kafka.listener.ContainerProperties.AckMode;

/**
 * @author Gary Russell
 * @since 2.8
 *
 */
public class ManualImmediateNackBatchTests extends ManualNackBatchTests {

	static {
		ackMode = AckMode.MANUAL_IMMEDIATE;
	}

}
