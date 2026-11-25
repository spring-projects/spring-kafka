/*
 * Copyright and licensing information for this source code can be found in
 * the NOTICE and LICENSE files distributed with this project.
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
