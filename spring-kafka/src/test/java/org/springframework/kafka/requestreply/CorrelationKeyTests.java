/*
 * Copyright 2022-present the original author or authors.
 */

package org.springframework.kafka.requestreply;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Gary Russell
 * @since 2.8.10
 *
 */
public class CorrelationKeyTests {

	@Test
	void asString() {
		CorrelationKey key = new CorrelationKey(new byte[16]);
		assertThat(key.toString()).isEqualTo("00000000-0000-0000-0000-000000000000");
		key = new CorrelationKey(new byte[10]);
		assertThat(key.toString()).isEqualTo("00000000000000000000");
	}

}
