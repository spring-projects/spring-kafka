/*
 * Copyright 2018-present the original author or authors.
 */

package org.springframework.kafka.retrytopic;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;

/**
 * @author tomazlemos
 * @since 14/02/21
 */
public abstract class TestClockUtils {

	public static Clock CLOCK = Clock.fixed(Instant.ofEpochMilli(325587600000L), ZoneId.systemDefault());

}
