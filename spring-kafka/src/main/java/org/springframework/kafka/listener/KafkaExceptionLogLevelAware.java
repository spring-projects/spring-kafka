/*
 * Copyright 2020-present the original author or authors.
 */

package org.springframework.kafka.listener;

import org.springframework.kafka.KafkaException;
import org.springframework.util.Assert;

/**
 * A top level abstract class for classes that can be configured with a
 * {@link KafkaException.Level}.
 *
 * @author Gary Russell
 * @since 2.5
 *
 */
public abstract class KafkaExceptionLogLevelAware {

	private KafkaException.Level logLevel = KafkaException.Level.ERROR;

	/**
	 * Set the level at which the exception thrown by this handler is logged.
	 * @param logLevel the level (default ERROR).
	 */
	public void setLogLevel(KafkaException.Level logLevel) {
		Assert.notNull(logLevel, "'logLevel' cannot be null");
		this.logLevel = logLevel;
	}

	/**
	 * Set the level at which the exception thrown by this handler is logged.
	 * @return the level.
	 */
	protected KafkaException.Level getLogLevel() {
		return this.logLevel;
	}

}
