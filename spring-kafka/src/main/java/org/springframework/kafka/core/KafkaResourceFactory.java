/*
 * Copyright 2020-present the original author or authors.
 */

package org.springframework.kafka.core;

import java.util.Map;
import java.util.function.Supplier;

import org.apache.kafka.clients.CommonClientConfigs;
import org.jspecify.annotations.Nullable;

/**
 * Base class for consumer/producer/admin creators.
 *
 * @author Gary Russell
 * @since 2.5
 *
 */
public abstract class KafkaResourceFactory {

	private @Nullable Supplier<String> bootstrapServersSupplier;

	@Nullable
	protected String getBootstrapServers() {
		return this.bootstrapServersSupplier == null ? null : this.bootstrapServersSupplier.get();
	}

	/**
	 * Set a supplier for the bootstrap server list to override any configured in a
	 * subclass.
	 * @param bootstrapServersSupplier the supplier.
	 */
	public void setBootstrapServersSupplier(Supplier<String> bootstrapServersSupplier) {
		this.bootstrapServersSupplier = bootstrapServersSupplier;
	}

	/**
	 * Enhance the properties by calling the
	 * {@link #setBootstrapServersSupplier(Supplier)} and replace the bootstrap servers
	 * properties.
	 * @param configs the configs.
	 */
	protected void checkBootstrap(Map<String, Object> configs) {
		String bootstrapServers = getBootstrapServers();
		if (bootstrapServers != null) {
			configs.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		}
	}

}
