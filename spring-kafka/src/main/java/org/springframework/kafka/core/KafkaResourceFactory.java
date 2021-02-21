/*
 * Copyright 2020 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.kafka.core;

import java.util.Map;
import java.util.function.Supplier;

import org.apache.kafka.clients.CommonClientConfigs;

import org.springframework.lang.Nullable;

/**
 * Base class for consumer/producer/admin creators.
 *
 * @author Gary Russell
 * @since 2.5
 *
 */
public abstract class KafkaResourceFactory {

	private Supplier<String> bootstrapServersSupplier;

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
