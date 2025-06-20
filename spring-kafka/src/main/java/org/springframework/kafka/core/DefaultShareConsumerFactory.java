/*
 * Copyright 2025-present the original author or authors.
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

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

import org.apache.kafka.clients.consumer.KafkaShareConsumer;
import org.apache.kafka.clients.consumer.ShareConsumer;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.serialization.Deserializer;
import org.jspecify.annotations.Nullable;

import org.springframework.beans.factory.BeanNameAware;
import org.springframework.util.Assert;

/**
 * The {@link ShareConsumerFactory} implementation to produce new {@link ShareConsumer} instances
 * for provided {@link Map} {@code configs} and optional {@link Deserializer}s on each
 * {@link #createShareConsumer(String, String)} invocation.
 * <p>
 * If you are using {@link Deserializer}s that have no-arg constructors and require no setup, then simplest to
 * specify {@link Deserializer} classes in the configs passed to the
 * {@link DefaultShareConsumerFactory} constructor.
 * <p>
 * If that is not possible, but you are using {@link Deserializer}s that may be shared between all {@link ShareConsumer}
 * instances (and specifically that their close() method is a no-op), then you can pass in {@link Deserializer}
 * instances for one or both of the key and value deserializers.
 * <p>
 * If neither of the above is true then you may provide a {@link Supplier} for one or both {@link Deserializer}s
 * which will be used to obtain {@link Deserializer}(s) each time a {@link ShareConsumer} is created by the factory.
 *
 * @param <K> the key type.
 * @param <V> the value type.
 *
 * @author Soby Chacko
 * @since 4.0
 */
public class DefaultShareConsumerFactory<K, V> extends KafkaResourceFactory
		implements ShareConsumerFactory<K, V>, BeanNameAware {

	private final Map<String, Object> configs;

	private @Nullable Supplier<@Nullable Deserializer<K>> keyDeserializerSupplier;

	private @Nullable Supplier<@Nullable Deserializer<V>> valueDeserializerSupplier;

	private boolean configureDeserializers = true;

	private final List<Listener<K, V>> listeners = new ArrayList<>();

	private String beanName = "not.managed.by.Spring";

	/**
	 * Construct a factory with the provided configuration.
	 * @param configs the configuration.
	 */
	public DefaultShareConsumerFactory(Map<String, Object> configs) {
		this(configs, null, null);
	}

	/**
	 * Construct a factory with the provided configuration and deserializer suppliers.
	 * When the suppliers are invoked to get an instance, the deserializers'
	 * {@code configure()} methods will be called with the configuration map.
	 * @param configs the configuration.
	 * @param keyDeserializerSupplier the key {@link Deserializer} supplier function (nullable).
	 * @param valueDeserializerSupplier the value {@link Deserializer} supplier function (nullable).
	 */
	public DefaultShareConsumerFactory(Map<String, Object> configs,
			@Nullable Supplier<@Nullable Deserializer<K>> keyDeserializerSupplier,
			@Nullable Supplier<@Nullable Deserializer<V>> valueDeserializerSupplier) {
		this(configs, keyDeserializerSupplier, valueDeserializerSupplier, true);
	}

	/**
	 * Construct a factory with the provided configuration and deserializers.
	 * The deserializers' {@code configure()} methods will be called with the
	 * configuration map unless {@code configureDeserializers} is false.
	 * @param configs the configuration.
	 * @param keyDeserializer the key {@link Deserializer}.
	 * @param valueDeserializer the value {@link Deserializer}.
	 * @param configureDeserializers false to not configure the deserializers.
	 */
	public DefaultShareConsumerFactory(Map<String, Object> configs,
			@Nullable Deserializer<K> keyDeserializer,
			@Nullable Deserializer<V> valueDeserializer, boolean configureDeserializers) {
		this(configs, keyDeserializer != null ? () -> keyDeserializer : null,
				valueDeserializer != null ? () -> valueDeserializer : null, configureDeserializers);
	}

	/**
	 * Construct a factory with the provided configuration, deserializer suppliers, and deserializer config flag.
	 * When the suppliers are invoked to get an instance, the deserializers'
	 * {@code configure()} methods will be called with the configuration map unless
	 * {@code configureDeserializers} is false.
	 * @param configs the configuration.
	 * @param keyDeserializerSupplier the key {@link Deserializer} supplier function (nullable).
	 * @param valueDeserializerSupplier the value {@link Deserializer} supplier function (nullable).
	 * @param configureDeserializers whether to configure deserializers.
	 */
	public DefaultShareConsumerFactory(Map<String, Object> configs,
			@Nullable Supplier<@Nullable Deserializer<K>> keyDeserializerSupplier,
			@Nullable Supplier<@Nullable Deserializer<V>> valueDeserializerSupplier,
			boolean configureDeserializers) {
		this.configs = new ConcurrentHashMap<>(configs);
		this.configureDeserializers = configureDeserializers;
		this.keyDeserializerSupplier = keyDeserializerSupplier;
		this.valueDeserializerSupplier = valueDeserializerSupplier;
	}

	/**
	 * Create a share consumer with the provided group id and client id.
	 * @param groupId the group id (maybe null).
	 * @param clientId the client id.
	 * @return the share consumer.
	 */
	@Override
	public ShareConsumer<K, V> createShareConsumer(@Nullable String groupId, @Nullable String clientId) {
		return createRawConsumer(groupId, clientId);
	}

	/**
	 * Actually create the consumer.
	 * @param groupId the group id (maybe null).
	 * @param clientId the client id.
	 * @return the share consumer.
	 */
	protected ShareConsumer<K, V> createRawConsumer(@Nullable String groupId, @Nullable String clientId) {
		Map<String, Object> consumerProperties = new HashMap<>(this.configs);
		if (groupId != null) {
			consumerProperties.put("group.id", groupId);
		}
		if (clientId != null) {
			consumerProperties.put("client.id", clientId);
		}
		return new ExtendedShareConsumer(consumerProperties);
	}

	@Override
	public void setBeanName(String name) {
		this.beanName = name;
	}

	/**
	 * Set the key deserializer. The deserializer will be configured using the consumer
	 * configuration, unless {@link #setConfigureDeserializers(boolean)
	 * configureDeserializers} is false.
	 * @param keyDeserializer the deserializer.
	 */
	public void setKeyDeserializer(@Nullable Deserializer<K> keyDeserializer) {
		this.keyDeserializerSupplier = () -> keyDeserializer;
	}

	/**
	 * Set the value deserializer. The deserializer will be configured using the consumer
	 * configuration, unless {@link #setConfigureDeserializers(boolean)
	 * configureDeserializers} is false.
	 * @param valueDeserializer the value deserializer.
	 */
	public void setValueDeserializer(@Nullable Deserializer<V> valueDeserializer) {
		this.valueDeserializerSupplier = () -> valueDeserializer;
	}

	@Override
	@Nullable
	public Deserializer<K> getKeyDeserializer() {
		return this.keyDeserializerSupplier != null ? this.keyDeserializerSupplier.get() : null;
	}

	@Override
	@Nullable
	public Deserializer<V> getValueDeserializer() {
		return this.valueDeserializerSupplier != null ? this.valueDeserializerSupplier.get() : null;
	}

	/**
	 * Set a supplier to supply instances of the key deserializer. The deserializer will
	 * be configured using the consumer configuration, unless
	 * {@link #setConfigureDeserializers(boolean) configureDeserializers} is false.
	 * @param keyDeserializerSupplier the supplier (nullable).
	 */
	public void setKeyDeserializerSupplier(@Nullable Supplier<@Nullable Deserializer<K>> keyDeserializerSupplier) {
		this.keyDeserializerSupplier = keyDeserializerSupplier;
	}

	/**
	 * Set a supplier to supply instances of the value deserializer. The deserializer will
	 * be configured using the consumer configuration, unless
	 * {@link #setConfigureDeserializers(boolean) configureDeserializers} is false.
	 * @param valueDeserializerSupplier the supplier (nullable).
	 */
	public void setValueDeserializerSupplier(@Nullable Supplier<@Nullable Deserializer<V>> valueDeserializerSupplier) {
		this.valueDeserializerSupplier = valueDeserializerSupplier;
	}

	/**
	 * Set to false (default true) to prevent programmatically provided deserializers (via
	 * constructor or setters) from being configured using the consumer configuration,
	 * e.g. if the deserializers are already fully configured.
	 * @param configureDeserializers false to not configure.
	 * @see #setKeyDeserializer(Deserializer)
	 * @see #setKeyDeserializerSupplier(Supplier)
	 * @see #setValueDeserializer(Deserializer)
	 * @see #setValueDeserializerSupplier(Supplier)
	 **/
	public void setConfigureDeserializers(boolean configureDeserializers) {
		this.configureDeserializers = configureDeserializers;
	}

	/**
	 * Get the current list of listeners.
	 * @return the listeners.
	 */
	@Override
	public List<Listener<K, V>> getListeners() {
		return Collections.unmodifiableList(this.listeners);
	}

	/**
	 * Add a listener.
	 * @param listener the listener.
	 */
	@Override
	public void addListener(Listener<K, V> listener) {
		Assert.notNull(listener, "'listener' cannot be null");
		this.listeners.add(listener);
	}

	/**
	 * Add a listener at a specific index.
	 * <p>
	 * This method allows insertion of a listener at a particular position in the internal listener list.
	 * While this enables ordering of listener callbacks (which can be important for certain monitoring or extension scenarios),
	 * there is intentionally no corresponding {@code removeListener(int index)} contract. Removing listeners by index is
	 * discouraged because the position of a listener can change if others are added or removed, making it easy to
	 * accidentally remove the wrong one. Managing listeners by their reference (object) is safer and less error-prone,
	 * especially as listeners are usually set up once during initialization.
	 * {@see #removeListener(Listener)}
	 * </p>
	 * @param index the index (list position).
	 * @param listener the listener to add.
	 */
	@Override
	public void addListener(int index, Listener<K, V> listener) {
		Assert.notNull(listener, "'listener' cannot be null");
		if (index >= this.listeners.size()) {
			this.listeners.add(listener);
		}
		else {
			this.listeners.add(index, listener);
		}
	}

	/**
	 * Remove a listener.
	 * @param listener the listener.
	 * @return true if removed.
	 */
	@Override
	public boolean removeListener(Listener<K, V> listener) {
		return this.listeners.remove(listener);
	}

	@Nullable
	private Deserializer<K> keyDeserializer(Map<String, Object> configs) {
		Deserializer<K> deserializer =
				this.keyDeserializerSupplier != null
						? this.keyDeserializerSupplier.get()
						: null;
		if (deserializer != null && this.configureDeserializers) {
			deserializer.configure(configs, true);
		}
		return deserializer;
	}

	@Nullable
	private Deserializer<V> valueDeserializer(Map<String, Object> configs) {
		Deserializer<V> deserializer =
				this.valueDeserializerSupplier != null
						? this.valueDeserializerSupplier.get()
						: null;
		if (deserializer != null && this.configureDeserializers) {
			deserializer.configure(configs, false);
		}
		return deserializer;
	}

	@Override
	public Map<String, Object> getConfigurationProperties() {
		return Collections.unmodifiableMap(this.configs);
	}

	protected class ExtendedShareConsumer extends KafkaShareConsumer<K, V> {

		private @Nullable String idForListeners;

		protected ExtendedShareConsumer(Map<String, Object> configProps) {
			super(configProps, keyDeserializer(configProps), valueDeserializer(configProps));

			if (!DefaultShareConsumerFactory.this.listeners.isEmpty()) {
				Iterator<MetricName> metricIterator = metrics().keySet().iterator();
				String clientId = "unknown";
				if (metricIterator.hasNext()) {
					clientId = metricIterator.next().tags().get("client-id");
				}
				this.idForListeners = DefaultShareConsumerFactory.this.beanName + "." + clientId;
				for (Listener<K, V> listener : DefaultShareConsumerFactory.this.listeners) {
					listener.consumerAdded(this.idForListeners, this);
				}
			}
		}

		@Override
		public void close() {
			super.close();
			notifyConsumerRemoved();
		}

		@Override
		public void close(Duration timeout) {
			super.close(timeout);
			notifyConsumerRemoved();
		}

		private void notifyConsumerRemoved() {
			for (Listener<K, V> listener : DefaultShareConsumerFactory.this.listeners) {
				listener.consumerRemoved(this.idForListeners, this);
			}
		}

	}

}
