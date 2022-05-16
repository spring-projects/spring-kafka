/*
 * Copyright 2016-2022 the original author or authors.
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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

import org.aopalliance.aop.Advice;
import org.aopalliance.intercept.MethodInterceptor;
import org.aopalliance.intercept.MethodInvocation;
import org.apache.commons.logging.LogFactory;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.serialization.Deserializer;

import org.springframework.aop.framework.ProxyFactory;
import org.springframework.aop.support.NameMatchMethodPointcutAdvisor;
import org.springframework.beans.factory.BeanNameAware;
import org.springframework.core.log.LogAccessor;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

/**
 * The {@link ConsumerFactory} implementation to produce new {@link Consumer} instances
 * for provided {@link Map} {@code configs} and optional {@link Deserializer}s on each {@link #createConsumer()}
 * invocation.
 * <p>
 * If you are using {@link Deserializer}s that have no-arg constructors and require no setup, then simplest to
 * specify {@link Deserializer} classes against {@link ConsumerConfig#KEY_DESERIALIZER_CLASS_CONFIG} and
 * {@link ConsumerConfig#VALUE_DESERIALIZER_CLASS_CONFIG} keys in the {@code configs} passed to the
 * {@link DefaultKafkaConsumerFactory} constructor.
 * <p>
 * If that is not possible, but you are using {@link Deserializer}s that may be shared between all {@link Consumer}
 * instances (and specifically that their close() method is a no-op), then you can pass in {@link Deserializer}
 * instances for one or both of the key and value deserializers.
 * <p>
 * If neither of the above is true then you may provide a {@link Supplier} for one or both {@link Deserializer}s
 * which will be used to obtain {@link Deserializer}(s) each time a {@link Consumer} is created by the factory.
 *
 * @param <K> the key type.
 * @param <V> the value type.
 *
 * @author Gary Russell
 * @author Murali Reddy
 * @author Artem Bilan
 * @author Chris Gilbert
 */
public class DefaultKafkaConsumerFactory<K, V> extends KafkaResourceFactory
		implements ConsumerFactory<K, V>, BeanNameAware {

	private static final LogAccessor LOGGER = new LogAccessor(LogFactory.getLog(DefaultKafkaConsumerFactory.class));

	private final Map<String, Object> configs;

	private final List<Listener<K, V>> listeners = new ArrayList<>();

	private final List<ConsumerPostProcessor<K, V>> postProcessors = new ArrayList<>();

	private Supplier<Deserializer<K>> keyDeserializerSupplier;

	private Supplier<Deserializer<V>> valueDeserializerSupplier;

	private String beanName = "not.managed.by.Spring";

	private boolean configureDeserializers = true;

	/**
	 * Construct a factory with the provided configuration.
	 * @param configs the configuration.
	 */
	public DefaultKafkaConsumerFactory(Map<String, Object> configs) {
		this(configs, () -> null, () -> null);
	}

	/**
	 * Construct a factory with the provided configuration and deserializers.
	 * The deserializers' {@code configure()} methods will be called with the
	 * configuration map.
	 * @param configs the configuration.
	 * @param keyDeserializer the key {@link Deserializer}.
	 * @param valueDeserializer the value {@link Deserializer}.
	 */
	public DefaultKafkaConsumerFactory(Map<String, Object> configs,
			@Nullable Deserializer<K> keyDeserializer,
			@Nullable Deserializer<V> valueDeserializer) {

		this(configs, () -> keyDeserializer, () -> valueDeserializer);
	}

	/**
	 * Construct a factory with the provided configuration and deserializers.
	 * The deserializers' {@code configure()} methods will be called with the
	 * configuration map unless {@code configureDeserializers} is false.
	 * @param configs the configuration.
	 * @param keyDeserializer the key {@link Deserializer}.
	 * @param valueDeserializer the value {@link Deserializer}.
	 * @param configureDeserializers false to not configure the deserializers.
	 * @since 2.8.7
	 */
	public DefaultKafkaConsumerFactory(Map<String, Object> configs,
			@Nullable Deserializer<K> keyDeserializer,
			@Nullable Deserializer<V> valueDeserializer, boolean configureDeserializers) {

		this(configs, () -> keyDeserializer, () -> valueDeserializer, configureDeserializers);
	}

	/**
	 * Construct a factory with the provided configuration and deserializer suppliers.
	 * When the suppliers are invoked to get an instance, the deserializers'
	 * {@code configure()} methods will be called with the configuration map.
	 * @param configs the configuration.
	 * @param keyDeserializerSupplier the key {@link Deserializer} supplier function.
	 * @param valueDeserializerSupplier the value {@link Deserializer} supplier function.
	 * @since 2.3
	 */
	public DefaultKafkaConsumerFactory(Map<String, Object> configs,
			@Nullable Supplier<Deserializer<K>> keyDeserializerSupplier,
			@Nullable Supplier<Deserializer<V>> valueDeserializerSupplier) {

		this(configs, keyDeserializerSupplier, valueDeserializerSupplier, true);
	}

	/**
	 * Construct a factory with the provided configuration and deserializer suppliers.
	 * When the suppliers are invoked to get an instance, the deserializers'
	 * {@code configure()} methods will be called with the configuration map unless
	 * {@code configureDeserializers} is false.
	 * @param configs the configuration.
	 * @param keyDeserializerSupplier the key {@link Deserializer} supplier function.
	 * @param valueDeserializerSupplier the value {@link Deserializer} supplier function.
	 * @param configureDeserializers false to not configure the deserializers.
	 * @since 2.8.7
	 */
	public DefaultKafkaConsumerFactory(Map<String, Object> configs,
			@Nullable Supplier<Deserializer<K>> keyDeserializerSupplier,
			@Nullable Supplier<Deserializer<V>> valueDeserializerSupplier, boolean configureDeserializers) {

		this.configs = new ConcurrentHashMap<>(configs);
		this.configureDeserializers = configureDeserializers;
		this.keyDeserializerSupplier = keyDeserializerSupplier(keyDeserializerSupplier);
		this.valueDeserializerSupplier = valueDeserializerSupplier(valueDeserializerSupplier);
	}

	private Supplier<Deserializer<K>> keyDeserializerSupplier(
			@Nullable Supplier<Deserializer<K>> keyDeserializerSupplier) {

		if (!this.configureDeserializers) {
			return keyDeserializerSupplier;
		}
		return keyDeserializerSupplier == null
				? () -> null
				: () -> {
					Deserializer<K> deserializer = keyDeserializerSupplier.get();
					if (deserializer != null) {
						deserializer.configure(this.configs, true);
					}
					return deserializer;
				};
	}

	private Supplier<Deserializer<V>> valueDeserializerSupplier(
			@Nullable Supplier<Deserializer<V>> valueDeserializerSupplier) {

		if (!this.configureDeserializers) {
			return valueDeserializerSupplier;
		}
		return valueDeserializerSupplier == null
				? () -> null
				: () -> {
					Deserializer<V> deserializer = valueDeserializerSupplier.get();
					if (deserializer != null) {
						deserializer.configure(this.configs, false);
					}
					return deserializer;
				};
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
		this.keyDeserializerSupplier = keyDeserializerSupplier(() -> keyDeserializer);
	}

	/**
	 * Set the value deserializer. The deserializer will be configured using the consumer
	 * configuration, unless {@link #setConfigureDeserializers(boolean)
	 * configureDeserializers} is false.
	 * @param valueDeserializer the value deserializer.
	 */
	public void setValueDeserializer(@Nullable Deserializer<V> valueDeserializer) {
		this.valueDeserializerSupplier = valueDeserializerSupplier(() -> valueDeserializer);
	}

	/**
	 * Set a supplier to supply instances of the key deserializer. The deserializer will
	 * be configured using the consumer configuration, unless
	 * {@link #setConfigureDeserializers(boolean) configureDeserializers} is false.
	 * @param keyDeserializerSupplier the supplier.
	 * @since 2.8
	 */
	public void setKeyDeserializerSupplier(Supplier<Deserializer<K>> keyDeserializerSupplier) {
		this.keyDeserializerSupplier = keyDeserializerSupplier(keyDeserializerSupplier);
	}

	/**
	 * Set a supplier to supply instances of the value deserializer. The deserializer will
	 * be configured using the consumer configuration, unless
	 * {@link #setConfigureDeserializers(boolean) configureDeserializers} is false.
	 * @param valueDeserializerSupplier the supplier.
	 * @since 2.8
	 */
	public void setValueDeserializerSupplier(Supplier<Deserializer<V>> valueDeserializerSupplier) {
		this.valueDeserializerSupplier = valueDeserializerSupplier(valueDeserializerSupplier);
	}


	/**
	 * Set to false (default true) to prevent programmatically provided deserializers (via
	 * constructor or setters) from being configured using the producer configuration,
	 * e.g. if the deserializers are already fully configured.
	 * @param configureDeserializers false to not configure.
	 * @since 2.8.7
	 * @see #setKeyDeserializer(Deserializer)
	 * @see #setKeyDeserializerSupplier(Supplier)
	 * @see #setValueDeserializer(Deserializer)
	 * @see #setValueDeserializerSupplier(Supplier)
	 **/
	public void setConfigureDeserializers(boolean configureDeserializers) {
		this.configureDeserializers = configureDeserializers;
	}

	@Override
	public Map<String, Object> getConfigurationProperties() {
		Map<String, Object> configs2 = new HashMap<>(this.configs);
		checkBootstrap(configs2);
		return Collections.unmodifiableMap(configs2);
	}

	@Override
	public Deserializer<K> getKeyDeserializer() {
		return this.keyDeserializerSupplier.get();
	}

	@Override
	public Deserializer<V> getValueDeserializer() {
		return this.valueDeserializerSupplier.get();
	}

	/**
	 * Get the current list of listeners.
	 * @return the listeners.
	 * @since 2.5
	 */
	@Override
	public List<Listener<K, V>> getListeners() {
		return Collections.unmodifiableList(this.listeners);
	}

	@Override
	public List<ConsumerPostProcessor<K, V>> getPostProcessors() {
		return Collections.unmodifiableList(this.postProcessors);
	}

	/**
	 * Add a listener.
	 * @param listener the listener.
	 * @since 2.5
	 */
	@Override
	public void addListener(Listener<K, V> listener) {
		Assert.notNull(listener, "'listener' cannot be null");
		this.listeners.add(listener);
	}

	/**
	 * Add a listener at a specific index.
	 * @param index the index (list position).
	 * @param listener the listener.
	 * @since 2.5
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

	@Override
	public void addPostProcessor(ConsumerPostProcessor<K, V> postProcessor) {
		Assert.notNull(postProcessor, "'postProcessor' cannot be null");
		this.postProcessors.add(postProcessor);
	}

	@Override
	public boolean removePostProcessor(ConsumerPostProcessor<K, V> postProcessor) {
		return this.postProcessors.remove(postProcessor);
	}

	/**
	 * Remove a listener.
	 * @param listener the listener.
	 * @return true if removed.
	 * @since 2.5
	 */
	@Override
	public boolean removeListener(Listener<K, V> listener) {
		return this.listeners.remove(listener);
	}

	@Override
	public void updateConfigs(Map<String, Object> updates) {
		this.configs.putAll(updates);
	}

	@Override
	public void removeConfig(String configKey) {
		this.configs.remove(configKey);
	}

	@Override
	public Consumer<K, V> createConsumer(@Nullable String groupId, @Nullable String clientIdPrefix,
			@Nullable String clientIdSuffix) {

		return createKafkaConsumer(groupId, clientIdPrefix, clientIdSuffix, null);
	}

	@Override
	public Consumer<K, V> createConsumer(@Nullable String groupId, @Nullable String clientIdPrefix,
			@Nullable final String clientIdSuffixArg, @Nullable Properties properties) {

		return createKafkaConsumer(groupId, clientIdPrefix, clientIdSuffixArg, properties);
	}

	protected Consumer<K, V> createKafkaConsumer(@Nullable String groupId, @Nullable String clientIdPrefixArg,
			@Nullable String clientIdSuffixArg, @Nullable Properties properties) {

		boolean overrideClientIdPrefix = StringUtils.hasText(clientIdPrefixArg);
		String clientIdPrefix = clientIdPrefixArg;
		String clientIdSuffix = clientIdSuffixArg;
		if (clientIdPrefix == null) {
			clientIdPrefix = "";
		}
		if (clientIdSuffix == null) {
			clientIdSuffix = "";
		}
		boolean shouldModifyClientId = (this.configs.containsKey(ConsumerConfig.CLIENT_ID_CONFIG)
				&& StringUtils.hasText(clientIdSuffix)) || overrideClientIdPrefix;
		if (groupId == null
				&& (properties == null || properties.stringPropertyNames().size() == 0)
				&& !shouldModifyClientId) {
			return createKafkaConsumer(new HashMap<>(this.configs));
		}
		else {
			return createConsumerWithAdjustedProperties(groupId, clientIdPrefix, properties, overrideClientIdPrefix,
					clientIdSuffix, shouldModifyClientId);
		}
	}

	private Consumer<K, V> createConsumerWithAdjustedProperties(@Nullable String groupId, String clientIdPrefix,
			@Nullable Properties properties, boolean overrideClientIdPrefix, String clientIdSuffix,
			boolean shouldModifyClientId) {

		Map<String, Object> modifiedConfigs = new HashMap<>(this.configs);
		if (groupId != null) {
			modifiedConfigs.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
		}
		if (shouldModifyClientId) {
			modifiedConfigs.put(ConsumerConfig.CLIENT_ID_CONFIG,
					(overrideClientIdPrefix ? clientIdPrefix
							: modifiedConfigs.get(ConsumerConfig.CLIENT_ID_CONFIG)) + clientIdSuffix);
		}
		if (properties != null) {
			Set<String> stringPropertyNames = properties.stringPropertyNames();  // to get any nested default Properties
			stringPropertyNames
					.stream()
					.filter(name -> !name.equals(ConsumerConfig.CLIENT_ID_CONFIG)
							&& !name.equals(ConsumerConfig.GROUP_ID_CONFIG))
					.forEach(name -> modifiedConfigs.put(name, properties.getProperty(name)));
			properties.entrySet().stream()
					.filter(entry -> !entry.getKey().equals(ConsumerConfig.CLIENT_ID_CONFIG)
							&& !entry.getKey().equals(ConsumerConfig.GROUP_ID_CONFIG)
							&& !stringPropertyNames.contains(entry.getKey())
							&& entry.getKey() instanceof String)
					.forEach(entry -> modifiedConfigs.put((String) entry.getKey(), entry.getValue()));
			checkInaccessible(properties, modifiedConfigs);
		}
		return createKafkaConsumer(modifiedConfigs);
	}

	private void checkInaccessible(Properties properties, Map<String, Object> modifiedConfigs) {
		List<Object> inaccessible = null;
		for (Enumeration<?> propertyNames = properties.propertyNames(); propertyNames.hasMoreElements(); ) {
			Object nextElement = propertyNames.nextElement();
			if (!modifiedConfigs.containsKey(nextElement)) {
				if (inaccessible == null) {
					inaccessible = new ArrayList<>();
				}
				inaccessible.add(nextElement);
			}
		}
		if (inaccessible != null) {
			LOGGER.error("Non-String-valued default properties are inaccessible; use String values or "
					+ "make them explicit properties instead of defaults: "
					+ inaccessible);
		}
	}

	@SuppressWarnings("resource")
	protected Consumer<K, V> createKafkaConsumer(Map<String, Object> configProps) {
		checkBootstrap(configProps);
		Consumer<K, V> kafkaConsumer = createRawConsumer(configProps);

		if (this.listeners.size() > 0) {
			Map<MetricName, ? extends Metric> metrics = kafkaConsumer.metrics();
			Iterator<MetricName> metricIterator = metrics.keySet().iterator();
			String clientId;
			if (metricIterator.hasNext()) {
				clientId = metricIterator.next().tags().get("client-id");
			}
			else {
				clientId = "unknown";
			}
			String id = this.beanName + "." + clientId;
			kafkaConsumer = createProxy(kafkaConsumer, id);
			for (Listener<K, V> listener : this.listeners) {
				listener.consumerAdded(id, kafkaConsumer);
			}
		}
		for (ConsumerPostProcessor<K, V> pp : this.postProcessors) {
			kafkaConsumer = pp.apply(kafkaConsumer);
		}
		return kafkaConsumer;
	}

	/**
	 * Create a Consumer.
	 * @param configProps the configuration properties.
	 * @return the consumer.
	 * @since 2.5
	 */
	protected Consumer<K, V> createRawConsumer(Map<String, Object> configProps) {
		return new KafkaConsumer<>(configProps, this.keyDeserializerSupplier.get(),
				this.valueDeserializerSupplier.get());
	}

	@SuppressWarnings("unchecked")
	private Consumer<K, V> createProxy(Consumer<K, V> kafkaConsumer, String id) {
		ProxyFactory pf = new ProxyFactory(kafkaConsumer);
		Advice advice = new MethodInterceptor() {

			@Override
			public Object invoke(MethodInvocation invocation) throws Throwable {
				DefaultKafkaConsumerFactory.this.listeners.forEach(listener ->
						listener.consumerRemoved(id, kafkaConsumer));
				return invocation.proceed();
			}

		};
		NameMatchMethodPointcutAdvisor advisor = new NameMatchMethodPointcutAdvisor(advice);
		advisor.addMethodName("close");
		pf.addAdvisor(advisor);
		return (Consumer<K, V>) pf.getProxy();
	}

	@Override
	public boolean isAutoCommit() {
		Object auto = this.configs.get(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG);
		return auto instanceof Boolean ? (Boolean) auto
				: auto instanceof String ? Boolean.valueOf((String) auto) : true;
	}

}
