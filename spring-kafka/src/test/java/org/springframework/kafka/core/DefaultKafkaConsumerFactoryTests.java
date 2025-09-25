/*
 * Copyright 2017-present the original author or authors.
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
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.kafka.clients.consumer.CloseOptions;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import org.springframework.aop.framework.ProxyFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.kafka.core.ConsumerFactory.Listener;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.kafka.support.SendResult;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.kafka.transaction.KafkaTransactionManager;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/**
 * @author Gary Russell
 * @author Chris Gilbert
 * @author Artem Bilan
 * @author Adrian Gygax
 * @author Soby Chacko
 * @author Yaniv Nahoum
 *
 * @since 1.0.6
 */
@EmbeddedKafka(topics = { "txCache1", "txCache2", "txCacheSendFromListener" },
		brokerProperties = {
				"transaction.state.log.replication.factor=1",
				"transaction.state.log.min.isr=1" }
)
@SpringJUnitConfig
@DirtiesContext
public class DefaultKafkaConsumerFactoryTests {

	@Autowired
	private EmbeddedKafkaBroker embeddedKafka;

	@Test
	public void testProvidedDeserializerInstancesAreShared() {
		ConsumerFactory<String, String> target = new DefaultKafkaConsumerFactory<>(Collections.emptyMap(),
				new StringDeserializer() { }, null);
		assertThat(target.getKeyDeserializer()).isSameAs(target.getKeyDeserializer());
	}

	@Test
	public void testSupplierProvidedDeserializersAreNotShared() {
		ConsumerFactory<String, String> target = new DefaultKafkaConsumerFactory<>(Collections.emptyMap(),
				() -> new StringDeserializer() { }, null);
		assertThat(target.getKeyDeserializer()).isNotSameAs(target.getKeyDeserializer());
	}

	@Test
	public void testNoOverridesWhenCreatingConsumer() {
		Map<String, Object> originalConfig = Collections.emptyMap();
		final Map<String, Object> configPassedToKafkaConsumer = new HashMap<>();
		DefaultKafkaConsumerFactory<String, String> target =
				new DefaultKafkaConsumerFactory<String, String>(originalConfig) {

					@Override
					protected KafkaConsumer<String, String> createKafkaConsumer(Map<String, Object> configProps) {
						configPassedToKafkaConsumer.putAll(configProps);
						return null;
					}
				};
		target.createConsumer(null, null, null, null);
		assertThat(configPassedToKafkaConsumer).isEqualTo(originalConfig);
	}

	@Test
	public void testBootstrapServersSupplier() {
		Map<String, Object> originalConfig = Collections.emptyMap();
		final Map<String, Object> configPassedToKafkaConsumer = new HashMap<>();
		DefaultKafkaConsumerFactory<String, String> target =
				new DefaultKafkaConsumerFactory<String, String>(originalConfig) {

					@Override
					protected Consumer<String, String> createRawConsumer(Map<String, Object> configProps) {
						configPassedToKafkaConsumer.putAll(configProps);
						return null;
					}
				};
		target.setApplicationContext(createApplicationContextWithApplicationName());
		target.setBootstrapServersSupplier(() -> "foo");
		target.createConsumer(null, null, null, null);
		assertThat(configPassedToKafkaConsumer.get(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG)).isEqualTo("foo");
	}

	@Test
	public void testPropertyOverridesWhenCreatingConsumer() {
		Map<String, Object> originalConfig = Stream
				.of(new AbstractMap.SimpleEntry<>("config1", new Object()),
						new AbstractMap.SimpleEntry<>("config2", new Object()))
				.collect(Collectors.toMap(AbstractMap.SimpleEntry::getKey, AbstractMap.SimpleEntry::getValue));
		Properties overrides = new Properties();
		overrides.setProperty("config1", "overridden");
		final Map<String, Object> configPassedToKafkaConsumer = new HashMap<>();
		DefaultKafkaConsumerFactory<String, String> target =
				new DefaultKafkaConsumerFactory<String, String>(originalConfig) {

					@Override
					protected KafkaConsumer<String, String> createKafkaConsumer(Map<String, Object> configProps) {
						configPassedToKafkaConsumer.putAll(configProps);
						return null;
					}
				};
		target.setApplicationContext(createApplicationContextWithApplicationName());
		target.createConsumer(null, null, null, overrides);
		assertThat(configPassedToKafkaConsumer.get("config1")).isEqualTo("overridden");
		assertThat(configPassedToKafkaConsumer.get("config2")).isSameAs(originalConfig.get("config2"));
		target.updateConfigs(Map.of("config1", "newValue"));
		assertThat(target.getConfigurationProperties().get("config1")).isEqualTo("newValue");
		target.removeConfig("config1");
		assertThat(target.getConfigurationProperties().get("config1")).isNull();
	}

	@Test
	public void testSuffixOnExistingClientIdWhenCreatingConsumer() {
		Map<String, Object> originalConfig = Collections.singletonMap(ConsumerConfig.CLIENT_ID_CONFIG, "original");
		final Map<String, Object> configPassedToKafkaConsumer = new HashMap<>();
		DefaultKafkaConsumerFactory<String, String> target =
				new DefaultKafkaConsumerFactory<String, String>(originalConfig) {

					@Override
					protected KafkaConsumer<String, String> createKafkaConsumer(Map<String, Object> configProps) {
						configPassedToKafkaConsumer.putAll(configProps);
						return null;
					}
				};
		target.setApplicationContext(createApplicationContextWithApplicationName());
		target.createConsumer(null, null, "-1", null);
		assertThat(configPassedToKafkaConsumer.get(ConsumerConfig.CLIENT_ID_CONFIG)).isEqualTo("original-1");
	}

	@Test
	public void testSuffixWithoutExistingClientIdWhenCreatingConsumer() {
		final Map<String, Object> configPassedToKafkaConsumer = new HashMap<>();
		DefaultKafkaConsumerFactory<String, String> target =
				new DefaultKafkaConsumerFactory<String, String>(Collections.emptyMap()) {

					@Override
					protected KafkaConsumer<String, String> createKafkaConsumer(Map<String, Object> configProps) {
						configPassedToKafkaConsumer.putAll(configProps);
						return null;
					}
				};
		target.createConsumer(null, null, "-1", null);
		assertThat(configPassedToKafkaConsumer.get(ConsumerConfig.CLIENT_ID_CONFIG)).isNull();
	}

	@Test
	public void testPrefixOnExistingClientIdWhenCreatingConsumer() {
		Map<String, Object> originalConfig = Collections.singletonMap(ConsumerConfig.CLIENT_ID_CONFIG, "original");
		final Map<String, Object> configPassedToKafkaConsumer = new HashMap<>();
		DefaultKafkaConsumerFactory<String, String> target =
				new DefaultKafkaConsumerFactory<String, String>(originalConfig) {

					@Override
					protected KafkaConsumer<String, String> createKafkaConsumer(Map<String, Object> configProps) {
						configPassedToKafkaConsumer.putAll(configProps);
						return null;
					}
				};
		target.setApplicationContext(createApplicationContextWithApplicationName());
		target.createConsumer(null, "overridden", null, null);
		assertThat(configPassedToKafkaConsumer.get(ConsumerConfig.CLIENT_ID_CONFIG)).isEqualTo("overridden");
	}

	@Test
	public void testPrefixWithoutExistingClientIdWhenCreatingConsumer() {
		final Map<String, Object> configPassedToKafkaConsumer = new HashMap<>();
		DefaultKafkaConsumerFactory<String, String> target =
				new DefaultKafkaConsumerFactory<String, String>(Collections.emptyMap()) {

					@Override
					protected KafkaConsumer<String, String> createKafkaConsumer(Map<String, Object> configProps) {
						configPassedToKafkaConsumer.putAll(configProps);
						return null;
					}
				};
		target.setApplicationContext(createApplicationContextWithApplicationName());
		target.createConsumer(null, "overridden", null, null);
		assertThat(configPassedToKafkaConsumer.get(ConsumerConfig.CLIENT_ID_CONFIG)).isEqualTo("overridden");
	}

	@Test
	public void testSuffixAndPrefixOnExistingClientIdWhenCreatingConsumer() {
		Map<String, Object> originalConfig = Collections.singletonMap(ConsumerConfig.CLIENT_ID_CONFIG, "original");
		final Map<String, Object> configPassedToKafkaConsumer = new HashMap<>();
		DefaultKafkaConsumerFactory<String, String> target =
				new DefaultKafkaConsumerFactory<String, String>(originalConfig) {

					@Override
					protected KafkaConsumer<String, String> createKafkaConsumer(Map<String, Object> configProps) {
						configPassedToKafkaConsumer.putAll(configProps);
						return null;
					}
				};
		target.setApplicationContext(createApplicationContextWithApplicationName());
		target.createConsumer(null, "overridden", "-1", null);
		assertThat(configPassedToKafkaConsumer.get(ConsumerConfig.CLIENT_ID_CONFIG)).isEqualTo("overridden-1");
	}

	@Test
	public void testSuffixAndPrefixOnOverridenClientIdWhenCreatingConsumer() {
		Map<String, Object> originalConfig = Collections.singletonMap(ConsumerConfig.CLIENT_ID_CONFIG, "original");
		Properties overrides = new Properties();
		overrides.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, "property-overridden");
		final Map<String, Object> configPassedToKafkaConsumer = new HashMap<>();
		DefaultKafkaConsumerFactory<String, String> target =
				new DefaultKafkaConsumerFactory<String, String>(originalConfig) {

					@Override
					protected KafkaConsumer<String, String> createKafkaConsumer(Map<String, Object> configProps) {
						configPassedToKafkaConsumer.putAll(configProps);
						return null;
					}
				};
		target.setApplicationContext(createApplicationContextWithApplicationName());
		target.createConsumer(null, "overridden", "-1", overrides);
		assertThat(configPassedToKafkaConsumer.get(ConsumerConfig.CLIENT_ID_CONFIG)).isEqualTo("overridden-1");
	}

	@Test
	public void testApplicationNameIfNoGroupIdAsClientIdWhenCreatingConsumer() {
		final Map<String, Object> configPassedToKafkaConsumer = new HashMap<>();
		DefaultKafkaConsumerFactory<String, String> target =
				new DefaultKafkaConsumerFactory<String, String>(Map.of()) {

					@Override
					protected KafkaConsumer<String, String> createKafkaConsumer(Map<String, Object> configProps) {
						configPassedToKafkaConsumer.putAll(configProps);
						return null;
					}
				};
		target.setApplicationContext(createApplicationContextWithApplicationName());
		target.createConsumer(null, null, "-1", null);
		assertThat(configPassedToKafkaConsumer.get(ConsumerConfig.CLIENT_ID_CONFIG)).isEqualTo("appname-consumer-1");
	}

	@Test
	public void testOverriddenGroupIdWhenCreatingConsumer() {
		Map<String, Object> originalConfig = Collections.singletonMap(ConsumerConfig.GROUP_ID_CONFIG, "original");
		final Map<String, Object> configPassedToKafkaConsumer = new HashMap<>();
		DefaultKafkaConsumerFactory<String, String> target =
				new DefaultKafkaConsumerFactory<String, String>(originalConfig) {

					@Override
					protected KafkaConsumer<String, String> createKafkaConsumer(Map<String, Object> configProps) {
						configPassedToKafkaConsumer.putAll(configProps);
						return null;
					}
				};
		target.createConsumer("overridden", null, null, null);
		assertThat(configPassedToKafkaConsumer.get(ConsumerConfig.GROUP_ID_CONFIG)).isEqualTo("overridden");
	}

	@Test
	public void testOverriddenGroupIdWithoutExistingGroupIdWhenCreatingConsumer() {
		final Map<String, Object> configPassedToKafkaConsumer = new HashMap<>();
		DefaultKafkaConsumerFactory<String, String> target =
				new DefaultKafkaConsumerFactory<String, String>(Collections.emptyMap()) {

					@Override
					protected KafkaConsumer<String, String> createKafkaConsumer(Map<String, Object> configProps) {
						configPassedToKafkaConsumer.putAll(configProps);
						return null;
					}
				};
		target.createConsumer("overridden", null, null, null);
		assertThat(configPassedToKafkaConsumer.get(ConsumerConfig.GROUP_ID_CONFIG)).isEqualTo("overridden");
	}

	@Test
	public void testOverriddenGroupIdOnOverriddenPropertyWhenCreatingConsumer() {
		Map<String, Object> originalConfig = Collections.singletonMap(ConsumerConfig.GROUP_ID_CONFIG, "original");
		Properties overrides = new Properties();
		overrides.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "property-overridden");
		final Map<String, Object> configPassedToKafkaConsumer = new HashMap<>();
		DefaultKafkaConsumerFactory<String, String> target =
				new DefaultKafkaConsumerFactory<String, String>(originalConfig) {

					@Override
					protected KafkaConsumer<String, String> createKafkaConsumer(Map<String, Object> configProps) {
						configPassedToKafkaConsumer.putAll(configProps);
						return null;
					}
				};
		target.createConsumer("overridden", null, null, overrides);
		assertThat(configPassedToKafkaConsumer.get(ConsumerConfig.GROUP_ID_CONFIG)).isEqualTo("overridden");
	}

	@Test
	public void testOverriddenMaxPollRecordsOnly() {
		Map<String, Object> originalConfig = Collections.singletonMap(ConsumerConfig.GROUP_ID_CONFIG, "original");
		Properties overrides = new Properties();
		overrides.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "2");
		final Map<String, Object> configPassedToKafkaConsumer = new HashMap<>();
		DefaultKafkaConsumerFactory<String, String> target =
				new DefaultKafkaConsumerFactory<String, String>(originalConfig) {

					@Override
					protected KafkaConsumer<String, String> createKafkaConsumer(Map<String, Object> configProps) {
						configPassedToKafkaConsumer.putAll(configProps);
						return null;
					}

				};
		target.createConsumer(null, null, null, overrides);
		assertThat(configPassedToKafkaConsumer.get(ConsumerConfig.MAX_POLL_RECORDS_CONFIG)).isEqualTo("2");
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testNestedTxProducerIsCached() throws Exception {
		Map<String, Object> producerProps = KafkaTestUtils.producerProps(this.embeddedKafka);
		DefaultKafkaProducerFactory<Integer, String> pf = new DefaultKafkaProducerFactory<>(producerProps);
		KafkaTemplate<Integer, String> template = new KafkaTemplate<>(pf);
		DefaultKafkaProducerFactory<Integer, String> pfTx = new DefaultKafkaProducerFactory<>(producerProps);
		pfTx.setTransactionIdPrefix("fooTx.");
		KafkaOperations<Integer, String> templateTx = new KafkaTemplate<>(pfTx);
		Map<String, Object> consumerProps = KafkaTestUtils.consumerProps(this.embeddedKafka, "txCache1Group", false);
		DefaultKafkaConsumerFactory<Integer, String> cf = new DefaultKafkaConsumerFactory<>(consumerProps);
		AtomicReference<Consumer<Integer, String>> wrapped = new AtomicReference<>();
		cf.addPostProcessor(consumer -> {
			ProxyFactory prox = new ProxyFactory();
			prox.setTarget(consumer);
			@SuppressWarnings("unchecked")
			Consumer<Integer, String> proxy = (Consumer<Integer, String>) prox.getProxy();
			wrapped.set(proxy);
			return proxy;
		});
		ContainerProperties containerProps = new ContainerProperties("txCache1");
		CountDownLatch latch = new CountDownLatch(1);
		containerProps.setMessageListener((MessageListener<Integer, String>) r -> {
			templateTx.executeInTransaction(t -> t.send("txCacheSendFromListener", "bar"));
			templateTx.executeInTransaction(t -> t.send("txCacheSendFromListener", "baz"));
			latch.countDown();
		});
		KafkaTransactionManager<Integer, String> tm = new KafkaTransactionManager<>(pfTx);
		containerProps.setKafkaAwareTransactionManager(tm);
		KafkaMessageListenerContainer<Integer, String> container = new KafkaMessageListenerContainer<>(cf,
				containerProps);
		container.start();
		try {
			CompletableFuture<SendResult<Integer, String>> future = template.send("txCache1", "foo");
			future.get(10, TimeUnit.SECONDS);
			pf.getCache();
			assertThat(KafkaTestUtils.getPropertyValue(pf, "cache", Map.class)).hasSize(0);
			assertThat(latch.await(30, TimeUnit.SECONDS)).isTrue();
			assertThat(KafkaTestUtils.getPropertyValue(pfTx, "cache", Map.class)).hasSize(1);
			assertThat(pfTx.getCache()).hasSize(1);
			assertThat(KafkaTestUtils.getPropertyValue(container, "listenerConsumer.consumer")).isSameAs(wrapped.get());
		}
		finally {
			container.stop();
			pf.destroy();
			pfTx.destroy();
		}
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testNestedTxProducerIsFixed() throws Exception {
		Map<String, Object> producerProps = KafkaTestUtils.producerProps(this.embeddedKafka);
		DefaultKafkaProducerFactory<Integer, String> pf = new DefaultKafkaProducerFactory<>(producerProps);
		KafkaTemplate<Integer, String> template = new KafkaTemplate<>(pf);
		DefaultKafkaProducerFactory<Integer, String> pfTx = new DefaultKafkaProducerFactory<>(producerProps);
		pfTx.setTransactionIdPrefix("fooTx.fixed.");
		TransactionIdSuffixStrategy suffixStrategy = new DefaultTransactionIdSuffixStrategy(3);
		pfTx.setTransactionIdSuffixStrategy(suffixStrategy);
		KafkaOperations<Integer, String> templateTx = new KafkaTemplate<>(pfTx);
		Map<String, Object> consumerProps = KafkaTestUtils.consumerProps(this.embeddedKafka, "txCache1FixedGroup", false);
		DefaultKafkaConsumerFactory<Integer, String> cf = new DefaultKafkaConsumerFactory<>(consumerProps);
		AtomicReference<Consumer<Integer, String>> wrapped = new AtomicReference<>();
		cf.addPostProcessor(consumer -> {
			ProxyFactory prox = new ProxyFactory();
			prox.setTarget(consumer);
			@SuppressWarnings("unchecked")
			Consumer<Integer, String> proxy = (Consumer<Integer, String>) prox.getProxy();
			wrapped.set(proxy);
			return proxy;
		});
		ContainerProperties containerProps = new ContainerProperties("txCache1Fixed");
		CountDownLatch latch = new CountDownLatch(1);
		containerProps.setMessageListener((MessageListener<Integer, String>) r -> {
			templateTx.executeInTransaction(t -> t.send("txCacheSendFromListener", "bar"));
			templateTx.executeInTransaction(t -> t.send("txCacheSendFromListener", "baz"));
			latch.countDown();
		});
		KafkaTransactionManager<Integer, String> tm = new KafkaTransactionManager<>(pfTx);
		containerProps.setKafkaAwareTransactionManager(tm);
		KafkaMessageListenerContainer<Integer, String> container = new KafkaMessageListenerContainer<>(cf,
				containerProps);
		container.start();
		try {
			CompletableFuture<SendResult<Integer, String>> future = template.send("txCache1Fixed", "foo");
			future.get(10, TimeUnit.SECONDS);
			pf.getCache();
			assertThat(KafkaTestUtils.getPropertyValue(pf, "cache", Map.class)).hasSize(0);
			assertThat(latch.await(30, TimeUnit.SECONDS)).isTrue();
			assertThat(KafkaTestUtils.getPropertyValue(pfTx, "cache", Map.class)).hasSize(1);
			assertThat(pfTx.getCache()).hasSize(1);
			assertThat(KafkaTestUtils.getPropertyValue(suffixStrategy, "suffixCache", Map.class)).hasSize(1);
			//  1 tm tx producer and 1 templateTx tx producer
			assertThat(KafkaTestUtils.getPropertyValue(container, "listenerConsumer.consumer")).isSameAs(wrapped.get());
		}
		finally {
			container.stop();
			pf.destroy();
			pfTx.destroy();
		}
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@ParameterizedTest
	@ValueSource(booleans = { true, false })
	void listener(boolean closeWithTimeout) {
		Map<String, Object> consumerConfig = KafkaTestUtils.consumerProps(this.embeddedKafka, "txCache1Group", false);
		consumerConfig.put(ConsumerConfig.CLIENT_ID_CONFIG, "foo-0");
		DefaultKafkaConsumerFactory cf = new DefaultKafkaConsumerFactory(consumerConfig);
		List<String> adds = new ArrayList<>();
		List<String> removals = new ArrayList<>();

		cf.addListener(new Listener() {

			@Override
			public void consumerAdded(String id, Consumer consumer) {
				adds.add(id);
			}

			@Override
			public void consumerRemoved(String id, Consumer consumer) {
				removals.add(id);
			}

		});
		cf.setBeanName("cf");

		Consumer consumer = cf.createConsumer();
		assertThat(adds).hasSize(1);
		assertThat(adds.get(0)).isEqualTo("cf.foo-0");
		assertThat(removals).isEmpty();
		if (closeWithTimeout) {
			consumer.close(CloseOptions.timeout(Duration.ofSeconds(10)));
		}
		else {
			consumer.close();
		}
		assertThat(removals).hasSize(1);
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Test
	void configDeserializer() {
		Deserializer key = mock(Deserializer.class);
		Deserializer value = mock(Deserializer.class);
		Map<String, Object> config = KafkaTestUtils.consumerProps(this.embeddedKafka, "mockGroup", false);
		DefaultKafkaConsumerFactory cf = new DefaultKafkaConsumerFactory(config, key, value);
		Deserializer keyDeserializer = cf.getKeyDeserializer();
		assertThat(keyDeserializer).isSameAs(key);
		cf.createKafkaConsumer(config);
		verify(key).configure(config, true);
		Deserializer valueDeserializer = cf.getValueDeserializer();
		assertThat(valueDeserializer).isSameAs(value);
		verify(value).configure(config, false);
	}

	private static ApplicationContext createApplicationContextWithApplicationName() {
		final Environment environment = mock(Environment.class);
		given(environment.getProperty("spring.application.name")).willReturn("appname");
		final ApplicationContext applicationContext = mock(ApplicationContext.class);
		given(applicationContext.getEnvironment()).willReturn(environment);
		return applicationContext;
	}

	@Configuration
	public static class Config {

	}

}
