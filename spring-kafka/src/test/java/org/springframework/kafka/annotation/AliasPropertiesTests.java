/*
 * Copyright 2018-2021 the original author or authors.
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

package org.springframework.kafka.annotation;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.AnnotatedElement;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.AliasFor;
import org.springframework.kafka.annotation.AliasPropertiesTests.Config.ClassAndMethodLevelListener;
import org.springframework.kafka.annotation.KafkaListenerAnnotationBeanPostProcessor.AnnotationEnhancer;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerConfigUtils;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.stereotype.Component;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

/**
 * @author Gary Russell
 * @author Artem Bilan
 *
 * @since 2.2
 *
 */
@SpringJUnitConfig
@DirtiesContext
public class AliasPropertiesTests {

	@Autowired
	private KafkaTemplate<Integer, String> template;

	@Autowired
	private Config config;

	@Autowired
	private KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry;

	@Autowired
	private ClassAndMethodLevelListener classLevel;

	@Autowired
	private ClassAndMethodLevelListener repeatable;

	@Test
	public void testAliasFor() throws Exception {
		this.template.send("alias.tests", "foo");
		assertThat(this.config.latch.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(this.classLevel.latch.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(this.repeatable.latch.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(this.config.kafkaListenerEndpointRegistry()).isSameAs(this.kafkaListenerEndpointRegistry);
		assertThat(this.kafkaListenerEndpointRegistry.getListenerContainer("onMethodInConfigClass").getGroupId())
				.isEqualTo("onMethodInConfigClass.Config.listen1");
		assertThat(this.kafkaListenerEndpointRegistry.getListenerContainer("onMethod").getGroupId())
				.isEqualTo("onMethod.ClassAndMethodLevelListener.listen1");
		assertThat(this.kafkaListenerEndpointRegistry.getListenerContainer("onClass").getGroupId())
				.isEqualTo("onClass.ClassAndMethodLevelListener");
		assertThat(this.kafkaListenerEndpointRegistry.getListenerContainer("onMethodRepeatable1").getGroupId())
				.isEqualTo("onMethodRepeatable1.RepeatableClassAndMethodLevelListener.listen1");
		assertThat(this.kafkaListenerEndpointRegistry.getListenerContainer("onClassRepeatable1").getGroupId())
				.isEqualTo("onClassRepeatable1.RepeatableClassAndMethodLevelListener");
		assertThat(this.kafkaListenerEndpointRegistry.getListenerContainer("onMethodRepeatable2").getGroupId())
				.isEqualTo("onMethodRepeatable2.RepeatableClassAndMethodLevelListener.listen1");
		assertThat(this.kafkaListenerEndpointRegistry.getListenerContainer("onClassRepeatable2").getGroupId())
				.isEqualTo("onClassRepeatable2.RepeatableClassAndMethodLevelListener");
		assertThat(this.config.orderedCalledFirst).isTrue();
		assertThat(Config.orderedCalledFirst.get()).isTrue();
	}

	@Configuration
	@EnableKafka
	public static class Config {

		final CountDownLatch latch = new CountDownLatch(1);

		static AtomicBoolean orderedCalledFirst = new AtomicBoolean(true);

		@Bean
		public static AnnotationEnhancer mainEnhancer() {
			return (attrs, element) -> {
				attrs.put("groupId", attrs.get("id") + "." + (element instanceof Class
						? ((Class<?>) element).getSimpleName()
						: ((Method) element).getDeclaringClass().getSimpleName()
								+  "." + ((Method) element).getName()));
				orderedCalledFirst.set(true);
				return attrs;
			};
		}

		@Bean
		public static OrderedEnhancer ordered() {
			return new OrderedEnhancer(orderedCalledFirst);
		}

		@Bean(KafkaListenerConfigUtils.KAFKA_LISTENER_ENDPOINT_REGISTRY_BEAN_NAME)
		public KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry() {
			return new KafkaListenerEndpointRegistry();
		}

		@Bean
		public EmbeddedKafkaBroker embeddedKafka() {
			return new EmbeddedKafkaBroker(1, true, "alias.tests");
		}

		@Bean
		public KafkaListenerContainerFactory<?> kafkaListenerContainerFactory() {
			ConcurrentKafkaListenerContainerFactory<Integer, String> factory =
					new ConcurrentKafkaListenerContainerFactory<>();
			factory.setConsumerFactory(consumerFactory());
			return factory;
		}

		@Bean
		public DefaultKafkaConsumerFactory<Integer, String> consumerFactory() {
			return new DefaultKafkaConsumerFactory<>(consumerConfigs());
		}

		@Bean
		public Map<String, Object> consumerConfigs() {
			Map<String, Object> consumerProps =
					KafkaTestUtils.consumerProps("myAliasGroup", "false", embeddedKafka());
			return consumerProps;
		}

		@Bean
		public KafkaTemplate<Integer, String> template() {
			return new KafkaTemplate<>(producerFactory());
		}

		@Bean
		public ProducerFactory<Integer, String> producerFactory() {
			return new DefaultKafkaProducerFactory<>(producerConfigs());
		}

		@Bean
		public Map<String, Object> producerConfigs() {
			return KafkaTestUtils.producerProps(embeddedKafka());
		}

		@MyListener(id = "onMethodInConfigClass", value = "alias.tests")
		public void listen1(String in) {
			this.latch.countDown();
		}

		@Component
		@MyListener(id = "onClass", value = "alias.tests")
		public static class ClassAndMethodLevelListener {

			private final CountDownLatch latch = new CountDownLatch(2);

			@KafkaHandler
			void listen(String in) {
				this.latch.countDown();
			}

			@MyListener(id = "onMethod", value = "alias.tests")
			public void listen1(String in) {
				this.latch.countDown();
			}

		}

		@Component
		@KafkaListener(id = "onClassRepeatable1", topics = "alias.tests")
		@KafkaListener(id = "onClassRepeatable2", topics = "alias.tests")
		public static class RepeatableClassAndMethodLevelListener {

			private final CountDownLatch latch = new CountDownLatch(4);

			@KafkaHandler
			void listen(String in) {
				this.latch.countDown();
			}

			@KafkaListener(id = "onMethodRepeatable1", topics = "alias.tests")
			@KafkaListener(id = "onMethodRepeatable2", topics = "alias.tests")
			public void listen1(String in) {
				this.latch.countDown();
			}

		}

		public static class OrderedEnhancer implements AnnotationEnhancer, Ordered {

			private final AtomicBoolean orderedCalledFirst;

			public OrderedEnhancer(AtomicBoolean orderedCalledFirst) {
				this.orderedCalledFirst = orderedCalledFirst;
			}

			@Override
			public Map<String, Object> apply(Map<String, Object> attrs, AnnotatedElement ele) {
				this.orderedCalledFirst.set(false);
				return attrs;
			}

			@Override
			public int getOrder() {
				return 0;
			}

		}

	}

	@Target({ ElementType.METHOD, ElementType.TYPE })
	@Retention(RetentionPolicy.RUNTIME)
	@KafkaListener
	public @interface MyListener {

		@AliasFor(annotation = KafkaListener.class, attribute = "id")
		String id() default "";

		@AliasFor(annotation = KafkaListener.class, attribute = "topics")
		String[] value();

	}

}
