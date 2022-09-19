/*
 * Copyright 2022 the original author or authors.
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

package org.springframework.kafka.support.micrometer;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import io.micrometer.common.KeyValues;
import io.micrometer.core.tck.MeterRegistryAssert;
import io.micrometer.observation.ObservationRegistry;
import io.micrometer.tracing.Span.Kind;
import io.micrometer.tracing.exporter.FinishedSpan;
import io.micrometer.tracing.test.SampleTestRunner;
import io.micrometer.tracing.test.simple.SpanAssert;
import io.micrometer.tracing.test.simple.SpansAssert;

/**
 * @author Artem Bilan
 * @author Gary Russell
 *
 * @since 3.0
 */
public class ObservationIntegrationTests extends SampleTestRunner {

	@SuppressWarnings("unchecked")
	@Override
	public SampleTestRunnerConsumer yourCode() {
		// template -> listener -> template -> listener
		return (bb, meterRegistry) -> {
			ObservationRegistry observationRegistry = getObservationRegistry();
			try (AnnotationConfigApplicationContext applicationContext = new AnnotationConfigApplicationContext()) {
				applicationContext.registerBean(ObservationRegistry.class, () -> observationRegistry);
				applicationContext.register(Config.class);
				applicationContext.refresh();
				applicationContext.getBean(KafkaTemplate.class).send("int.observation.testT1", "test");
				assertThat(applicationContext.getBean(Listener.class).latch1.await(10, TimeUnit.SECONDS)).isTrue();
			}

			List<FinishedSpan> finishedSpans = bb.getFinishedSpans();
			SpansAssert.assertThat(finishedSpans)
					.haveSameTraceId()
					.hasSize(4);
			List<FinishedSpan> producerSpans = finishedSpans.stream()
					.filter(span -> span.getKind().equals(Kind.PRODUCER))
					.collect(Collectors.toList());
			List<FinishedSpan> consumerSpans = finishedSpans.stream()
					.filter(span -> span.getKind().equals(Kind.CONSUMER))
					.collect(Collectors.toList());
			SpanAssert.assertThat(producerSpans.get(0))
					.hasTag("spring.kafka.template.name", "template");
			SpanAssert.assertThat(producerSpans.get(1))
					.hasTag("spring.kafka.template.name", "template");
			SpanAssert.assertThat(consumerSpans.get(0))
					.hasTagWithKey("spring.kafka.listener.id");
			assertThat(consumerSpans.get(0).getTags().get("spring.kafka.listener.id")).isIn("obs1-0", "obs2-0");
			SpanAssert.assertThat(consumerSpans.get(1))
					.hasTagWithKey("spring.kafka.listener.id");
			assertThat(consumerSpans.get(1).getTags().get("spring.kafka.listener.id")).isIn("obs1-0", "obs2-0");
			assertThat(consumerSpans.get(0).getTags().get("spring.kafka.listener.id"))
					.isNotEqualTo(consumerSpans.get(1).getTags().get("spring.kafka.listener.id"));

			MeterRegistryAssert.assertThat(getMeterRegistry())
					.hasTimerWithNameAndTags("spring.kafka.template",
							KeyValues.of("spring.kafka.template.name", "template"))
					.hasTimerWithNameAndTags("spring.kafka.template",
							KeyValues.of("spring.kafka.template.name", "template"))
					.hasTimerWithNameAndTags("spring.kafka.listener",
							KeyValues.of("spring.kafka.listener.id", "obs1-0"))
					.hasTimerWithNameAndTags("spring.kafka.listener",
							KeyValues.of("spring.kafka.listener.id", "obs2-0"));
		};
	}


	@Configuration
	@EnableKafka
	public static class Config {

		@Bean
		EmbeddedKafkaBroker broker() {
			return new EmbeddedKafkaBroker(1, true, 1, "int.observation.testT1", "int.observation.testT2");
		}

		@Bean
		ProducerFactory<Integer, String> producerFactory(EmbeddedKafkaBroker broker) {
			Map<String, Object> producerProps = KafkaTestUtils.producerProps(broker);
			return new DefaultKafkaProducerFactory<>(producerProps);
		}

		@Bean
		ConsumerFactory<Integer, String> consumerFactory(EmbeddedKafkaBroker broker) {
			Map<String, Object> consumerProps = KafkaTestUtils.consumerProps("obs", "false", broker);
			return new DefaultKafkaConsumerFactory<>(consumerProps);
		}

		@Bean
		KafkaTemplate<Integer, String> template(ProducerFactory<Integer, String> pf) {
			KafkaTemplate<Integer, String> template = new KafkaTemplate<>(pf);
			template.setObservationEnabled(true);
			return template;
		}

		@Bean
		ConcurrentKafkaListenerContainerFactory<Integer, String> kafkaListenerContainerFactory(
				ConsumerFactory<Integer, String> cf) {

			ConcurrentKafkaListenerContainerFactory<Integer, String> factory =
					new ConcurrentKafkaListenerContainerFactory<>();
			factory.setConsumerFactory(cf);
			factory.getContainerProperties().setObservationEnabled(true);
			return factory;
		}

		@Bean
		Listener listener(KafkaTemplate<Integer, String> template) {
			return new Listener(template);
		}

	}

	public static class Listener {

		private final KafkaTemplate<Integer, String> template;

		final CountDownLatch latch1 = new CountDownLatch(1);

		volatile ConsumerRecord<Integer, String> record;

		public Listener(KafkaTemplate<Integer, String> template) {
			this.template = template;
		}

		@KafkaListener(id = "obs1", topics = "int.observation.testT1")
		void listen1(ConsumerRecord<Integer, String> in) {
			this.template.send("int.observation.testT2", in.value());
		}

		@KafkaListener(id = "obs2", topics = "int.observation.testT2")
		void listen2(ConsumerRecord<Integer, String> in) {
			this.record = in;
			this.latch1.countDown();
		}

	}


}
