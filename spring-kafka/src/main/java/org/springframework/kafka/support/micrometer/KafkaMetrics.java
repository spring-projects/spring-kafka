/*
 * Copyright 2020-2022 the original author or authors.
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

import java.util.Collections;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.logging.LogFactory;

import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.log.LogAccessor;
import org.springframework.lang.Nullable;
import org.springframework.util.ObjectUtils;

import io.micrometer.common.Tag;
import io.micrometer.common.Tags;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.observation.TimerObservationHandler;
import io.micrometer.observation.Observation.Context;
import io.micrometer.observation.ObservationRegistry;

/**
 * Observability utilities.
 *
 * @author Gary Russell
 * @since 2.5
 *
 */
public final class KafkaMetrics {

	private static final LogAccessor logger = new LogAccessor(LogFactory.getLog(KafkaMetrics.class));

	private static final ObservationRegistry OBSERVATION_REGISTRY;

	static {
		OBSERVATION_REGISTRY = ObservationRegistry.create();
		OBSERVATION_REGISTRY.observationConfig()
				.observationHandler(new CachingTimerObservationHandler(Metrics.globalRegistry));
	}

	private KafkaMetrics() {
	}

	/**
	 * Create an {@link ObservationRegistry} from the context's {@link MeterRegistry}.
	 * @param context the application context from which to obtain the meter registry.
	 * @return the registry.
	 */
	public static ObservationRegistry getRegistry(@Nullable ApplicationContext context) {
		if (context == null) {
			return OBSERVATION_REGISTRY;
		}
		Map<String, MeterRegistry> registries = context.getBeansOfType(MeterRegistry.class, false, false);
		registries = filterRegistries(registries, context);
		if (registries.size() == 1) {
			MeterRegistry unique = registries.values().iterator().next();
			ObservationRegistry registry = ObservationRegistry.create();
			registry.observationConfig().observationHandler(new TimerObservationHandler(unique));
			return registry;
		}
		else {
			logger.warn("No micrometer registry present (or more than one and not one marked @Primary), "
					+ "using the default global registry");
			return OBSERVATION_REGISTRY;
		}
	}

	private static Map<String, MeterRegistry> filterRegistries(Map<String, MeterRegistry> registries,
			ApplicationContext context) {

		if (registries.size() == 1) {
			return registries;
		}
		MeterRegistry primary = null;
		if (context instanceof ConfigurableApplicationContext) {
			BeanDefinitionRegistry bdr = (BeanDefinitionRegistry) ((ConfigurableApplicationContext) context)
					.getBeanFactory();
			for (Entry<String, MeterRegistry> entry : registries.entrySet()) {
				BeanDefinition beanDefinition = bdr.getBeanDefinition(entry.getKey());
				if (beanDefinition.isPrimary()) {
					if (primary != null) {
						primary = null;
						break;
					}
					else {
						primary = entry.getValue();
					}
				}
			}
		}
		if (primary != null) {
			return Collections.singletonMap("primary", primary);
		}
		else {
			return registries;
		}
	}

	/**
	 * Return the tags provider for a {@code KafkaTemplate}.
	 * @param extraTags the extra tags.
	 * @return the provider.
	 */
	public static KafkaTagsProvider templateTagsProvider(Map<String, String> extraTags) {

		return new KafkaTagsProvider() {

			@Override
			public Tags getLowCardinalityTags(Context context) {
				String beanName = context.get(KafkaTemplateObservation.TemplateLowCardinalityTags.BEAN_NAME.getKey());
				if (beanName == null) {
					beanName = "template.not.managed.by.spring";
				}
				Tag tag = KafkaTemplateObservation.TemplateLowCardinalityTags.BEAN_NAME.of(beanName);
				if (ObjectUtils.isEmpty(extraTags)) {
					return Tags.of(tag);
				}
				return Tags.of(tag).and(extraTags.entrySet().stream()
						.map(entry -> Tag.of(entry.getKey(), entry.getValue()))
						.toList());
			}

		};

	}

	/**
	 * Return the tags provider for a Kafka listener.
	 * @param extraTags the extra tags.
	 * @return the provider.
	 */
	public static KafkaTagsProvider listenerTagsProvider(Map<String, String> extraTags) {

		return new KafkaTagsProvider() {

			@Override
			public Tags getLowCardinalityTags(Context context) {
				String id = context.get(KafkaListenerObservation.ListenerLowCardinalityTags.LISTENER_ID.getKey());
				if (id == null) {
					id = "listener.not.managed.by.spring";
				}
				Tag tag = KafkaListenerObservation.ListenerLowCardinalityTags.LISTENER_ID.of(id);
				if (ObjectUtils.isEmpty(extraTags)) {
					return Tags.of(tag);
				}
				return Tags.of(tag).and(extraTags.entrySet().stream()
						.map(entry -> Tag.of(entry.getKey(), entry.getValue()))
						.toList()
						.toArray(new Tag[0]));
			}

		};

	}

}
