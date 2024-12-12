/*
 * Copyright 2017-2024 the original author or authors.
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

package org.springframework.kafka.test.context;

import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.beans.factory.support.DefaultSingletonBeanRegistry;
import org.springframework.beans.factory.support.RootBeanDefinition;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.EmbeddedKafkaBrokerFactory;
import org.springframework.test.context.ContextCustomizer;
import org.springframework.test.context.MergedContextConfiguration;
import org.springframework.util.Assert;

/**
 * The {@link ContextCustomizer} implementation for the {@link EmbeddedKafkaBroker} bean registration.
 *
 * @author Artem Bilan
 * @author Elliot Metsger
 * @author Zach Olauson
 * @author Oleg Artyomov
 * @author Sergio Lourenco
 * @author Pawel Lozinski
 * @author Seonghwan Lee
 *
 * @since 1.3
 */
class EmbeddedKafkaContextCustomizer implements ContextCustomizer {

	private final EmbeddedKafka embeddedKafka;

	EmbeddedKafkaContextCustomizer(EmbeddedKafka embeddedKafka) {
		this.embeddedKafka = embeddedKafka;
	}

	@Override
	public void customizeContext(ConfigurableApplicationContext context, MergedContextConfiguration mergedConfig) {
		ConfigurableListableBeanFactory beanFactory = context.getBeanFactory();
		Assert.isInstanceOf(DefaultSingletonBeanRegistry.class, beanFactory);

		ConfigurableEnvironment environment = context.getEnvironment();

		EmbeddedKafkaBroker embeddedKafkaBroker =
				EmbeddedKafkaBrokerFactory.create(this.embeddedKafka, environment::resolvePlaceholders);

		((BeanDefinitionRegistry) beanFactory).registerBeanDefinition(EmbeddedKafkaBroker.BEAN_NAME,
				new RootBeanDefinition(EmbeddedKafkaBroker.class, () -> embeddedKafkaBroker));
	}

	@Override
	public int hashCode() {
		return this.embeddedKafka.hashCode();
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == null || obj.getClass() != getClass()) {
			return false;
		}
		EmbeddedKafkaContextCustomizer customizer = (EmbeddedKafkaContextCustomizer) obj;
		return this.embeddedKafka.equals(customizer.embeddedKafka);
	}

}

