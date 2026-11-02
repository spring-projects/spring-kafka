/*
 * Copyright 2017-present the original author or authors.
 */

package org.springframework.kafka.test.context;

import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.EmbeddedKafkaBrokerFactory;
import org.springframework.test.context.ContextCustomizer;
import org.springframework.test.context.MergedContextConfiguration;

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
		ConfigurableEnvironment environment = context.getEnvironment();

		EmbeddedKafkaBroker embeddedKafkaBroker =
				EmbeddedKafkaBrokerFactory.create(this.embeddedKafka, environment::resolvePlaceholders);

		GenericApplicationContext genericApplicationContext = (GenericApplicationContext) context;

		genericApplicationContext.registerBean(EmbeddedKafkaBroker.BEAN_NAME,
				EmbeddedKafkaBroker.class, () -> embeddedKafkaBroker);
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

