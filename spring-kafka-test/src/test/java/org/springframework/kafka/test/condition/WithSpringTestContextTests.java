/*
 * Copyright and licensing information for this source code can be found in
 * the NOTICE and LICENSE files distributed with this project.
 */

package org.springframework.kafka.test.condition;

import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Gary Russell
 * @since 2.7.2
 *
 */
@SpringJUnitConfig
@EmbeddedKafka
@DirtiesContext
public class WithSpringTestContextTests {

	@Test
	void canAutowireBrokerInMethod(@Autowired EmbeddedKafkaBroker broker) {
		assertThat(broker).isNotNull();
	}

	@Configuration
	static class Config {

	}

}
