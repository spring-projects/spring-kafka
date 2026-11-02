/*
 * Copyright 2018-present the original author or authors.
 */

package org.springframework.kafka.aot;

import org.apache.kafka.common.security.oauthbearer.DefaultJwtValidator;
import org.junit.jupiter.api.Test;

import org.springframework.aot.hint.MemberCategory;
import org.springframework.aot.hint.RuntimeHints;
import org.springframework.aot.hint.predicate.RuntimeHintsPredicates;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Wai Leong
 * @since 4.0.1
 */
class KafkaRuntimeHintsTest {

	@Test
	void shouldRegisterDefaultJwtValidatorRuntimeHints() {
		RuntimeHints hints = new RuntimeHints();
		new KafkaRuntimeHints().registerHints(hints, getClass().getClassLoader());
		assertThat(RuntimeHintsPredicates.reflection().onType(DefaultJwtValidator.class)
				.withMemberCategories(
						MemberCategory.INVOKE_DECLARED_CONSTRUCTORS,
						MemberCategory.INVOKE_DECLARED_METHODS))
				.accepts(hints);
	}
}
