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

package org.springframework.kafka.listener;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

/**
 * @author Gary Russell
 * @since 2.8.4
 *
 */
public class ExceptionClassifierTests {

	@Test
	void testDefault() {
		ExceptionClassifier ec = new ExceptionClassifier() {
		};
		assertThat(ec.getClassifier().classify(new Exception())).isTrue();
		assertThat(ec.getClassifier().classify(new ClassCastException())).isFalse();
		ec.removeClassification(ClassCastException.class);
		assertThat(ec.getClassifier().classify(new ClassCastException())).isTrue();
		assertThat(ec.getClassifier().classify(new IllegalStateException())).isTrue();
		ec.addNotRetryableExceptions(IllegalStateException.class);
		assertThat(ec.getClassifier().classify(new IllegalStateException())).isFalse();
	}

	@Test
	void testDefaultFalse() {
		ExceptionClassifier ec = new ExceptionClassifier() {
		};
		assertThat(ec.getClassifier().classify(new Exception())).isTrue();
		ec.defaultFalse();
		assertThat(ec.getClassifier().classify(new Exception())).isFalse();
		assertThat(ec.getClassifier().classify(new IllegalStateException())).isFalse();
		ec.addRetryableExceptions(IllegalStateException.class);
		assertThat(ec.getClassifier().classify(new IllegalStateException())).isTrue();
		ec.removeClassification(IllegalStateException.class);
		assertThat(ec.getClassifier().classify(new IllegalStateException())).isFalse();
	}

}
