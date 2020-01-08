/*
 * Copyright 2017-2020 the original author or authors.
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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.utils.KafkaTestUtils;


/**
 * @author Oleg Artyomov
 * @author Sergio Lourenco
 * @author Artem Bilan
 * @author Gary Russell
 *
 * @since 1.3
 */
public class EmbeddedKafkaContextCustomizerTests {

	private EmbeddedKafka annotationFromFirstClass;

	private EmbeddedKafka annotationFromSecondClass;

	@BeforeEach
	void beforeEachTest() {
		annotationFromFirstClass = AnnotationUtils.findAnnotation(TestWithEmbeddedKafka.class, EmbeddedKafka.class);
		annotationFromSecondClass =
				AnnotationUtils.findAnnotation(SecondTestWithEmbeddedKafka.class, EmbeddedKafka.class);
	}


	@Test
	void testHashCode() {
		assertThat(new EmbeddedKafkaContextCustomizer(annotationFromFirstClass).hashCode()).isNotEqualTo(0);
		assertThat(new EmbeddedKafkaContextCustomizer(annotationFromFirstClass).hashCode())
				.isEqualTo(new EmbeddedKafkaContextCustomizer(annotationFromSecondClass).hashCode());
	}


	@Test
	void testEquals() {
		assertThat(new EmbeddedKafkaContextCustomizer(annotationFromFirstClass))
				.isEqualTo(new EmbeddedKafkaContextCustomizer(annotationFromSecondClass));
		assertThat(new EmbeddedKafkaContextCustomizer(annotationFromFirstClass)).isNotEqualTo(new Object());
	}

	@Test
	void testPorts() {
		EmbeddedKafka annotationWithPorts =
				AnnotationUtils.findAnnotation(TestWithEmbeddedKafkaPorts.class, EmbeddedKafka.class);
		EmbeddedKafkaContextCustomizer customizer = new EmbeddedKafkaContextCustomizer(annotationWithPorts);
		ConfigurableApplicationContext context = mock(ConfigurableApplicationContext.class);
		BeanFactoryStub factoryStub = new BeanFactoryStub();
		given(context.getBeanFactory()).willReturn(factoryStub);
		given(context.getEnvironment()).willReturn(mock(ConfigurableEnvironment.class));
		customizer.customizeContext(context, null);

		assertThat(factoryStub.getBroker().getBrokersAsString())
				.isEqualTo("127.0.0.1:" + annotationWithPorts.ports()[0]);
		assertThat(KafkaTestUtils.getPropertyValue(factoryStub.getBroker(), "brokerListProperty"))
				.isEqualTo("my.bss.prop");
	}

	@Test
	void testMulti() {
		EmbeddedKafka annotationWithPorts =
				AnnotationUtils.findAnnotation(TestWithEmbeddedKafkaMulti.class, EmbeddedKafka.class);
		EmbeddedKafkaContextCustomizer customizer = new EmbeddedKafkaContextCustomizer(annotationWithPorts);
		ConfigurableApplicationContext context = mock(ConfigurableApplicationContext.class);
		BeanFactoryStub factoryStub = new BeanFactoryStub();
		given(context.getBeanFactory()).willReturn(factoryStub);
		given(context.getEnvironment()).willReturn(mock(ConfigurableEnvironment.class));
		customizer.customizeContext(context, null);

		assertThat(factoryStub.getBroker().getBrokersAsString())
			.matches("127.0.0.1:[0-9]+,127.0.0.1:[0-9]+");
	}


	@EmbeddedKafka
	private class TestWithEmbeddedKafka {

	}

	@EmbeddedKafka
	private class SecondTestWithEmbeddedKafka {

	}

	@EmbeddedKafka(ports = 8085, bootstrapServersProperty = "my.bss.prop")
	private class TestWithEmbeddedKafkaPorts {

	}

	@EmbeddedKafka(count = 2)
	private class TestWithEmbeddedKafkaMulti {

	}
	@SuppressWarnings("serial")
	private class BeanFactoryStub extends DefaultListableBeanFactory {

		private Object bean;

		public EmbeddedKafkaBroker getBroker() {
			return (EmbeddedKafkaBroker) bean;
		}

		@Override
		public Object initializeBean(Object existingBean, String beanName) throws BeansException {
			this.bean = existingBean;
			return bean;
		}

		@Override
		public void registerSingleton(String beanName, Object singletonObject) {

		}

		@Override
		public void registerDisposableBean(String beanName, DisposableBean bean) {

		}

	}

}
