/*
 * Copyright 2017 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.kafka.test.context;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.then;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.withSettings;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.beans.factory.support.DefaultSingletonBeanRegistry;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.test.context.MergedContextConfiguration;
import org.springframework.util.Assert;

/**
 * Provides unit tests for {@link EmbeddedKafkaContextCustomizer}
 *
 * @author Elliot Metsger (emetsger@jhu.edu)
 */
public class EmbeddedKafkaContextCustomizerTest {

	/**
	 * Insures that for each property supplied by {@link EmbeddedKafka#brokerProperties()}, {@link
	 * org.springframework.core.env.PropertyResolver#resolvePlaceholders(String)} is invoked.
	 * <p>
	 * Because {@code EmbeddedKafkaContextCustomizer} encapsulates the creation of {@link
	 * org.springframework.kafka.test.rule.KafkaEmbedded KafkaEmbedded},
	 * and because {@code KafkaEmbedded} does not expose much state, it is difficult to directly verify that a property
	 * string was resolved to a specific value.  Instead, this test insures that a call to {@code
	 * PropertyResolver#resolvePlaceholders(String)} occurs for each property string returned by {@code EmbeddedKafka
	 * #brokerProperties()}
	 * </p>
	 */
	@Test
	public void testBrokerPropertyResolution() {

		// Keys are the properties as they may appear in the @EmbeddedKafka brokerProperties attribute
		// Values are the result of applying the Spring PropertyResolver.resolvePlaceholders(...) method and are
		// currently ignored by this test
		Map<String, String> expectedResults = new HashMap<String, String>() {
			{
				put("property.key = ${property.value}", "property.key = resolvedValue");
				put("foo = bar", "foo = bar");
			}
		};

		// Expectations for EmbeddedKafka
		EmbeddedKafka embeddedKafka = mock(EmbeddedKafka.class);
		given(embeddedKafka.count()).willReturn(1);
		given(embeddedKafka.partitions()).willReturn(2);
		given(embeddedKafka.brokerProperties()).willReturn(
				new ArrayList<>(expectedResults.keySet()).toArray(new String[]{}));

		// Collaborators and expectations for the application context and environment.
		DefaultSingletonBeanRegistry beanFactory = mock(DefaultSingletonBeanRegistry.class,
				withSettings().extraInterfaces(ConfigurableListableBeanFactory.class));
		ConfigurableApplicationContext context = mock(ConfigurableApplicationContext.class);
		ConfigurableEnvironment env = mock(ConfigurableEnvironment.class);
		given(context.getEnvironment()).willReturn(env);
		given(context.getBeanFactory()).willReturn((ConfigurableListableBeanFactory) beanFactory);

		// Mock the property resolution behavior of PropertyResolver.resolvePlaceholders(...)
		// by looking up the resolved values from the map
		given(env.resolvePlaceholders(anyString()))
				.willAnswer(invocation -> {
					Object propertyString = invocation.getArgument(0);
					Assert.isTrue(expectedResults.containsKey(propertyString),
							"Unexpected property string '" + propertyString + "'"); // sanity
					return expectedResults.get(propertyString);
				});

		EmbeddedKafkaContextCustomizer underTest = new EmbeddedKafkaContextCustomizer(embeddedKafka);
		underTest.customizeContext(context, mock(MergedContextConfiguration.class));

		// Verify that an attempt was made to resolve each property string
		then(env).should(times(expectedResults.size())).resolvePlaceholders(anyString());

	}

}
