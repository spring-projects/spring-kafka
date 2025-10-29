/*
 * Copyright 2025-present the original author or authors.
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

package org.springframework.kafka.core;

import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.UUID;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;

import javax.tools.JavaCompiler;
import javax.tools.ToolProvider;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import org.springframework.context.support.GenericApplicationContext;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.condition.EmbeddedKafkaCondition;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * Reproducer for GH:
 * https://github.com/spring-projects/spring-kafka/issues/4109 Tests the
 * class-loading behavior when a SASL callback handler class is packaged with
 * the application
 *
 * @author Alexandros Papadakis
 */
@EmbeddedKafka(topics = { KafkaTemplateTests.INT_KEY_TOPIC, KafkaTemplateTests.STRING_KEY_TOPIC })
class KafkaSaslHandlerClassloadingTest {

	public static final String INT_KEY_TOPIC = "intKeyTopic";

	public static final String STRING_KEY_TOPIC = "stringKeyTopic";

	private static EmbeddedKafkaBroker embeddedKafka;

	private static final String FQCN = "xxx.yyy.kafka.auth.XyzAuthenticateCallbackHandler";

	private static final String SOURCE = "package xxx.yyy.kafka.auth;\n" + "import javax.security.auth.callback.*;\n"
			+ "import java.util.List;\n" + "import java.util.Map;\n"
			+ "import javax.security.auth.login.AppConfigurationEntry;\n"
			+ "import org.apache.kafka.common.security.auth.AuthenticateCallbackHandler;\n"
			+ "public class XyzAuthenticateCallbackHandler implements AuthenticateCallbackHandler {\n"
			+ "  @Override public void configure(Map<String, ?> configs, String mechanism, List<AppConfigurationEntry> jaasConfigEntries) { }\n"
			+ "  @Override public void handle(Callback[] callbacks) throws UnsupportedCallbackException { }\n"
			+ "  @Override public void close() { }\n" + "}\n";

	/**
	 * Build a tiny jar that contains only the callback handler class; return the
	 * child ClassLoader that can see it.
	 */
	private URLClassLoader makeChildLoaderWithHandler(@TempDir Path tempDir) throws Exception {
		// 1) write source
		Path srcDir = tempDir.resolve("src/xxx/yyy/kafka/auth");
		Files.createDirectories(srcDir);
		Path javaFile = srcDir.resolve("XyzAuthenticateCallbackHandler.java");
		Files.writeString(javaFile, SOURCE);

		// 2) compile using current test classpath
		JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
		assertThat(compiler).as("Tests must run on a JDK (not a JRE) to get the JavaCompiler").isNotNull();

		Path classesDir = tempDir.resolve("classes");
		Files.createDirectories(classesDir);
		int rc = compiler.run(null, null, null, "-classpath", System.getProperty("java.class.path"), "-d",
				classesDir.toString(), javaFile.toString());
		assertThat(rc).as("Compilation failed").isZero();

		// 3) jar it
		Path jar = tempDir.resolve("handler.jar");
		try (JarOutputStream jos = new JarOutputStream(Files.newOutputStream(jar))) {
			String entryName = "xxx/yyy/kafka/auth/XyzAuthenticateCallbackHandler.class";
			jos.putNextEntry(new JarEntry(entryName));
			byte[] bytes = Files.readAllBytes(classesDir.resolve(entryName));
			jos.write(bytes);
			jos.closeEntry();
		}

		// 4) child loader that can see ONLY the handler jar (parent is the current TCCL
		// for Kafka & Spring classes)
		URL[] urls = new URL[] { jar.toUri().toURL() };
		return new URLClassLoader(urls, Thread.currentThread().getContextClassLoader());
	}

	@BeforeAll
	public static void setUp() {
		embeddedKafka = EmbeddedKafkaCondition.getBroker();
	}

	@AfterAll
	public static void tearDown() {
	}

	private Map<String, Object> producerProps() {
		Map<String, Object> props = KafkaTestUtils.consumerProps(embeddedKafka,
				"KafkaSaslHandlerTests" + UUID.randomUUID(), false);
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		// SASL wiring to reach the callback-handler parsing path
		props.put("security.protocol", "SASL_SSL");
		props.put("sasl.login.callback.handler.class", FQCN);
		props.put("sasl.jaas.config", "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required;");
		props.put("sasl.mechanism", "OAUTHBEARER");
		return props;
	}

	@Test
	void reproduces_failure_when_handler_is_only_visible_to_child_loader(@TempDir Path temp) throws Exception {
		URLClassLoader child = makeChildLoaderWithHandler(temp);

		// TCCL remains the parent (cannot see the handler class)
		ClassLoader original = Thread.currentThread().getContextClassLoader();
		try {
			Thread.currentThread().setContextClassLoader(original); // explicit for clarity

			Map<String, Object> props = producerProps();

			DefaultKafkaProducerFactory<String, String> factory = new DefaultKafkaProducerFactory<>(props);

			Throwable thrown = catchThrowable(factory::createProducer);
			assertThat(thrown).as("Expected class-loading failure when only the child loader has the handler")
					.isInstanceOf(ConfigException.class);

			assertThat(thrown.getMessage()).as("Exception message should indicate missing class")
					.matches("(?s).*could not be found.*|(?s).*cannot be found.*");

			thrown.printStackTrace();
		}
		finally {
			Thread.currentThread().setContextClassLoader(original);
			child.close();
		}
	}

	@Test
	void succeeds_when_fix_provides_Class_instance_from_correct_loader(@TempDir Path temp) throws Exception {
		URLClassLoader appCtxLoader = makeChildLoaderWithHandler(temp);

		ClassLoader original = Thread.currentThread().getContextClassLoader();
		try {
			// TCCL still can't see the class (remains parent)
			Thread.currentThread().setContextClassLoader(original);

			Map<String, Object> props = producerProps();

			// Create an ApplicationContext whose ClassLoader == appCtxLoader
			try (GenericApplicationContext ctx = new GenericApplicationContext()) {
				ctx.setClassLoader(appCtxLoader); // <-- critical: handler only here
				// Register the factory as a bean so it receives the ApplicationContext
				ctx.registerBean(DefaultKafkaProducerFactory.class, () -> new DefaultKafkaProducerFactory<>(props));
				ctx.refresh();

				DefaultKafkaProducerFactory<?, ?> factory = ctx.getBean(DefaultKafkaProducerFactory.class);

				assertThatCode(factory::createProducer)
						.as("Passing a Class<?> bypasses name lookups and should succeed").doesNotThrowAnyException();
			}
		}
		finally {
			Thread.currentThread().setContextClassLoader(original);
			appCtxLoader.close();
		}
	}
}
