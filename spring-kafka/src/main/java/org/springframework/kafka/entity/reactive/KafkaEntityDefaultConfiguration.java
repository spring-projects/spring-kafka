/*
 * Copyright 2016-2025 the original author or authors.
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

package org.springframework.kafka.entity.reactive;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import org.apache.commons.logging.LogFactory;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ListTopicsResult;

import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.support.DefaultSingletonBeanRegistry;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.log.LogAccessor;
import org.springframework.kafka.entity.KafkaEntity;
import org.springframework.kafka.entity.KafkaEntityException;
import org.springframework.kafka.entity.KafkaEntityKey;
import org.springframework.kafka.entity.KafkaEntityUtil;

/**
 * This Bean creates and registers all KafkaEntity Bean at starting a
 * spring-boot app.
 *
 * @author Popovics Boglarka
 */
@Configuration
public class KafkaEntityDefaultConfiguration implements InitializingBean, DisposableBean {

	private final LogAccessor logger = new LogAccessor(LogFactory.getLog(getClass()));

	private List<String> bootstrapServers = new ArrayList<>(Collections.singletonList("localhost:9092"));

	private ApplicationContext applicationContext;

	private Set<String> beanNames = new HashSet<>();

	/**
	 * Use this constructor if you want to create an instance manually.
	 *
	 * @param applicationContext spring applicationcontext
	 * @param bootstrapServers   kafka bootstrap URL, default is looking at spirng
	 *                           application property
	 *                           spring.kafka.bootstrap-servers, if it is empty than
	 *                           "localhost:9092"
	 */
	public KafkaEntityDefaultConfiguration(@Autowired ApplicationContext applicationContext,
			@Value("${spring.kafka.bootstrap-servers:localhost:9092}") List<String> bootstrapServers) {
		super();
		this.applicationContext = applicationContext;
		if (bootstrapServers != null && !bootstrapServers.isEmpty()) {
			this.bootstrapServers = Collections.unmodifiableList(bootstrapServers);
		}
		this.logger.warn("bootstrapServers: " + bootstrapServers);
	}

	private List<KafkaEntityException> errors = new ArrayList<>();

	@Override
	public void afterPropertiesSet() {

		StringBuilder result = new StringBuilder();
		String[] allBeans = this.applicationContext.getBeanDefinitionNames();
		DefaultSingletonBeanRegistry registry = (DefaultSingletonBeanRegistry) this.applicationContext
				.getAutowireCapableBeanFactory();
		for (String beanName : allBeans) {
			result.append(beanName).append("\n");

			if ("kafkaEntityDefaultConfiguration".equals(beanName)) {
				continue;
			}

			Object bean = this.applicationContext.getBean(beanName);
			if (this.logger.isTraceEnabled()) {
				this.logger.trace(" bean -> " + bean.getClass());
			}
			for (Field field : bean.getClass().getDeclaredFields()) {
				if (this.logger.isTraceEnabled()) {
					this.logger.trace("    field  -> " + field.getName());
				}
				try {

					if (field.isAnnotationPresent(KafkaEntityPublisher.class)
							|| field.isAnnotationPresent(KafkaEntitySubscriber.class)
							|| field.isAnnotationPresent(KafkaEntityProcessor.class)) {
						String newBeanName = bean.getClass().getName() + "#" + field.getName();
						Class<?> entity = (Class<?>) ((ParameterizedType) field.getGenericType()).getActualTypeArguments()[0];
						if (!registry.containsSingleton(newBeanName)) {
							DisposableBean newInstance = null;
							if (field.isAnnotationPresent(KafkaEntityPublisher.class)) {

								KafkaEntityPublisher kafkaEntityPublisher = field
										.getAnnotation(KafkaEntityPublisher.class);

								newInstance = registerKafkaEntityPublisherBean(bean, entity, newBeanName,
										kafkaEntityPublisher);

							}
							else if (field.isAnnotationPresent(KafkaEntitySubscriber.class)) {
								KafkaEntitySubscriber kafkaEntitySubscriber = field
										.getAnnotation(KafkaEntitySubscriber.class);

								newInstance = registerKafkaEntitySubscriberBean(bean, entity, newBeanName, kafkaEntitySubscriber);

							}
							else if (field.isAnnotationPresent(KafkaEntityProcessor.class)) {
								KafkaEntityProcessor kafkaEntityProcessor = field.getAnnotation(KafkaEntityProcessor.class);

								newInstance = registerKafkaEntityProcessorBean(bean, entity, newBeanName, kafkaEntityProcessor);
							}
							registerBean(registry, bean, field, newBeanName, newInstance);
						}
					}
				}
				catch (KafkaEntityException e) {
					this.logger.error(e, "Error by registering Kafka Entity Bean");
					this.errors.add(e);
				}
				catch (Exception e) {
					KafkaEntityException kafkaEx = new KafkaEntityException(beanName, e);
					this.logger.error(kafkaEx, "Error by registering Kafka Entity Bean");
					this.errors.add(kafkaEx);
				}
			}
		}
		String string = result.toString();
		if (this.logger.isTraceEnabled()) {
			this.logger.trace("postConstruct-getAllBeans(): " + string);
		}

	}

	private DisposableBean registerKafkaEntityPublisherBean(Object bean, Class<?> entity, String newBeanName,
			KafkaEntityPublisher kafkaEntityPublisher)
			throws KafkaEntityException, NoSuchMethodException, IllegalAccessException, InvocationTargetException {

		if (this.logger.isDebugEnabled()) {
			this.logger.debug("registering " + newBeanName + " as Publisher");
		}
		handleKafkaEntity(newBeanName, entity);

		Class<SimpleKafkaEntityPublisher> clazz = SimpleKafkaEntityPublisher.class;
		Method method = clazz.getMethod("create", List.class, KafkaEntityPublisher.class, Class.class, String.class);

		Object obj = method.invoke(null, this.bootstrapServers, kafkaEntityPublisher, entity, newBeanName);
		SimpleKafkaEntityPublisher<?, ?> newInstance = (SimpleKafkaEntityPublisher<?, ?>) obj;

		return newInstance;
	}

	private DisposableBean registerKafkaEntitySubscriberBean(Object bean, Class<?> entity, String newBeanName,
			KafkaEntitySubscriber kafkaEntitySubscriber)
			throws KafkaEntityException, NoSuchMethodException, IllegalAccessException, InvocationTargetException {

		if (this.logger.isDebugEnabled()) {
			this.logger.debug("registering " + newBeanName + " as Subscriber");
		}
		handleKafkaEntity(newBeanName, entity);

		Class<SimpleKafkaEntitySubscriber> clazz = SimpleKafkaEntitySubscriber.class;
		Method method = clazz.getMethod("create", List.class, KafkaEntitySubscriber.class, Class.class, String.class);

		Object obj = method.invoke(null, this.bootstrapServers, kafkaEntitySubscriber, entity, newBeanName);
		SimpleKafkaEntitySubscriber<?, ?> newInstance = (SimpleKafkaEntitySubscriber<?, ?>) obj;

		return newInstance;
	}

	private DisposableBean registerKafkaEntityProcessorBean(Object bean, Class<?> entity, String newBeanName,
			KafkaEntityProcessor kafkaEntityProcessor)
			throws KafkaEntityException, NoSuchMethodException, IllegalAccessException, InvocationTargetException {

		if (this.logger.isDebugEnabled()) {
			this.logger.debug("registering " + newBeanName + " as Processor");
		}
		handleKafkaEntity(newBeanName, entity);

		Class<SimpleKafkaEntityProcessor> clazz = SimpleKafkaEntityProcessor.class;
		Method method = clazz.getMethod("create", List.class, KafkaEntityProcessor.class, Class.class, String.class);

		Object obj = method.invoke(null, this.bootstrapServers, kafkaEntityProcessor, entity, newBeanName);
		SimpleKafkaEntityProcessor<?, ?> newInstance = (SimpleKafkaEntityProcessor<?, ?>) obj;

		return newInstance;
	}

	private void registerBean(DefaultSingletonBeanRegistry registry, Object bean, Field field, String newBeanName,
			DisposableBean newInstance) throws IllegalAccessException {
		registry.registerSingleton(newBeanName, newInstance);
		registry.registerDisposableBean(newBeanName, newInstance);
		field.setAccessible(true);
		field.set(bean, newInstance);
		this.beanNames.add(newBeanName);
	}

	private void handleKafkaEntity(String beanName, Class<?> entity) throws KafkaEntityException {
		if (!entity.isAnnotationPresent(KafkaEntity.class)) {
			throw new KafkaEntityException(beanName, entity.getName() + " must be a @KafkaEntity");
		}

		boolean foundKeyAnnotation = false;
		for (Field field : entity.getDeclaredFields()) {
			if (this.logger.isTraceEnabled()) {
				this.logger.trace("    field  -> " + field.getName());
			}
			if (field.isAnnotationPresent(KafkaEntityKey.class)) {
				foundKeyAnnotation = true;
				break;
			}
		}

		if (!foundKeyAnnotation) {
			throw new KafkaEntityException(beanName, entity.getName() + " @" + KafkaEntityKey.class.getSimpleName()
					+ " is mandatory in @" + KafkaEntity.class.getSimpleName());
		}

		try {
			checkTopic(entity);
		}
		catch (InterruptedException | ExecutionException e) {
			throw new KafkaEntityException(beanName, e);
		}
	}

	private void checkTopic(Class<?> entity) throws InterruptedException, ExecutionException, KafkaEntityException {
		Map<String, Object> conf = new HashMap<>();
		conf.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, this.bootstrapServers);
		conf.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "5000");
		try (AdminClient admin = AdminClient.create(conf);) {

			ListTopicsResult listTopics = admin.listTopics();
			Set<String> names = listTopics.names().get();
			if (this.logger.isTraceEnabled()) {
				this.logger.trace("names: " + names);
			}
			String topicName = KafkaEntityUtil.getTopicName(entity);
			boolean contains = names.contains(topicName);
			if (!contains) {
				throw new KafkaEntityException(topicName,
						"Topic " + topicName + " does not exist in " + this.bootstrapServers);
			}
		}
	}

	/**
	 * To get the exceptions, which were created while afterPropertiesSet() .
	 * @return list of KafkaEntityException
	 */
	public List<KafkaEntityException> getErrors() {
		return Collections.unmodifiableList(this.errors);
	}

	/**
	 * It throws the first error, which happenened at afterPropertiesSet() if any
	 * happpened.
	 * @throws KafkaEntityException the first exception
	 */
	public void throwFirstError() throws KafkaEntityException {
		if (!this.errors.isEmpty()) {
			throw this.errors.get(0);
		}
	}

	@Override
	public void destroy() throws Exception {

		DefaultSingletonBeanRegistry registry = (DefaultSingletonBeanRegistry) this.applicationContext
				.getAutowireCapableBeanFactory();

		this.beanNames.stream().forEach(beanName -> {
			if (this.logger.isDebugEnabled()) {
				this.logger.debug("destroying " + beanName);
			}
			registry.destroySingleton(beanName);
		});
	}

}
