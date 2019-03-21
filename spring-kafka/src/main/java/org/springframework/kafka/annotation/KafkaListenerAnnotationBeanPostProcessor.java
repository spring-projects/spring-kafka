/*
 * Copyright 2014-2017 the original author or authors.
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

package org.springframework.kafka.annotation;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.springframework.aop.framework.Advised;
import org.springframework.aop.support.AopUtils;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanFactoryAware;
import org.springframework.beans.factory.BeanInitializationException;
import org.springframework.beans.factory.ListableBeanFactory;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.beans.factory.config.BeanExpressionContext;
import org.springframework.beans.factory.config.BeanExpressionResolver;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.context.expression.StandardBeanExpressionResolver;
import org.springframework.core.MethodIntrospector;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.format.support.DefaultFormattingConversionService;
import org.springframework.kafka.config.KafkaListenerConfigUtils;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerEndpointRegistrar;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.config.MethodKafkaListenerEndpoint;
import org.springframework.kafka.config.MultiMethodKafkaListenerEndpoint;
import org.springframework.kafka.support.KafkaNull;
import org.springframework.kafka.support.TopicPartitionInitialOffset;
import org.springframework.messaging.converter.GenericMessageConverter;
import org.springframework.messaging.handler.annotation.support.DefaultMessageHandlerMethodFactory;
import org.springframework.messaging.handler.annotation.support.HeaderMethodArgumentResolver;
import org.springframework.messaging.handler.annotation.support.HeadersMethodArgumentResolver;
import org.springframework.messaging.handler.annotation.support.MessageHandlerMethodFactory;
import org.springframework.messaging.handler.annotation.support.MessageMethodArgumentResolver;
import org.springframework.messaging.handler.annotation.support.PayloadArgumentResolver;
import org.springframework.messaging.handler.invocation.HandlerMethodArgumentResolver;
import org.springframework.messaging.handler.invocation.InvocableHandlerMethod;
import org.springframework.util.Assert;
import org.springframework.util.ReflectionUtils;
import org.springframework.util.StringUtils;

/**
 * Bean post-processor that registers methods annotated with {@link KafkaListener}
 * to be invoked by a Kafka message listener container created under the covers
 * by a {@link org.springframework.kafka.config.KafkaListenerContainerFactory}
 * according to the parameters of the annotation.
 *
 * <p>Annotated methods can use flexible arguments as defined by {@link KafkaListener}.
 *
 * <p>This post-processor is automatically registered by Spring's {@link EnableKafka}
 * annotation.
 *
 * <p>Auto-detect any {@link KafkaListenerConfigurer} instances in the container,
 * allowing for customization of the registry to be used, the default container
 * factory or for fine-grained control over endpoints registration. See
 * {@link EnableKafka} Javadoc for complete usage details.
 *
 * @param <K> the key type.
 * @param <V> the value type.
 *
 * @author Stephane Nicoll
 * @author Juergen Hoeller
 * @author Gary Russell
 * @author Artem Bilan
 * @author Dariusz Szablinski
 *
 * @see KafkaListener
 * @see EnableKafka
 * @see KafkaListenerConfigurer
 * @see KafkaListenerEndpointRegistrar
 * @see KafkaListenerEndpointRegistry
 * @see org.springframework.kafka.config.KafkaListenerEndpoint
 * @see MethodKafkaListenerEndpoint
 */
public class KafkaListenerAnnotationBeanPostProcessor<K, V>
		implements BeanPostProcessor, Ordered, BeanFactoryAware, SmartInitializingSingleton {

	/**
	 * The bean name of the default {@link org.springframework.kafka.config.KafkaListenerContainerFactory}.
	 */
	public static final String DEFAULT_KAFKA_LISTENER_CONTAINER_FACTORY_BEAN_NAME = "kafkaListenerContainerFactory";

	private final Set<Class<?>> nonAnnotatedClasses =
			Collections.newSetFromMap(new ConcurrentHashMap<Class<?>, Boolean>(64));

	private final Log logger = LogFactory.getLog(getClass());

	private KafkaListenerEndpointRegistry endpointRegistry;

	private String containerFactoryBeanName = DEFAULT_KAFKA_LISTENER_CONTAINER_FACTORY_BEAN_NAME;

	private BeanFactory beanFactory;

	private final KafkaHandlerMethodFactoryAdapter messageHandlerMethodFactory =
			new KafkaHandlerMethodFactoryAdapter();

	private final KafkaListenerEndpointRegistrar registrar = new KafkaListenerEndpointRegistrar();

	private final AtomicInteger counter = new AtomicInteger();

	private BeanExpressionResolver resolver = new StandardBeanExpressionResolver();

	private BeanExpressionContext expressionContext;

	@Override
	public int getOrder() {
		return LOWEST_PRECEDENCE;
	}

	/**
	 * Set the {@link KafkaListenerEndpointRegistry} that will hold the created
	 * endpoint and manage the lifecycle of the related listener container.
	 * @param endpointRegistry the {@link KafkaListenerEndpointRegistry} to set.
	 */
	public void setEndpointRegistry(KafkaListenerEndpointRegistry endpointRegistry) {
		this.endpointRegistry = endpointRegistry;
	}

	/**
	 * Set the name of the {@link KafkaListenerContainerFactory} to use by default.
	 * <p>If none is specified, "KafkaListenerContainerFactory" is assumed to be defined.
	 * @param containerFactoryBeanName the {@link KafkaListenerContainerFactory} bean name.
	 */
	public void setContainerFactoryBeanName(String containerFactoryBeanName) {
		this.containerFactoryBeanName = containerFactoryBeanName;
	}

	/**
	 * Set the {@link MessageHandlerMethodFactory} to use to configure the message
	 * listener responsible to serve an endpoint detected by this processor.
	 * <p>By default, {@link DefaultMessageHandlerMethodFactory} is used and it
	 * can be configured further to support additional method arguments
	 * or to customize conversion and validation support. See
	 * {@link DefaultMessageHandlerMethodFactory} Javadoc for more details.
	 * @param messageHandlerMethodFactory the {@link MessageHandlerMethodFactory} instance.
	 */
	public void setMessageHandlerMethodFactory(MessageHandlerMethodFactory messageHandlerMethodFactory) {
		this.messageHandlerMethodFactory.setMessageHandlerMethodFactory(messageHandlerMethodFactory);
	}

	/**
	 * Making a {@link BeanFactory} available is optional; if not set,
	 * {@link KafkaListenerConfigurer} beans won't get autodetected and an
	 * {@link #setEndpointRegistry endpoint registry} has to be explicitly configured.
	 * @param beanFactory the {@link BeanFactory} to be used.
	 */
	@Override
	public void setBeanFactory(BeanFactory beanFactory) {
		this.beanFactory = beanFactory;
		if (beanFactory instanceof ConfigurableListableBeanFactory) {
			this.resolver = ((ConfigurableListableBeanFactory) beanFactory).getBeanExpressionResolver();
			this.expressionContext = new BeanExpressionContext((ConfigurableListableBeanFactory) beanFactory, null);
		}
	}


	@Override
	public void afterSingletonsInstantiated() {
		this.registrar.setBeanFactory(this.beanFactory);

		if (this.beanFactory instanceof ListableBeanFactory) {
			Map<String, KafkaListenerConfigurer> instances =
					((ListableBeanFactory) this.beanFactory).getBeansOfType(KafkaListenerConfigurer.class);
			for (KafkaListenerConfigurer configurer : instances.values()) {
				configurer.configureKafkaListeners(this.registrar);
			}
		}

		if (this.registrar.getEndpointRegistry() == null) {
			if (this.endpointRegistry == null) {
				Assert.state(this.beanFactory != null,
						"BeanFactory must be set to find endpoint registry by bean name");
				this.endpointRegistry = this.beanFactory.getBean(
						KafkaListenerConfigUtils.KAFKA_LISTENER_ENDPOINT_REGISTRY_BEAN_NAME,
						KafkaListenerEndpointRegistry.class);
			}
			this.registrar.setEndpointRegistry(this.endpointRegistry);
		}

		if (this.containerFactoryBeanName != null) {
			this.registrar.setContainerFactoryBeanName(this.containerFactoryBeanName);
		}

		// Set the custom handler method factory once resolved by the configurer
		MessageHandlerMethodFactory handlerMethodFactory = this.registrar.getMessageHandlerMethodFactory();
		if (handlerMethodFactory != null) {
			this.messageHandlerMethodFactory.setMessageHandlerMethodFactory(handlerMethodFactory);
		}

		// Actually register all listeners
		this.registrar.afterPropertiesSet();
	}


	@Override
	public Object postProcessBeforeInitialization(Object bean, String beanName) throws BeansException {
		return bean;
	}

	@Override
	public Object postProcessAfterInitialization(final Object bean, final String beanName) throws BeansException {
		if (!this.nonAnnotatedClasses.contains(bean.getClass())) {
			Class<?> targetClass = AopUtils.getTargetClass(bean);
			Collection<KafkaListener> classLevelListeners = findListenerAnnotations(targetClass);
			final boolean hasClassLevelListeners = classLevelListeners.size() > 0;
			final List<Method> multiMethods = new ArrayList<Method>();
			Map<Method, Set<KafkaListener>> annotatedMethods = MethodIntrospector.selectMethods(targetClass,
					new MethodIntrospector.MetadataLookup<Set<KafkaListener>>() {

						@Override
						public Set<KafkaListener> inspect(Method method) {
							Set<KafkaListener> listenerMethods = findListenerAnnotations(method);
							return (!listenerMethods.isEmpty() ? listenerMethods : null);
						}

					});
			if (hasClassLevelListeners) {
				Set<Method> methodsWithHandler = MethodIntrospector.selectMethods(targetClass,
						new ReflectionUtils.MethodFilter() {

							@Override
							public boolean matches(Method method) {
								return AnnotationUtils.findAnnotation(method, KafkaHandler.class) != null;
							}

						});
				multiMethods.addAll(methodsWithHandler);
			}
			if (annotatedMethods.isEmpty()) {
				this.nonAnnotatedClasses.add(bean.getClass());
				if (this.logger.isTraceEnabled()) {
					this.logger.trace("No @KafkaListener annotations found on bean type: " + bean.getClass());
				}
			}
			else {
				// Non-empty set of methods
				for (Map.Entry<Method, Set<KafkaListener>> entry : annotatedMethods.entrySet()) {
					Method method = entry.getKey();
					for (KafkaListener listener : entry.getValue()) {
						processKafkaListener(listener, method, bean, beanName);
					}
				}
				if (this.logger.isDebugEnabled()) {
					this.logger.debug(annotatedMethods.size() + " @KafkaListener methods processed on bean '"
							+ beanName + "': " + annotatedMethods);
				}
			}
			if (hasClassLevelListeners) {
				processMultiMethodListeners(classLevelListeners, multiMethods, bean, beanName);
			}
		}
		return bean;
	}

	/*
	 * AnnotationUtils.getRepeatableAnnotations does not look at interfaces
	 */
	private Collection<KafkaListener> findListenerAnnotations(Class<?> clazz) {
		Set<KafkaListener> listeners = new HashSet<KafkaListener>();
		KafkaListener ann = AnnotationUtils.findAnnotation(clazz, KafkaListener.class);
		if (ann != null) {
			listeners.add(ann);
		}
		KafkaListeners anns = AnnotationUtils.findAnnotation(clazz, KafkaListeners.class);
		if (anns != null) {
			listeners.addAll(Arrays.asList(anns.value()));
		}
		return listeners;
	}

	/*
	 * AnnotationUtils.getRepeatableAnnotations does not look at interfaces
	 */
	private Set<KafkaListener> findListenerAnnotations(Method method) {
		Set<KafkaListener> listeners = new HashSet<KafkaListener>();
		KafkaListener ann = AnnotationUtils.findAnnotation(method, KafkaListener.class);
		if (ann != null) {
			listeners.add(ann);
		}
		KafkaListeners anns = AnnotationUtils.findAnnotation(method, KafkaListeners.class);
		if (anns != null) {
			listeners.addAll(Arrays.asList(anns.value()));
		}
		return listeners;
	}

	private void processMultiMethodListeners(Collection<KafkaListener> classLevelListeners, List<Method> multiMethods,
			Object bean, String beanName) {
		List<Method> checkedMethods = new ArrayList<Method>();
		for (Method method : multiMethods) {
			checkedMethods.add(checkProxy(method, bean));
		}
		for (KafkaListener classLevelListener : classLevelListeners) {
			MultiMethodKafkaListenerEndpoint<K, V> endpoint = new MultiMethodKafkaListenerEndpoint<K, V>(checkedMethods,
					bean);
			endpoint.setBeanFactory(this.beanFactory);
			processListener(endpoint, classLevelListener, bean, bean.getClass(), beanName);
		}
	}

	protected void processKafkaListener(KafkaListener kafkaListener, Method method, Object bean, String beanName) {
		Method methodToUse = checkProxy(method, bean);
		MethodKafkaListenerEndpoint<K, V> endpoint = new MethodKafkaListenerEndpoint<K, V>();
		endpoint.setMethod(methodToUse);
		endpoint.setBeanFactory(this.beanFactory);
		processListener(endpoint, kafkaListener, bean, methodToUse, beanName);
	}

	private Method checkProxy(Method methodArg, Object bean) {
		Method method = methodArg;
		if (AopUtils.isJdkDynamicProxy(bean)) {
			try {
				// Found a @KafkaListener method on the target class for this JDK proxy ->
				// is it also present on the proxy itself?
				method = bean.getClass().getMethod(method.getName(), method.getParameterTypes());
				Class<?>[] proxiedInterfaces = ((Advised) bean).getProxiedInterfaces();
				for (Class<?> iface : proxiedInterfaces) {
					try {
						method = iface.getMethod(method.getName(), method.getParameterTypes());
						break;
					}
					catch (NoSuchMethodException noMethod) {
					}
				}
			}
			catch (SecurityException ex) {
				ReflectionUtils.handleReflectionException(ex);
			}
			catch (NoSuchMethodException ex) {
				throw new IllegalStateException(String.format(
						"@KafkaListener method '%s' found on bean target class '%s', " +
						"but not found in any interface(s) for bean JDK proxy. Either " +
						"pull the method up to an interface or switch to subclass (CGLIB) " +
						"proxies by setting proxy-target-class/proxyTargetClass " +
						"attribute to 'true'", method.getName(), method.getDeclaringClass().getSimpleName()), ex);
			}
		}
		return method;
	}

	protected void processListener(MethodKafkaListenerEndpoint<?, ?> endpoint, KafkaListener kafkaListener, Object bean,
			Object adminTarget, String beanName) {
		endpoint.setBean(bean);
		endpoint.setMessageHandlerMethodFactory(this.messageHandlerMethodFactory);
		endpoint.setId(getEndpointId(kafkaListener));
		endpoint.setTopicPartitions(resolveTopicPartitions(kafkaListener));
		endpoint.setTopics(resolveTopics(kafkaListener));
		endpoint.setTopicPattern(resolvePattern(kafkaListener));
		String group = kafkaListener.group();
		if (StringUtils.hasText(group)) {
			Object resolvedGroup = resolveExpression(group);
			if (resolvedGroup instanceof String) {
				endpoint.setGroup((String) resolvedGroup);
			}
		}

		KafkaListenerContainerFactory<?> factory = null;
		String containerFactoryBeanName = resolve(kafkaListener.containerFactory());
		if (StringUtils.hasText(containerFactoryBeanName)) {
			Assert.state(this.beanFactory != null, "BeanFactory must be set to obtain container factory by bean name");
			try {
				factory = this.beanFactory.getBean(containerFactoryBeanName, KafkaListenerContainerFactory.class);
			}
			catch (NoSuchBeanDefinitionException ex) {
				throw new BeanInitializationException("Could not register Kafka listener endpoint on [" + adminTarget
						+ "] for bean " + beanName + ", no " + KafkaListenerContainerFactory.class.getSimpleName()
						+ " with id '" + containerFactoryBeanName + "' was found in the application context", ex);
			}
		}

		this.registrar.registerEndpoint(endpoint, factory);
	}

	private String getEndpointId(KafkaListener kafkaListener) {
		if (StringUtils.hasText(kafkaListener.id())) {
			return resolve(kafkaListener.id());
		}
		else {
			return "org.springframework.kafka.KafkaListenerEndpointContainer#" + this.counter.getAndIncrement();
		}
	}

	private TopicPartitionInitialOffset[] resolveTopicPartitions(KafkaListener kafkaListener) {
		TopicPartition[] topicPartitions = kafkaListener.topicPartitions();
		List<TopicPartitionInitialOffset> result = new ArrayList<>();
		if (topicPartitions.length > 0) {
			for (TopicPartition topicPartition : topicPartitions) {
				result.addAll(resolveTopicPartitionsList(topicPartition));
			}
		}
		return result.toArray(new TopicPartitionInitialOffset[result.size()]);
	}

	private String[] resolveTopics(KafkaListener kafkaListener) {
		String[] topics = kafkaListener.topics();
		List<String> result = new ArrayList<>();
		if (topics.length > 0) {
			for (int i = 0; i < topics.length; i++) {
				Object topic = resolveExpression(topics[i]);
				resolveAsString(topic, result);
			}
		}
		return result.toArray(new String[result.size()]);
	}

	private Pattern resolvePattern(KafkaListener kafkaListener) {
		Pattern pattern = null;
		String text = kafkaListener.topicPattern();
		if (StringUtils.hasText(text)) {
			Object resolved = resolveExpression(text);
			if (resolved instanceof Pattern) {
				pattern = (Pattern) resolved;
			}
			else if (resolved instanceof String) {
				pattern = Pattern.compile((String) resolved);
			}
			else {
				throw new IllegalStateException(
						"topicPattern must resolve to a Pattern or String, not " + resolved.getClass());
			}
		}
		return pattern;
	}

	private List<TopicPartitionInitialOffset> resolveTopicPartitionsList(TopicPartition topicPartition) {
		Object topic = resolveExpression(topicPartition.topic());
		Assert.state(topic instanceof String,
				"topic in @TopicPartition must resolve to a String, not " + topic.getClass());
		Assert.state(StringUtils.hasText((String) topic), "topic in @TopicPartition must not be empty");
		String[] partitions = topicPartition.partitions();
		PartitionOffset[] partitionOffsets = topicPartition.partitionOffsets();
		Assert.state(partitions.length > 0 || partitionOffsets.length > 0,
				"At least one 'partition' or 'partitionOffset' required in @TopicPartition for topic '" + topic + "'");
		List<TopicPartitionInitialOffset> result = new ArrayList<>();
		for (int i = 0; i < partitions.length; i++) {
			resolvePartitionAsInteger((String) topic, resolveExpression(partitions[i]), result);
		}

		for (PartitionOffset partitionOffset : partitionOffsets) {
			Object partitionValue = resolveExpression(partitionOffset.partition());
			Integer partition;
			if (partitionValue instanceof String) {
				Assert.state(StringUtils.hasText((String) partitionValue),
						"partition in @PartitionOffset for topic '" + topic + "' cannot be empty");
				partition = Integer.valueOf((String) partitionValue);
			}
			else if (partitionValue instanceof Integer) {
				partition = (Integer) partitionValue;
			}
			else {
				throw new IllegalArgumentException(String.format(
						"@PartitionOffset for topic '%s' can't resolve '%s' as an Integer or String, resolved to '%s'",
							topic, partitionOffset.partition(), partitionValue.getClass()));
			}

			Object initialOffsetValue = resolveExpression(partitionOffset.initialOffset());
			Long initialOffset;
			if (initialOffsetValue instanceof String) {
				Assert.state(StringUtils.hasText((String) initialOffsetValue),
						"'initialOffset' in @PartitionOffset for topic '" + topic + "' cannot be empty");
				initialOffset = Long.valueOf((String) initialOffsetValue);
			}
			else if (initialOffsetValue instanceof Long) {
				initialOffset = (Long) initialOffsetValue;
			}
			else {
				throw new IllegalArgumentException(String.format(
						"@PartitionOffset for topic '%s' can't resolve '%s' as a Long or String, resolved to '%s'",
							topic, partitionOffset.initialOffset(), initialOffsetValue.getClass()));
			}

			Object relativeToCurrentValue = resolveExpression(partitionOffset.relativeToCurrent());
			Boolean relativeToCurrent;
			if (relativeToCurrentValue instanceof String) {
				relativeToCurrent = Boolean.valueOf((String) relativeToCurrentValue);
			}
			else if (relativeToCurrentValue instanceof Boolean) {
				relativeToCurrent = (Boolean) relativeToCurrentValue;
			}
			else {
				throw new IllegalArgumentException(String.format(
						"@PartitionOffset for topic '%s' can't resolve '%s' as a Boolean or String, resolved to '%s'",
							topic, partitionOffset.relativeToCurrent(), relativeToCurrentValue.getClass()));
			}

			TopicPartitionInitialOffset topicPartitionOffset =
					new TopicPartitionInitialOffset((String) topic, partition, initialOffset, relativeToCurrent);
			if (!result.contains(topicPartitionOffset)) {
				result.add(topicPartitionOffset);
			}
			else {
				throw new IllegalArgumentException(
						String.format("@TopicPartition can't have the same partition configuration twice: [%s]",
								topicPartitionOffset));
			}
		}
		return result;
	}

	@SuppressWarnings("unchecked")
	private void resolveAsString(Object resolvedValue, List<String> result) {
		if (resolvedValue instanceof String[]) {
			for (Object object : (String[]) resolvedValue) {
				resolveAsString(object, result);
			}
		}
		else if (resolvedValue instanceof String) {
			result.add((String) resolvedValue);
		}
		else if (resolvedValue instanceof Iterable) {
			for (Object object : (Iterable<Object>) resolvedValue) {
				resolveAsString(object, result);
			}
		}
		else {
			throw new IllegalArgumentException(String.format(
					"@KafKaListener can't resolve '%s' as a String", resolvedValue));
		}
	}

	@SuppressWarnings("unchecked")
	private void resolvePartitionAsInteger(String topic, Object resolvedValue,
			List<TopicPartitionInitialOffset> result) {
		if (resolvedValue instanceof String[]) {
			for (Object object : (String[]) resolvedValue) {
				resolvePartitionAsInteger(topic, object, result);
			}
		}
		else if (resolvedValue instanceof String) {
			Assert.state(StringUtils.hasText((String) resolvedValue),
					"partition in @TopicPartition for topic '" + topic + "' cannot be empty");
			result.add(new TopicPartitionInitialOffset(topic, Integer.valueOf((String) resolvedValue)));
		}
		else if (resolvedValue instanceof Integer[]) {
			for (Integer partition : (Integer[]) resolvedValue) {
				result.add(new TopicPartitionInitialOffset(topic, partition));
			}
		}
		else if (resolvedValue instanceof Integer) {
			result.add(new TopicPartitionInitialOffset(topic, (Integer) resolvedValue));
		}
		else if (resolvedValue instanceof Iterable) {
			for (Object object : (Iterable<Object>) resolvedValue) {
				resolvePartitionAsInteger(topic, object, result);
			}
		}
		else {
			throw new IllegalArgumentException(String.format(
					"@KafKaListener for topic '%s' can't resolve '%s' as an Integer or String", topic, resolvedValue));
		}
	}

	private Object resolveExpression(String value) {
		String resolvedValue = resolve(value);

		if (!(resolvedValue.startsWith("#{") && value.endsWith("}"))) {
			return resolvedValue;
		}

		return this.resolver.evaluate(resolvedValue, this.expressionContext);
	}

	/**
	 * Resolve the specified value if possible.
	 * @param value the value to resolve
	 * @return the resolved value
	 * @see ConfigurableBeanFactory#resolveEmbeddedValue
	 */
	private String resolve(String value) {
		if (this.beanFactory != null && this.beanFactory instanceof ConfigurableBeanFactory) {
			return ((ConfigurableBeanFactory) this.beanFactory).resolveEmbeddedValue(value);
		}
		return value;
	}

	/**
	 * An {@link MessageHandlerMethodFactory} adapter that offers a configurable underlying
	 * instance to use. Useful if the factory to use is determined once the endpoints
	 * have been registered but not created yet.
	 * @see KafkaListenerEndpointRegistrar#setMessageHandlerMethodFactory
	 */
	private class KafkaHandlerMethodFactoryAdapter implements MessageHandlerMethodFactory {

		private MessageHandlerMethodFactory messageHandlerMethodFactory;

		public void setMessageHandlerMethodFactory(MessageHandlerMethodFactory kafkaHandlerMethodFactory1) {
			this.messageHandlerMethodFactory = kafkaHandlerMethodFactory1;
		}

		@Override
		public InvocableHandlerMethod createInvocableHandlerMethod(Object bean, Method method) {
			return getMessageHandlerMethodFactory().createInvocableHandlerMethod(bean, method);
		}

		private MessageHandlerMethodFactory getMessageHandlerMethodFactory() {
			if (this.messageHandlerMethodFactory == null) {
				this.messageHandlerMethodFactory = createDefaultMessageHandlerMethodFactory();
			}
			return this.messageHandlerMethodFactory;
		}

		private MessageHandlerMethodFactory createDefaultMessageHandlerMethodFactory() {
			DefaultMessageHandlerMethodFactory defaultFactory = new DefaultMessageHandlerMethodFactory();
			defaultFactory.setBeanFactory(KafkaListenerAnnotationBeanPostProcessor.this.beanFactory);

			ConfigurableBeanFactory cbf =
					(KafkaListenerAnnotationBeanPostProcessor.this.beanFactory instanceof ConfigurableBeanFactory ?
							(ConfigurableBeanFactory) KafkaListenerAnnotationBeanPostProcessor.this.beanFactory : null);

			DefaultFormattingConversionService conversionService = new DefaultFormattingConversionService();
			defaultFactory.setConversionService(conversionService);

			List<HandlerMethodArgumentResolver> argumentResolvers = new ArrayList<>();

			// Annotation-based argument resolution
			argumentResolvers.add(new HeaderMethodArgumentResolver(conversionService, cbf));
			argumentResolvers.add(new HeadersMethodArgumentResolver());

			// Type-based argument resolution
			final GenericMessageConverter messageConverter = new GenericMessageConverter(conversionService);
			argumentResolvers.add(new MessageMethodArgumentResolver(messageConverter));
			argumentResolvers.add(new PayloadArgumentResolver(messageConverter) {

				@Override
				protected boolean isEmptyPayload(Object payload) {
					return payload == null || payload instanceof KafkaNull;
				}

			});
			defaultFactory.setArgumentResolvers(argumentResolvers);

			defaultFactory.afterPropertiesSet();
			return defaultFactory;
		}

	}

}
