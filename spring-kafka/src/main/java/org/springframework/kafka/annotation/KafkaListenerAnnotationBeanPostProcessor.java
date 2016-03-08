/*
 * Copyright 2014-2016 the original author or authors.
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

package org.springframework.kafka.annotation;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;

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
import org.springframework.core.Ordered;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.kafka.config.KafkaListenerConfigUtils;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerEndpointRegistrar;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.config.MethodKafkaListenerEndpoint;
import org.springframework.kafka.config.MultiMethodKafkaListenerEndpoint;
import org.springframework.messaging.handler.annotation.support.DefaultMessageHandlerMethodFactory;
import org.springframework.messaging.handler.annotation.support.MessageHandlerMethodFactory;
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
 * @author Stephane Nicoll
 * @author Juergen Hoeller
 * @author Gary Russell
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
	static final String DEFAULT_KAFKA_LISTENER_CONTAINER_FACTORY_BEAN_NAME = "kafkaListenerContainerFactory";


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
		Class<?> targetClass = AopUtils.getTargetClass(bean);
		Collection<KafkaListener> classLevelListeners = findListenerAnnotations(targetClass);
		final boolean hasClassLevelListeners = classLevelListeners.size() > 0;
		final List<Method> multiMethods = new ArrayList<Method>();
		ReflectionUtils.doWithMethods(targetClass, new ReflectionUtils.MethodCallback() {

			@Override
			public void doWith(Method method) throws IllegalArgumentException, IllegalAccessException {
				for (KafkaListener KafkaListener : findListenerAnnotations(method)) {
					processKafkaListener(KafkaListener, method, bean, beanName);
				}
				if (hasClassLevelListeners) {
					KafkaHandler KafkaHandler = AnnotationUtils.findAnnotation(method, KafkaHandler.class);
					if (KafkaHandler != null) {
						multiMethods.add(method);
					}
				}
			}
		});
		if (hasClassLevelListeners) {
			processMultiMethodListeners(classLevelListeners, multiMethods, bean, beanName);
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
	private Collection<KafkaListener> findListenerAnnotations(Method method) {
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

	protected void processKafkaListener(KafkaListener KafkaListener, Method method, Object bean, String beanName) {
		Method methodToUse = checkProxy(method, bean);
		MethodKafkaListenerEndpoint<K, V> endpoint = new MethodKafkaListenerEndpoint<K, V>();
		endpoint.setMethod(methodToUse);
		endpoint.setBeanFactory(this.beanFactory);
		processListener(endpoint, KafkaListener, bean, methodToUse, beanName);
	}

	private Method checkProxy(Method method, Object bean) {
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
								"attribute to 'true'", method.getName(), method.getDeclaringClass().getSimpleName()));
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
				throw new BeanInitializationException("Could not register Kafka listener endpoint on [" +
						adminTarget + "] for bean " + beanName + ", no "
						+ KafkaListenerContainerFactory.class.getSimpleName() + " with id '" +
						containerFactoryBeanName + "' was found in the application context", ex);
			}
		}

		this.registrar.registerEndpoint(endpoint, factory);
	}

	private String getEndpointId(KafkaListener KafkaListener) {
		if (StringUtils.hasText(KafkaListener.id())) {
			return resolve(KafkaListener.id());
		}
		else {
			return "org.springframework.kafka.KafkaListenerEndpointContainer#" + this.counter.getAndIncrement();
		}
	}

	private org.apache.kafka.common.TopicPartition[] resolveTopicPartitions(KafkaListener kafkaListener) {
		TopicPartition[] partitions = kafkaListener.topicPartitions();
		List<org.apache.kafka.common.TopicPartition> result = new ArrayList<>();
		if (partitions.length > 0) {
			for (int i = 0; i < partitions.length; i++) {
				Object topic = resolveExpression(partitions[i].topic());
				Assert.state(topic instanceof String, "topic in @TopicPartition must resolve to a String, not"
							+ topic.getClass());
				Object partition = resolveExpression(partitions[i].partition());
				Assert.state(partition instanceof Integer || partition instanceof String,
						"partition in @TopicPartition must resolve to an Integer or String, not a "
								+ partition.getClass());
				if (partition instanceof String) {
					partition = Integer.valueOf((String) partition);
				}
				result.add(new org.apache.kafka.common.TopicPartition((String) topic, (Integer) partition));
			}
		}
		return result.toArray(new org.apache.kafka.common.TopicPartition[result.size()]);
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
				throw new IllegalStateException("topicPattern must resolve to a Pattern or String, not "
						+ resolved.getClass());
			}
		}
		return pattern;
	}

	@SuppressWarnings("unchecked")
	private void resolveAsString(Object resolvedValue, List<String> result) {
		if (resolvedValue instanceof String[]) {
			for (Object object : (String[]) resolvedValue) {
				resolveAsString(object, result);
			}
		}
		if (resolvedValue instanceof String) {
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

	private Object resolveExpression(String value) {
		String resolvedValue = resolve(value);

		if (!(resolvedValue.startsWith("#{") && value.endsWith("}"))) {
			return resolvedValue;
		}

		return this.resolver.evaluate(resolvedValue, this.expressionContext);
	}

	/**
	 * Resolve the specified value if possible.
	 *
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

		public void setMessageHandlerMethodFactory(MessageHandlerMethodFactory KafkaHandlerMethodFactory1) {
			this.messageHandlerMethodFactory = KafkaHandlerMethodFactory1;
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
			defaultFactory.afterPropertiesSet();
			return defaultFactory;
		}

	}

}
