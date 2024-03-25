package org.springframework.kafka.config;

/**
 * Configuration constants for internal sharing across subpackages.
 *
 * @author Juergen Hoeller
 * @author Gary Russell
 * @author Tomaz Fernandes
 * @author Joe Kim
 */
public enum KafkaListenerConfigTypes {

	KAFKA_LISTENER_ANNOTATION_PROCESSOR_BEAN_NAME(KAFKA_CONFIG_PATH.KAFKA_LISTENER_ANNOTATION_PROCESSOR_BEAN_NAME),
	KAFKA_LISTENER_ENDPOINT_REGISTRY_BEAN_NAME(KAFKA_CONFIG_PATH.KAFKA_LISTENER_ENDPOINT_REGISTRY_BEAN_NAME),
	KAFKA_CONSUMER_BACK_OFF_MANAGER_BEAN_NAME(KAFKA_CONFIG_PATH.KAFKA_CONSUMER_BACK_OFF_MANAGER_BEAN_NAME);

	private final String beanName;

	KafkaListenerConfigTypes(final String beanName) {
		this.beanName = beanName;
	}

	public String getBeanName() {
		return this.beanName;
	}

	public static class KAFKA_CONFIG_PATH {
		/**
		 * The bean name of the internally managed Kafka listener annotation processor.
		 */
		public static final String KAFKA_LISTENER_ANNOTATION_PROCESSOR_BEAN_NAME =
				"org.springframework.kafka.config.internalKafkaListenerAnnotationProcessor";

		/**
		 * The bean name of the internally managed Kafka listener endpoint registry.
		 */
		public static final String KAFKA_LISTENER_ENDPOINT_REGISTRY_BEAN_NAME =
				"org.springframework.kafka.config.internalKafkaListenerEndpointRegistry";

		/**
		 * The bean name of the internally managed Kafka consumer back off manager.
		 */
		public static final String KAFKA_CONSUMER_BACK_OFF_MANAGER_BEAN_NAME =
				"org.springframework.kafka.config.internalKafkaConsumerBackOffManager";
	}

}
