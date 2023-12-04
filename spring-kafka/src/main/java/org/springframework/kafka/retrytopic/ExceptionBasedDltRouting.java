package org.springframework.kafka.retrytopic;

public @interface ExceptionBasedDltRouting {
    ExceptionBasedDestinationDlt[] routing() default {};
}
