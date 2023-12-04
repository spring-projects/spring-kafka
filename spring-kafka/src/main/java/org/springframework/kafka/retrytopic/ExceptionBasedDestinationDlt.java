package org.springframework.kafka.retrytopic;

public @interface ExceptionBasedDestinationDlt {
    String customSuffix();
    Class<? extends Throwable>[] exceptions();
}
