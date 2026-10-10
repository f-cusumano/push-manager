package com.fcusumano.push.properties;

import org.springframework.boot.context.properties.ConfigurationProperties;


/**
 * Ogni modulo che utilizzi push-manager dovrà definire
 * queste configurazioni nell'application.yml.
 * Valori di default nei commenti.
 */
@ConfigurationProperties(prefix = "push-manager")
public record PushProperties(
        long timeoutMs, //0L
        boolean heartbeatEnabled, //true
        long heartbeatIntervalMs, //1500L
        long clientRetryMs //2000L
) {}
