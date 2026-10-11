package com.fcusumano.push.properties;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

import java.util.List;


/**
 * Configurazione STOMP di push-manager, letta dal prefisso {@code push-manager.stomp}.
 * <p>
 * Usata solo con {@code @EnablePushMessaging(stomp = true)}. I default sono dichiarati con
 * {@link DefaultValue}: una stringa nulla renderebbe la funzionalità inutilizzabile.
 * </p>
 *
 * @param endpoint endpoint WebSocket registrato dalla libreria; vuoto = nessun endpoint della libreria,
 *                 i client usano quelli dell'applicazione
 * @param allowedOriginPatterns origini ammesse sull'endpoint della libreria; assente = solo la stessa origine
 * @param destinationPrefix prefisso riservato alla libreria ({@code SUBSCRIBE {prefix}/{CHANNEL}},
 *                          {@code SEND {prefix}/filter/{subscription-id}}): deve iniziare e non finire con {@code /}
 *
 * @author Francesco Cusumano
 */
@ConfigurationProperties(prefix = "push-manager.stomp")
public record StompProperties(
        @DefaultValue("/push-ws") String endpoint, //vuoto = nessun endpoint della libreria
        List<String> allowedOriginPatterns, //assente = solo stessa origine
        @DefaultValue("/push") String destinationPrefix //riservato alla libreria
) {}
