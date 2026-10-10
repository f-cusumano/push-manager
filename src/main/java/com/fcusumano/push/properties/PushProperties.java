package com.fcusumano.push.properties;

import org.springframework.boot.context.properties.ConfigurationProperties;


/**
 * Configurazione di push-manager, letta dal prefisso {@code push-manager} dell'application.yml.
 * <p>
 * Ogni modulo che utilizzi push-manager dovrebbe definire queste configurazioni.
 * Nei commenti i valori consigliati: una proprietà assente assume il default Java
 * ({@code 0} o {@code false}), quindi senza configurazione l'heartbeat resta disattivato.
 * </p>
 *
 * @param timeoutMs timeout in millisecondi dell'{@code SseEmitter}; {@code 0} = nessun timeout
 * @param heartbeatEnabled abilita l'invio periodico dell'evento {@code HEARTBEAT} ai client in streaming
 * @param heartbeatIntervalMs intervallo in millisecondi tra due heartbeat; ignorato se non positivo
 * @param clientRetryMs tempo di riconnessione suggerito al browser, inviato con l'evento {@code CONNECTED}
 *
 * @author Francesco Cusumano
 */
@ConfigurationProperties(prefix = "push-manager")
public record PushProperties(
        long timeoutMs, //0L
        boolean heartbeatEnabled, //true
        long heartbeatIntervalMs, //1500L
        long clientRetryMs //2000L
) {}
