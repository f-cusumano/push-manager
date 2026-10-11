package com.fcusumano.push.client;

import jakarta.validation.constraints.NotBlank;


/**
 * Identità di un client STOMP: una sottoscrizione all'interno di una sessione WebSocket.
 *
 * @param sessionId id della sessione STOMP ({@code simpSessionId})
 * @param subscriptionId id della sottoscrizione scelto dal client (header {@code id})
 *
 * @author Francesco Cusumano
 */
public record StompSubscription(@NotBlank String sessionId, @NotBlank String subscriptionId) {

    public static StompSubscription of(String sessionId, String subscriptionId) {
        return new StompSubscription(sessionId, subscriptionId);
    }

    @Override
    public String toString() { //Per i log
        return "'" + subscriptionId + "' of session '" + sessionId + "'";
    }
}
