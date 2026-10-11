package com.fcusumano.push.controller;

import com.fcusumano.push.client.StompSubscription;
import jakarta.validation.constraints.NotNull;


/**
 * Corpo del messaggio {@code ERROR} inviato quando una sottoscrizione o una modifica dei filtri viene rifiutata.
 *
 * @param subscriptionId id della sottoscrizione interessata
 * @param destination destinazione del frame rifiutato
 * @param message motivo del rifiuto
 *
 * @author Francesco Cusumano
 */
public record StompErrorResponse(String subscriptionId, String destination, String message) {

    public static StompErrorResponse of(@NotNull StompSubscription subscription, String destination, String message) {
        return new StompErrorResponse(subscription.subscriptionId(), destination, message);
    }
}
