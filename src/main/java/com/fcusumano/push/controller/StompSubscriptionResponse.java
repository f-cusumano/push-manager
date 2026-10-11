package com.fcusumano.push.controller;

import com.fcusumano.push.channel.ChannelEnum;
import com.fcusumano.push.client.StompClient;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;


/**
 * Corpo del messaggio {@code CONNECTED} inviato su una sottoscrizione STOMP accettata,
 * controparte di {@link SseSubscriptionResponse}.
 *
 * @param subscriptionId id della sottoscrizione
 * @param channel canale sottoscritto
 * @param streamedObject nome semplice della classe dei payload del canale
 *
 * @author Francesco Cusumano
 */
public record StompSubscriptionResponse(@NotBlank String subscriptionId, @NotNull ChannelEnum channel, String streamedObject) {

    public static StompSubscriptionResponse of(@NotNull StompClient<?> client) {
        return new StompSubscriptionResponse(client.subscriptionId(), client.channel(), client.channel().payloadType().getSimpleName());
    }
}
