package com.fcusumano.push.controller;

import com.fcusumano.push.client.StompSubscription;
import com.fcusumano.push.util.StompDestinations;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.simp.SimpMessageHeaderAccessor;
import org.springframework.messaging.simp.SimpMessageType;
import org.springframework.messaging.simp.stomp.StompHeaderAccessor;
import org.springframework.messaging.support.NativeMessageHeaderAccessor;

import static com.fcusumano.push.util.StompMapper.FILTERS_HEADER;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.springframework.messaging.simp.SimpMessageType.DISCONNECT;
import static org.springframework.messaging.simp.SimpMessageType.MESSAGE;
import static org.springframework.messaging.simp.SimpMessageType.SUBSCRIBE;
import static org.springframework.messaging.simp.SimpMessageType.UNSUBSCRIBE;


/**
 * Frame STOMP del client rilevante per la libreria, letto dagli header del messaggio in ingresso.
 *
 * @param type tipo del frame: {@code SUBSCRIBE}, {@code MESSAGE} (frame {@code SEND}), {@code UNSUBSCRIBE} o {@code DISCONNECT}
 * @param sessionId id della sessione
 * @param subscription sottoscrizione dell'header {@code id}, {@code null} se assente
 * @param destination destinazione (opzionale)
 * @param receipt header {@code receipt} (opzionale)
 * @param filters header {@code filters} (opzionale)
 * @param ack header {@code ack} (opzionale)
 * @param body corpo del frame, vuoto se assente
 */
record StompFrame(@NotNull SimpMessageType type, @NotBlank String sessionId, StompSubscription subscription,
                  String destination, String receipt, String filters, String ack, @NotNull String body) {

    private static final String AUTO_ACK = "auto";

    //null se manca la sessione o il tipo non è SUBSCRIBE, MESSAGE (SEND), UNSUBSCRIBE o DISCONNECT
    static StompFrame of(Message<?> message) {
        final MessageHeaders headers = message.getHeaders();
        final SimpMessageType type = SimpMessageHeaderAccessor.getMessageType(headers);
        final String sessionId = SimpMessageHeaderAccessor.getSessionId(headers);
        if (sessionId == null || (type != SUBSCRIBE && type != MESSAGE && type != UNSUBSCRIBE && type != DISCONNECT)) return null;
        final String subscriptionId = SimpMessageHeaderAccessor.getSubscriptionId(headers); //Header 'id' di SUBSCRIBE e UNSUBSCRIBE
        return new StompFrame(type, sessionId,
                subscriptionId == null ? null : StompSubscription.of(sessionId, subscriptionId),
                SimpMessageHeaderAccessor.getDestination(headers),
                NativeMessageHeaderAccessor.getFirstNativeHeader(StompHeaderAccessor.STOMP_RECEIPT_HEADER, headers),
                NativeMessageHeaderAccessor.getFirstNativeHeader(FILTERS_HEADER, headers),
                NativeMessageHeaderAccessor.getFirstNativeHeader(StompHeaderAccessor.STOMP_ACK_HEADER, headers),
                message.getPayload() instanceof byte[] bytes ? new String(bytes, UTF_8) : "");
    }

    //ack assente o 'auto'
    boolean autoAck() {
        return ack == null || AUTO_ACK.equals(ack);
    }

    //SUBSCRIBE o SEND verso una destinazione della libreria
    boolean toPushDestination(@NotNull StompDestinations destinations) {
        return (type == SUBSCRIBE || type == MESSAGE) && destinations.isPush(destination);
    }

    @Override
    public String toString() { //Tipo, sottoscrizione e destinazione: mai il body
        return type + " of session '" + sessionId + "'"
               + (subscription == null ? "" : " with subscription '" + subscription.subscriptionId() + "'")
               + (destination == null ? "" : " to '" + destination + "'");
    }
}
