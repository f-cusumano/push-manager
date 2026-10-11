package com.fcusumano.push.util;

import com.fcusumano.push.client.StompClient;
import com.fcusumano.push.client.StompSubscription;
import com.fcusumano.push.event.EventType;
import com.fcusumano.push.event.PushEvent;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import org.springframework.messaging.Message;
import org.springframework.messaging.simp.SimpMessageHeaderAccessor;
import org.springframework.messaging.simp.SimpMessageType;
import org.springframework.messaging.simp.stomp.StompCommand;
import org.springframework.messaging.simp.stomp.StompHeaderAccessor;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.util.MimeTypeUtils;


/**
 * Mapping verso i messaggi STOMP scritti direttamente sul {@code clientOutboundChannel}.
 * <p>
 * Separata da {@link PushMapper}, così {@code PushMapper} non dipende da spring-messaging.
 * </p>
 *
 * @author Francesco Cusumano
 */
public final class StompMapper {

    /** Header nativo della {@code SUBSCRIBE} con i filtri iniziali ({@code FilteringRequest} in JSON). */
    public static final String FILTERS_HEADER = "filters";
    /** Header nativo dei messaggi con il nome dell'{@link EventType}. */
    public static final String EVENT_TYPE_HEADER = "event-type";
    /** Header nativo dei messaggi con l'id dell'evento. */
    public static final String EVENT_ID_HEADER = "event-id";
    /** Header nativo dei messaggi con il tempo di riconnessione suggerito, solo se valorizzato. */
    public static final String RETRY_MS_HEADER = "retry-ms";
    private static final byte[] EMPTY_PAYLOAD = new byte[0];

    private StompMapper() {
    }

    /**
     * Mappa un evento applicativo nel messaggio per un client.
     *
     * @param client <i>(obbligatorio)</i> il client destinatario
     * @param event <i>(obbligatorio)</i> l'evento
     * @param payload <i>(obbligatorio)</i> il payload JSON, intero o con i soli campi selezionati dal client
     * @return il messaggio da inviare sul {@code clientOutboundChannel}
     */
    public static Message<byte[]> toMessage(@NotNull StompClient<?> client, @NotNull PushEvent<?> event, byte[] payload) {
        return toMessage(client.subscription(), client.destination(), event.getEventType(), event.getMessageId(),
                event.getRetryMs(), payload);
    }

    /**
     * Crea un messaggio {@code MESSAGE} per una sottoscrizione; Spring lo completa con
     * {@code subscription} e {@code message-id} prima di inviarlo.
     *
     * @param subscription <i>(obbligatorio)</i> la sottoscrizione destinataria
     * @param destination <i>(obbligatorio)</i> la destinazione riportata nel messaggio
     * @param eventType <i>(obbligatorio)</i> il tipo di evento, nell'header {@code event-type}
     * @param eventId <i>(obbligatorio)</i> l'id dell'evento, nell'header {@code event-id}
     * @param retryMs <i>(opzionale)</i> il tempo di riconnessione, nell'header {@code retry-ms} solo se valorizzato
     * @param payload <i>(obbligatorio)</i> il corpo JSON
     * @return il messaggio da inviare sul {@code clientOutboundChannel}
     */
    public static Message<byte[]> toMessage(@NotNull StompSubscription subscription, @NotBlank String destination,
                                            @NotNull EventType eventType, @NotBlank String eventId, Long retryMs,
                                            byte[] payload) {
        final SimpMessageHeaderAccessor accessor = SimpMessageHeaderAccessor.create(SimpMessageType.MESSAGE); //Non StompHeaderAccessor: Spring deve completarlo come MESSAGE
        accessor.setSessionId(subscription.sessionId());
        accessor.setSubscriptionId(subscription.subscriptionId());
        accessor.setDestination(destination);
        accessor.setContentType(MimeTypeUtils.APPLICATION_JSON);
        accessor.setNativeHeader(EVENT_TYPE_HEADER, eventType.name());
        accessor.setNativeHeader(EVENT_ID_HEADER, eventId);
        if (retryMs != null) accessor.setNativeHeader(RETRY_MS_HEADER, retryMs.toString());
        accessor.setLeaveMutable(true); //Come SimpleBrokerMessageHandler.sendMessageToSubscribers
        return MessageBuilder.createMessage(payload, accessor.getMessageHeaders());
    }

    /**
     * Crea il frame {@code RECEIPT} di conferma di un frame del client.
     *
     * @param sessionId <i>(obbligatorio)</i> la sessione destinataria
     * @param receiptId <i>(obbligatorio)</i> il valore dell'header {@code receipt} del frame confermato
     * @return il messaggio da inviare sul {@code clientOutboundChannel}
     */
    public static Message<byte[]> toReceipt(@NotBlank String sessionId, @NotBlank String receiptId) {
        final StompHeaderAccessor accessor = StompHeaderAccessor.create(StompCommand.RECEIPT); //Usato così com'è da StompSubProtocolHandler
        accessor.setSessionId(sessionId);
        accessor.setReceiptId(receiptId);
        return MessageBuilder.createMessage(EMPTY_PAYLOAD, accessor.getMessageHeaders());
    }
}
