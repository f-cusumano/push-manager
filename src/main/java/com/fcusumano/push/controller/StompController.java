package com.fcusumano.push.controller;

import com.fcusumano.push.channel.ChannelEnum;
import com.fcusumano.push.client.StompSubscription;
import com.fcusumano.push.service.StompService;
import com.fcusumano.push.util.StompDestinations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.event.EventListener;
import org.springframework.core.Ordered;
import org.springframework.core.ResolvableType;
import org.springframework.core.annotation.Order;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.simp.SimpMessageHeaderAccessor;
import org.springframework.messaging.support.ChannelInterceptor;
import org.springframework.web.socket.messaging.SessionConnectEvent;
import org.springframework.web.socket.messaging.SessionDisconnectEvent;

import java.util.Arrays;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;


/**
 * Controller astratto da estendere nell'applicazione per ricevere eventi via STOMP su WebSocket,
 * controparte di {@link SseController}.
 * <p>
 * È un {@link ChannelInterceptor} che la libreria aggiunge al {@code clientInboundChannel} dopo gli interceptor
 * configurati (Spring Security e applicazione): trattiene i frame della libreria ({@code SUBSCRIBE {prefix}/{CHANNEL}},
 * la relativa {@code UNSUBSCRIBE} e ogni {@code SEND} sotto il prefisso) e li passa a {@link StompService},
 * così nessun broker li riceve. Gli altri frame proseguono invariati verso l'applicazione e il broker.
 * Apertura e chiusura delle sessioni arrivano dagli eventi di sessione di Spring; anche il {@code DISCONNECT}
 * chiude la sessione.
 * </p>
 * <p>
 * La sottoclasse può dipendere da altri bean, anche da {@code SimpMessagingTemplate}.
 * </p>
 * <pre>{@code
 * @Controller
 * public class PushStompEndpoint extends StompController<Channels> {}
 * }</pre>
 *
 * @param <C> enum dei canali generato dall'annotation processor
 *
 * @author Francesco Cusumano
 */
public abstract class StompController<C extends ChannelEnum> extends PushController<StompService> implements ChannelInterceptor {

    private static final Logger log = LoggerFactory.getLogger(StompController.class);

    private final Map<String, C> topic2channel; //Costanti dell'enum generato, per topic


    /** @throws IllegalStateException se la sottoclasse non indica l'enum dei canali */
    protected StompController() {
        topic2channel = mapChannels(getClass());
    }

    /**
     * Trattiene i frame della libreria e li gestisce; non solleva mai eccezioni, altrimenti Spring
     * risponderebbe con un frame {@code ERROR} chiudendo la connessione.
     *
     * @param message frame in ingresso
     * @param channel {@code clientInboundChannel}
     * @return {@code null} per i frame della libreria, il messaggio invariato altrimenti
     */
    //Non final: un eventuale proxy CGLIB del controller non la intercetterebbe
    @Override
    public Message<?> preSend(Message<?> message, MessageChannel channel) {
        final StompFrame frame = StompFrame.of(message);
        if (frame == null) return message; //CONNECT, heart-beat e messaggi interni passano sempre
        try {
            return handle(frame) ? null : message; //null = frame della libreria, trattenuto: nessun broker lo riceve
        } catch (RuntimeException e) { //Mai propagare: Spring risponderebbe con un frame ERROR chiudendo la connessione
            log.error("STOMP frame {} not handled", frame, e);
            return frame.toPushDestination(service.destinations()) ? null : message;
        }
    }

    /**
     * Apre la sessione STOMP alla connessione del client.
     *
     * @param event evento di connessione
     */
    @EventListener
    @Order(Ordered.HIGHEST_PRECEDENCE) //Prima dei listener dell'applicazione: se uno fallisce, Spring non chiama i successivi
    public void connect(SessionConnectEvent event) {
        service.connect(SimpMessageHeaderAccessor.getSessionId(event.getMessage().getHeaders()));
    }

    /**
     * Chiude la sessione STOMP disiscrivendo tutti i suoi client.
     *
     * @param event evento di chiusura
     */
    @EventListener //Pubblicato per ogni chiusura, anche se un interceptor precedente rifiuta il DISCONNECT sintetico
    @Order(Ordered.HIGHEST_PRECEDENCE) //Prima dei listener dell'applicazione: se uno fallisce, Spring non chiama i successivi
    public void disconnect(SessionDisconnectEvent event) {
        service.disconnect(event.getSessionId());
    }

    private boolean handle(StompFrame frame) {
        return switch (frame.type()) {
            case SUBSCRIBE -> subscribe(frame);
            case MESSAGE -> send(frame);
            case UNSUBSCRIBE -> frame.subscription() != null && service.unsubscribe(frame.subscription(), frame.receipt());
            case DISCONNECT -> disconnect(frame);
            default -> false;
        };
    }

    //DISCONNECT del client o quello sintetico di Spring a fine sessione: seconda via di chiusura, idempotente, se un listener precedente fallisce
    private boolean disconnect(StompFrame frame) {
        service.disconnect(frame.sessionId());
        return false; //Prosegue: il broker risponde al client
    }

    private boolean subscribe(StompFrame frame) {
        final StompDestinations destinations = service.destinations();
        if (!destinations.isPush(frame.destination())) return false;
        if (frame.subscription() == null) {
            log.warn("SUBSCRIBE to '{}' from session '{}' without id discarded", frame.destination(), frame.sessionId());
            return true;
        }
        final C channel = channel(destinations.topic(frame.destination()));
        if (channel == null) {
            service.reject(frame.subscription(), frame.destination(), "No push channel for destination '" + frame.destination() + "'");
        } else if (!frame.autoAck()) {
            service.reject(frame.subscription(), frame.destination(), "Only 'ack:auto' is supported on push subscriptions, found 'ack:" + frame.ack() + "'");
        } else {
            service.subscribe(frame.subscription(), channel, frame.filters(), frame.receipt());
        }
        return true;
    }

    private boolean send(StompFrame frame) {
        final StompDestinations destinations = service.destinations();
        if (!destinations.isPush(frame.destination())) return false;
        final String subscriptionId = destinations.filterSubscriptionId(frame.destination());
        if (subscriptionId == null) {
            log.warn("SEND to push destination '{}' from session '{}' discarded", frame.destination(), frame.sessionId()); //Solo il server pubblica sui canali push
        } else {
            service.updateFilters(StompSubscription.of(frame.sessionId(), subscriptionId), frame.body(), frame.receipt());
        }
        return true;
    }

    private C channel(String topic) {
        return topic == null ? null : topic2channel.get(topic); //Le mappe immutabili non accettano chiavi null
    }

    @SuppressWarnings("unchecked")
    private static <C extends ChannelEnum> Map<String, C> mapChannels(Class<?> controllerType) {
        final Class<?> channelType = ResolvableType.forClass(StompController.class, controllerType).getGeneric(0).resolve();
        if (channelType == null || !channelType.isEnum()) {
            throw new IllegalStateException("'" + controllerType.getName() + "' must extend StompController with the generated channel enum, e.g. StompController<Channels>");
        }
        return Arrays.stream((C[]) channelType.getEnumConstants())
                .collect(Collectors.toUnmodifiableMap(ChannelEnum::topic, Function.identity()));
    }
}
