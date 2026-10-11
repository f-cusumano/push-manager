package com.fcusumano.push.event;

import com.fcusumano.push.registry.StompRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Gestore degli eventi STOMP ({@link StompPushEvent}), da iniettare nell'applicazione per pubblicarli.
 * <p>
 * Come per SSE, la pubblicazione deve avvenire in una transazione: dopo il commit l'evento viene
 * consegnato in modo asincrono ai client sottoscritti al canale tramite lo {@link StompRegistry}.
 * </p>
 * <pre>{@code
 * stompEventManager.publishEvent(StompPushEvent.of(user, EventType.CREATED, Channels.USERS));
 * }</pre>
 *
 * @author Francesco Cusumano
 */
public class StompEventManager extends EventManager<StompPushEvent<?>, StompRegistry> {

    private static final Logger log = LoggerFactory.getLogger(StompEventManager.class);

    public StompEventManager(StompRegistry registry) {
        super(registry);
    }

    /**
     * Inoltra l'evento a tutti i client in streaming sul canale dell'evento
     * che non lo escludono tramite i propri filtri.
     */
    @Override
    public void handle(StompPushEvent<?> event) {
        log.debug("Publishing message on channel '{}'..", event.getChannel());
        final int delivered = registry.broadcast(event);
        log.debug("Delivered {} messages on channel '{}'..", delivered, event.getChannel());
    }
}
