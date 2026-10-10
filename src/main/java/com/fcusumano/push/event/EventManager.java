package com.fcusumano.push.event;

import com.fcusumano.push.registry.EventRegistry;
import org.jspecify.annotations.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.ApplicationEventPublisherAware;
import org.springframework.scheduling.annotation.Async;
import org.springframework.transaction.event.TransactionalEventListener;

import static org.springframework.transaction.event.TransactionPhase.AFTER_COMMIT;


/**
 * Classe astratta che definisce il comportamento comune per tutti i gestori
 * di eventi nel sistema di Push Messaging. Un {@code EventManager} coordina
 * la pubblicazione, l'ascolto e la gestione degli eventi {@link PushEvent},
 * fornendo un'infrastruttura robusta per elaborazioni asincrone e basate su
 * transazioni.
 *
 * <p>
 * La classe opera integrandosi con il meccanismo di event publishing di Spring
 * tramite {@link ApplicationEventPublisher} e osserva gli eventi pubblicati
 * utilizzando {@link TransactionalEventListener}, garantendo che la gestione
 * avvenga solo dopo il commit della transazione.
 * </p>
 *
 * <h2>Parametri generici</h2>
 * <ul>
 *     <li><b>E</b> – tipo dell’evento, estensione di {@link PushEvent};</li>
 *     <li><b>R</b> – tipo del registry associato, implementazione di {@link EventRegistry}
 *         specializzata per l’evento {@code E}.</li>
 * </ul>
 *
 * <h2>Funzionalità principali</h2>
 * <ul>
 *     <li><b>Pubblicazione eventi</b> tramite {@link #publishEvent(PushEvent)}, che richiede
 *         un contesto transazionale attivo.</li>
 *     <li><b>Ascolto automatico degli eventi</b> con gestione asincrona grazie a
 *         {@link Async} e {@link TransactionalEventListener}.</li>
 *     <li><b>Delegazione della logica applicativa</b> al metodo astratto {@link #handle(PushEvent)},
 *         implementato dalle sottoclassi.</li>
 *     <li><b>Accesso al registry</b> tramite il campo {@code registry}, utile per operazioni
 *         di lookup o gestione degli eventi.</li>
 * </ul>
 *
 * <h2>Comportamento del metodo publishEvent()</h2>
 * <p>
 * Pubblica un evento tramite {@link ApplicationEventPublisher}.
 * L’evento:
 * </p>
 * <ul>
 *     <li>viene registrato nei log con il suo contenuto descrittivo;</li>
 *     <li>viene propagato nel contesto Spring attraverso il meccanismo standard degli eventi;</li>
 *     <li>viene elaborato successivamente dall’ascoltatore {@link #onEvent(PushEvent)}.</li>
 * </ul>
 * <p>
 * <b>Nota importante:</b> la pubblicazione deve avvenire all’interno di una transazione,
 * poiché la gestione effettiva dell’evento avviene solo dopo il commit, secondo la
 * configurazione {@link TransactionalEventListener} con fase {@code AFTER_COMMIT}.
 * </p>
 *
 * <h2>Gestione degli eventi</h2>
 * <p>
 * Il metodo finale {@link #onEvent(PushEvent)}:
 * </p>
 * <ul>
 *     <li>ascolta tutti gli eventi di tipo {@code E};</li>
 *     <li>garantisce esecuzione asincrona grazie a {@link Async};</li>
 *     <li>chiama il metodo astratto {@link #handle(PushEvent)} per delegare la logica
 *         applicativa alle sottoclassi.</li>
 * </ul>
 *
 * <h2>Esempio di implementazione concreta</h2>
 * <pre>{@code
 * @Service
 * public class SseEventManager extends EventManager<SsePushEvent<?>, SseRegistry> {
 *
 *     public SseEventManager(SseRegistry registry) {
 *         super(registry);
 *     }
 *
 *     @Override
 *     protected void handle(SsePushEvent<?> event) {
 *         registry.broadcast(event);
 *     }
 * }
 * }</pre>
 *
 * <h2>Thread-safety e concorrenza</h2>
 * <ul>
 *     <li>La gestione degli eventi è asincrona: più eventi possono essere elaborati
 *         in parallelo.</li>
 *     <li>Le implementazioni del registry devono essere thread-safe se utilizzate in
 *         contesti concorrenti.</li>
 * </ul>
 *
 * <h2>Estendibilità</h2>
 * <p>
 * Le classi concrete devono implementare esclusivamente il metodo {@link #handle(PushEvent)},
 * definendo la logica operativa da eseguire al ricevimento dell’evento.
 * </p>
 *
 * @author Francesco Cusumano
 */
public abstract class EventManager<E extends PushEvent<?>, R extends EventRegistry<E>>
        implements ApplicationEventPublisherAware {

    private static final Logger log = LoggerFactory.getLogger(EventManager.class);

    protected final R registry;
    private ApplicationEventPublisher publisher;

    @Override //Automaticamente iniettato
    public void setApplicationEventPublisher(@NonNull ApplicationEventPublisher publisher) {
        this.publisher = publisher;
    }

    protected EventManager(R registry) {
        this.registry = registry;
    }

    /** ATTENZIONE: la pubblicazione dell'evento deve avvenire in un contesto transazionale. */
    public void publishEvent(E event) {
        log.debug("Publishing event {}..", event.description());
        publisher.publishEvent(event);
        log.debug("..event successfully published.");
    }

    //Per non ripetere l'annotaione su tutte le classi concrete
    @Async
    @TransactionalEventListener(phase = AFTER_COMMIT)
    public final void onEvent(E event) {
        log.debug("Captured event {}..", event.description());
        handle(event);
        log.debug("..event successfully handled.");
    }

    protected abstract void handle(E event);
}
