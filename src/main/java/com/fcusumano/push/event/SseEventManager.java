package com.fcusumano.push.event;

import com.fcusumano.push.registry.SseRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * Implementazione concreta di {@link EventManager} specializzata nella gestione
 * degli eventi SSE ({@link SsePushEvent}).
 * <p>
 * La classe si occupa di ricevere un evento già validato e committato
 * (grazie al meccanismo di {@link org.springframework.transaction.event.TransactionalEventListener}),
 * e di instradarlo verso tutti i client connessi al canale specificato tramite
 * il {@link SseRegistry}.
 * </p>
 *
 * <h2>Ruolo nel sistema</h2>
 * <p>
 * {@code SseEventManager} rappresenta il componente incaricato della
 * <b>distribuzione dei messaggi</b> ai client SSE.
 * Funziona come punto centrale per la pubblicazione degli eventi verso più
 * sessioni SSE, delegando al registry la gestione effettiva delle connessioni.
 * </p>
 *
 * <h2>Relazione con EventManager</h2>
 * <p>
 * La classe estende {@link EventManager} definendo:
 * </p>
 * <ul>
 *     <li>{@code E = SsePushEvent<?>}</li>
 *     <li>{@code R = SseRegistry}</li>
 * </ul>
 * <p>
 * Il metodo {@link #handle(SsePushEvent)} rappresenta la logica applicativa eseguita
 * dopo il commit della transazione che ha generato l’evento.
 * </p>
 *
 * <h2>Flusso operativo</h2>
 * <ol>
 *     <li>Una classe applicativa crea un {@link SsePushEvent} e lo pubblica tramite
 *         <b>publishEvent</b> (metodo ereditato).</li>
 *     <li>L’evento viene catturato dopo il commit tramite l’ascoltatore ereditato
 *         da {@code EventManager}.</li>
 *     <li>Il metodo {@link #handle(SsePushEvent)} invoca il registry per
 *         trasmettere l’evento ai client connessi.</li>
 * </ol>
 *
 * <h2>Metodo handle()</h2>
 * <p>
 * È il cuore della logica di consegna, dove:
 * </p>
 * <ul>
 *     <li>viene recuperato il canale dell’evento;</li>
 *     <li>viene chiamato {@link SseRegistry#broadcast} per distribuire l’evento;</li>
 *     <li>vengono registrate informazioni di debug sulla consegna.</li>
 * </ul>
 *
 * <h2>Estendibilità</h2>
 * <p>
 * La classe può essere estesa solo se si desidera:
 * </p>
 * <ul>
 *     <li>personalizzare ulteriori controlli sugli eventi;</li>
 *     <li>implementare logiche di audit o metrica;</li>
 *     <li>arricchire la gestione dei retry o delle politiche di consegna.</li>
 * </ul>
 *
 * <h2>Esempio di utilizzo</h2>
 * <pre>{@code
 * @Service
 * public class UserService {
 *
 *     private final SseEventManager sseEventManager;
 *
 *     public EventsService(SseEventManager sseEventManager) {
 *         this.sseEventManager = sseEventManager;
 *     }
 *
 *     @Transactional
 *     public User updateUser(User user) {
 *         ... //--> controlli e persistenza
 *         SsePushEvent<User> event = SsePushEvent.of(user, UPDATED, ANGRAFICA_USER);
 *         sseEventManager.publishEvent(event);
 *         return user;
 *     }
 * }
 * }</pre>
 *
 * <h2>Note</h2>
 * <ul>
 *     <li>La classe è annotata con {@code @Service} per consentire la rilevazione
 *         automatica da parte del container Spring.</li>
 *     <li>Il metodo {@code publishEvent()} può essere sovrascritto per aggiungere
 *         ulteriori controlli.</li>
 *     <li>Il metodo {@code handle()} è sempre chiamato in modo asincrono
 *         dopo il commit della transazione che ha pubblicato l’evento.</li>
 * </ul>
 *
 * @author Francesco Cusumano
 */
@Service
public class SseEventManager extends EventManager<SsePushEvent<?>, SseRegistry> {

    private static final Logger log = LoggerFactory.getLogger(SseEventManager.class);

    public SseEventManager(SseRegistry registry) {
        super(registry);
    }

//    @Override
//    public void publishEvent(SsePushEvent<?> event) {
//        if (Objects.equals(event.data.getClass(), event.getChannel().payloadType())) { //--> serve percorso identico
//            super.publishEvent(event);
//            return;
//        }
//        log.warn("Event '{}' not allowed for channel '{}'.", event.getMessageId(), event.getChannel().topic());
//    }

    /**
     * Inoltra l'evento a tutti i client in streaming sul canale dell'evento
     * che non lo escludono tramite i propri filtri.
     */
    @Override
    public void handle(SsePushEvent<?> event) {
        log.debug("Publishing message on channel '{}'..", event.getChannel());
        int delivered = registry.broadcast(event);
        log.debug("Delivered {} messages on channel '{}'..", delivered, event.getChannel());
    }
}
