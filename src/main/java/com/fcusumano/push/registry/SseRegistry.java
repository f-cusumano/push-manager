package com.fcusumano.push.registry;

import com.fcusumano.push.channel.ChannelEnum;
import com.fcusumano.push.client.SseClient;
import com.fcusumano.push.event.SsePushEvent;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.stream.Collectors;

import static com.fcusumano.push.client.ClientStatus.STREAMING;
import static com.fcusumano.push.util.ObjectMapper.toEventBuilder;


/**
 * Registry centrale per la gestione dei client connessi tramite Server‑Sent Events (SSE)
 * e per la distribuzione degli eventi {@link SsePushEvent} sui diversi canali di
 * messaggistica rappresentati da {@link ChannelEnum}.
 *
 * <p>
 * {@code SseRegistry} mantiene un insieme thread-safe di client attivi per ciascun
 * canale, permette la sottoscrizione e disiscrizione delle sessioni SSE, fornisce
 * funzionalità di lookup e implementa il meccanismo di broadcasting degli eventi.
 * </p>
 *
 * <h2>Struttura interna</h2>
 * <p>
 * La gestione dei client è basata su:
 * </p>
 * <ul>
 *     <li>una mappa <b>channel → set di client</b> rappresentata tramite
 *         {@link ConcurrentHashMap};</li>
 *     <li>una lista concorrente per ogni canale tramite {@link CopyOnWriteArraySet},
 *         per supportare aggiunte e rimozioni anche in presenza di streaming attivo;</li>
 *     <li>istanze {@link SseClient} che incapsulano sessionId, filtraggio e
 *         configurazione della connessione.</li>
 * </ul>
 *
 * <h2>Thread-safety</h2>
 * <ul>
 *     <li>La mappa dei client è basata su {@link ConcurrentHashMap}, sicura in presenza
 *         di accessi concorrenti.</li>
 *     <li>I set dei client sono {@link CopyOnWriteArraySet}, adatti alla lettura intensiva
 *         tipica dello streaming SSE.</li>
 * </ul>
 *
 * <h2>Esempio di utilizzo</h2>
 * <pre>{@code
 * SseClient<Object> client = registry.subscribeClient(MyChannels.NOTIFICATIONS, emitter);
 * registry.broadcast(event);   // Distribuisce l'evento a tutti i client attivi
 * }</pre>
 *
 * <h2>Note</h2>
 * <ul>
 *     <li>La classe è final perché rappresenta il registry concreto SSE.</li>
 *     <li>Si appoggia a {@link SsePushEvent} per garantire la coerenza tra canale e payload.</li>
 *     <li>Sarebbe opportuno introdurre una custom exception per <code>selectClient()</code>.</li>
 * </ul>
 *
 * @author Francesco Cusumano
 */
@Component
public final class SseRegistry implements EventRegistry<SsePushEvent<?>> {

    private static final Logger log = LoggerFactory.getLogger(SseRegistry.class);

    private final Map<ChannelEnum, Set<SseClient<?>>> channel2clients = new ConcurrentHashMap<>();

    /**
     * <h2>Sottoscrizione di un nuovo client</h2>
     * <ul>
     *     <li>genera un <b>sessionId</b> univoco di tipo {@link UUID};</li>
     *     <li>crea un nuovo {@link SseClient} in stato {@link com.fcusumano.push.client.ClientStatus#SUBSCRIBED};</li>
     *     <li>registra il client sul canale in input;</li>
     *     <li>registra la sottoscrizione nei log col numero aggiornato di client sottoscritti e attivi per il canale.</li>
     * </ul>
     * @param channel il canale sul quale deve essere registrato il nuovo client
     * @param emitter l'emitter da associare al canale per la gestione della messagistica
     * @return il client generato
     */
    @SuppressWarnings("unchecked")
    public SseClient<Object> subscribeClient(@NotNull ChannelEnum channel, @NotNull SseEmitter emitter) {
        final SseClient<Object> client = new SseClient<>(UUID.randomUUID().toString(), channel, emitter);

        channel2clients
                .computeIfAbsent(channel, k -> new CopyOnWriteArraySet<>())
                .add(client); // Safe erase
        log.debug("New client registered on channel {}. Total (subscribed/streaming): {}/{}", channel.topic(),
                channel2clients.get(channel).size(), getStreamingClients(channel).size());
        return client;
    }

    /**
     * <h2>Disiscrizione di un client da un canale tramite session-id</h2>
     * <p>
     * Rimuove il client associato al sessionId in unput.
     * Se un canale rimane privo di client, viene automaticamente eliminato dal registry.
     * </p>
     * @param channel il canale di trasmissione
     * @param sessionId l'identificativo univoco della sessione e del client
     */
    public void unsubscribeClient(@NotNull ChannelEnum channel, @NotBlank String sessionId) {
        final Set<SseClient<?>> clients = channel2clients.get(channel);
        if (clients == null) return;
        clients.removeIf(c -> sessionId.equals(c.sessionId()));
        log.debug("Unregistered emitter on channel '{}'. Total: '{}'", channel, clients.size());
        if (clients.isEmpty()) channel2clients.remove(channel);
    }

    /**
     * <h2>Selezione di un client tramite session-id</h2>
     * <p>
     * Il metodo effettua una lookup globale tra tutti i
     * canali alla ricerca del client con lo specifico sessionId in input.
     * In caso di successo restituisce il client, altrimenti solleva un’eccezione.
     * </p>
     *
     * @param sessionId l'identificativo univoco della sessione e del client
     * @return il client
     */
    public SseClient<Object> selectClient(@NotBlank String sessionId) {
        for (ChannelEnum channel: channel2clients.keySet()) {
            for (SseClient<?> client: channel2clients.get(channel)) {
                if (sessionId.equals(client.sessionId())) return (SseClient<Object>) client;
            }
        }
        log.warn("No client with session-id '{}' has been found", sessionId);
        throw new RuntimeException(); //Da raffinare --> NoSuchClientException
    }

    /**
     * <h2>Selezione di un emitter tramite session-id</h2>
     * <p>
     * Il metodo effettua recupera il client attraverso {@link #selectClient(String)}.
     * In caso di successo ne restituisce l'emitter, altrimenti solleva un’eccezione.
     * </p>
     *
     * @param sessionId l'identificativo univoco della sessione e del client
     * @return l'emitter relativo al client identificato
     */
    public SseEmitter selectEmitter(@NotBlank String sessionId) {
        return selectClient(sessionId).emitter();
    }

    /**
     * <h2>Broadcast degli eventi</h2>
     * <p>
     * Il metodo è responsabile dell’invio di un
     * evento SSE a tutti i client del canale che sono nello stato
     * {@link com.fcusumano.push.client.ClientStatus#STREAMING}.
     * </p>
     *
     * <h3>Flusso del broadcast</h3>
     * <ol>
     *     <li>Recupera i client streaming del canale.</li>
     *     <li>Converte l’evento in {@link SseEmitter.SseEventBuilder} tramite <code>toEventBuilder()</code>.</li>
     *     <li>Per ogni client:
     *         <ul>
     *             <li>verifica se è interessato tramite {@link #clientInterested(SseClient, SsePushEvent)};</li>
     *             <li>invoca {@code emitter.send(builder)};</li>
     *             <li>in caso di errore chiude l’emitter e registra il problema.</li>
     *         </ul>
     *     </li>
     *     <li>Restituisce il numero di messaggi consegnati.</li>
     * </ol>
     *
     * <h2>Filtraggio dei messaggi</h2>
     * <p>
     * Prima di inviare un evento a un client, il registry applica eventuali criteri
     * di filtraggio definiti nelle {@link com.fcusumano.push.filter.FilteringOptions}.
     * Un client riceve l’evento se:
     * </p>
     * <ul>
     *     <li>non ha filtri;</li>
     *     <li>oppure il tipo di evento è tra quelli permessi;</li>
     *     <li>oppure l’evento è <i>non muteable</i> (es. HEARTBEAT);</li>
     *     <li>e la condizione dei filtri è soddisfatta.</li>
     * </ul>
     * @param event un evento pubblicato su un dato canale
     * @param <T> il tipo di payload gestito dal canale
     * @return il numero di messaggi correttamente inviati ai client in ascolto e interessati
     */
    public <T> int broadcast(@NotNull SsePushEvent<T> event) {
        final ChannelEnum channel = event.getChannel();
        final Set<SseClient<?>> rawClients = getStreamingClients(channel);
        if (CollectionUtils.isEmpty(rawClients)) return 0;
        log.debug("Listening clients: {}", rawClients.size());

        @SuppressWarnings("unchecked") final Set<SseClient<T>> clients = rawClients.stream()
                .map(c -> (SseClient<T>) c).collect(Collectors.toSet());
        final SseEmitter.SseEventBuilder builder = toEventBuilder(event);
        int delivered = 0;
        for (SseClient<T> client : clients) {
            final SseEmitter emitter = client.emitter();
            if (clientInterested(client, event)) {
                try {
                    log.debug("Emitter '{}' sending on channel '{}'..", emitter, channel);
                    emitter.send(builder);
                    delivered++;
                } catch (Exception e) {
                    log.warn("Emitter '{}' send failed on channel '{}'. Closing emitter.", emitter, channel, e);
                    emitter.completeWithError(e);
                }
            } else {
                log.debug("Emitter '{}' not interested to stream this message on channel '{}'..", emitter, channel);
            }
        }
        return delivered;
    }

    /* Restituisce l'insieme di client registrati al canale in input e in stato STREAMING. */
    private Set<SseClient<?>> getStreamingClients(@NotNull ChannelEnum channel) {
        final Set<SseClient<?>> clients = channel2clients.get(channel);
        return CollectionUtils.isEmpty(clients) ? null : clients.stream()
                .filter(c -> c.status() == STREAMING)
                .collect(Collectors.toSet());
    }

    @SuppressWarnings("unchecked")
    /* Un client è interessato all'evento se:
        -   non ha filtri
        -   oppure il tipo di evento è tra quelli permessi
        -   oppure il tipo di evento è 'non muteable'
        -   e la condizione dei filtri è soddisfatta
     */
    private <T> boolean clientInterested(@NotNull SseClient<T> client, @NotNull SsePushEvent<T> event) {
        final var filteringOptions = client.filteringOptions();
        if (filteringOptions == null) return true;
        final var requiredEventTypes = filteringOptions.allowedEventTypes();
        final boolean eventPermitted = CollectionUtils.isEmpty(requiredEventTypes)
                                       || requiredEventTypes.contains(event.getEventType())
                                       || !event.getEventType().isMuteable();
        if (!eventPermitted) return false;
        final var condition = filteringOptions.condition();
        return condition == null || condition.test(event.getData());
    }
}
