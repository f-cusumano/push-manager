package com.fcusumano.push.service;

import com.fcusumano.push.channel.ChannelEnum;
import com.fcusumano.push.controller.SseSubscriptionResponse;
import com.fcusumano.push.event.SsePushEvent;
import com.fcusumano.push.exception.InvalidFilterException;
import com.fcusumano.push.exception.NoSuchClientException;
import com.fcusumano.push.filter.FilteringOptions;
import com.fcusumano.push.filter.FilteringRequest;
import com.fcusumano.push.client.SseClient;
import com.fcusumano.push.registry.SseRegistry;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;
import tools.jackson.databind.json.JsonMapper;

import java.time.Duration;
import java.util.concurrent.ScheduledFuture;
import java.util.function.Consumer;

import static com.fcusumano.push.client.ClientStatus.DISABLED;
import static com.fcusumano.push.client.ClientStatus.STREAMING;
import static com.fcusumano.push.event.EventType.CONNECTED;
import static com.fcusumano.push.event.EventType.DISCONNECTED;
import static com.fcusumano.push.event.EventType.HEARTBEAT;
import static com.fcusumano.push.filter.FilteringRequest.nullEquivalent;
import static com.fcusumano.push.util.PushMapper.mapToFilteringOptions;


/**
 * Servizio dedicato alla gestione operativa delle connessioni Server‑Sent Events (SSE),
 * basato sull’infrastruttura fornita da {@link PushService} e sul registry {@link SseRegistry}.
 *
 * <p>{@code SseService} gestisce l’intero ciclo di vita dei client SSE:</p>
 * <ul>
 *     <li>sottoscrizione al canale;</li>
 *     <li>aggiornamento dei criteri di filtraggio per i messaggi;</li>
 *     <li>avvio dello streaming;</li>
 *     <li>disconnessione e cleanup delle risorse;</li>
 *     <li>invio automatico di eventi connessi allo stato (CONNECTED, HEARTBEAT, DISCONNECTED).</li>
 * </ul>
 *
 * <p>
 * Il servizio funge da livello applicativo superiore rispetto al registry,
 * delegando a quest’ultimo la gestione delle sessioni, ma controllando logica,
 * filtraggio e configurazione dei client.
 * </p>
 *
 * @author Francesco Cusumano
 */
@Service
public class SseService extends PushService<SsePushEvent<?>, SseRegistry> {

    private static final Logger log = LoggerFactory.getLogger(SseService.class);

    private final JsonMapper jsonMapper;

    /** @param jsonMapper mapper dell'applicazione, usato per convertire i valori dei filtri nel tipo dei campi */
    public SseService(JsonMapper jsonMapper) {
        this.jsonMapper = jsonMapper;
    }

    /**
     * Registra un nuovo client SSE per il canale specificato e inizializza eventuali filtri.
     *
     * <p>L’operazione:</p>
     * <ul>
     *     <li>verifica e prepara eventuali criteri di filtraggio dei messaggi e selezione dei campi;</li>
     *     <li>crea un {@link SseEmitter} con il timeout definito in configurazione;</li>
     *     <li>registra il client tramite il {@link SseRegistry} associandolo ad un canale;</li>
     *     <li>restituisce una {@link SseSubscriptionResponse} contenente session-id e metadati.</li>
     * </ul>
     *
     * @param channel canale SSE a cui sottoscriversi
     * @param filters eventuali filtri iniziali (può essere null)
     * @return informazioni sulla sottoscrizione
     * @throws InvalidFilterException se i filtri non sono applicabili al payload del canale
     */
    @Transactional
    @SuppressWarnings("unchecked")
    public SseSubscriptionResponse subscribe(@NotNull ChannelEnum channel, FilteringRequest filters) {
        //Filtri preparati prima della registrazione: se non sono validi il client non viene registrato
        final FilteringOptions<Object> filteringOptions =
                mapToFilteringOptions(filters, (Class<Object>) channel.payloadType(), jsonMapper);
        // timeout 0L = no timeout (gestito dal client)
        final SseEmitter emitter = new SseEmitter(properties.timeoutMs());
        final SseClient<Object> client = registry.subscribeClient(channel, emitter);
        client.setFilteringOptions(filteringOptions);

        return new SseSubscriptionResponse(client.sessionId(), channel);
    }

    /**
     * Aggiorna o rimuove le opzioni di filtraggio per il client identificato dal session-id.
     * I filtri sono applicati a livello di eventi e determinano quali eventi il client
     * potrà ricevere.
     *
     * <p>Il metodo gestisce tre casi:</p>
     * <ul>
     *     <li>aggiunta di filtri quando non presenti;</li>
     *     <li>rimozione dei filtri quando sono presenti;</li>
     *     <li>aggiornamento dei filtri esistenti.</li>
     * </ul>
     *
     * @param sessionId identificativo della sessione SSE
     * @param filters nuovi filtri richiesti dal client (nessun filtro per rimuoverli)
     * @throws NoSuchClientException se nessun client ha il session-id indicato
     * @throws InvalidFilterException se i filtri non sono applicabili al payload del canale
     */
    @Transactional
    public void updateFilters(@NotBlank String sessionId, FilteringRequest filters) {
        final SseClient<Object> client = registry.selectClient(sessionId);
        if (nullEquivalent(filters) && client.filteringOptions() == null) return;
        final FilteringOptions<Object> filteringOptions = mapToFilteringOptions(filters, client.payloadType(), jsonMapper);
        if (client.filteringOptions() == null && !nullEquivalent(filters)) {
            log.info("Adding filters to client with session-id '{}'", sessionId);
        } else if (client.filteringOptions() != null && nullEquivalent(filters)) {
            log.info("Removing filters from client with session-id '{}'", sessionId);
        } else {
            log.info("Updating filters for client with session-id '{}'", sessionId);
        }
        client.setFilteringOptions(filteringOptions);
    }

    /**
     * Avvia la connessione SSE per il client con il session-id fornito.
     * Se il client è già in streaming, l’operazione non viene ripetuta.
     *
     * <p>Il metodo:</p>
     * <ul>
     *     <li>recupera il client dal registry;</li>
     *     <li>configura l’emitter (heartbeat, callback di disconnessione, evento CONNECTED);</li>
     *     <li>imposta lo stato del client su STREAMING.</li>
     * </ul>
     *
     * @param sessionId identificativo della sessione SSE
     * @return l’emitter configurato e pronto allo streaming
     * @throws NoSuchClientException se nessun client ha il session-id indicato
     */
    public SseEmitter startStreaming(@NotBlank String sessionId) {
        final SseClient<?> client = registry.selectClient(sessionId);
        final SseEmitter emitter = client.emitter();
        if (client.status() == STREAMING) {
            log.warn("Client with session-id '{}' is already streaming", sessionId);
        } else {
            configureEmitter(client);
            client.setStatus(STREAMING);
        }
        return emitter;
    }

    /**
     * Disconnette il client associato al session-id e completa la sessione SSE.
     *
     * <p>Il metodo:</p>
     * <ul>
     *     <li>recupera il client dal registry;</li>
     *     <li>se il client non ha mai aperto lo stream, lo rimuove subito dal registry;</li>
     *     <li>tenta di inviare un evento DISCONNECTED;</li>
     *     <li>invoca {@code emitter.complete()} per chiudere la connessione;</li>
     *     <li>per i client in streaming il registry gestisce automaticamente la rimozione del client
     *     e l'eventuale clean-up del registry.</li>
     * </ul>
     *
     * @param sessionId identificativo della sessione SSE da chiudere
     * @throws NoSuchClientException se nessun client ha il session-id indicato
     */
    @Transactional
    public void unsubscribe(@NotBlank String sessionId) {
        final SseClient<?> client = registry.selectClient(sessionId);
        final SseEmitter emitter = client.emitter();
        //Le callback di rimozione sono agganciate all'apertura dello stream: senza stream si rimuove qui
        if (client.status() != STREAMING) {
            client.setStatus(DISABLED);
            registry.unsubscribeClient(client.channel(), sessionId);
            log.info("Unsubscribed client with session-id '{}' (streaming never started)", sessionId);
        }
        try {
            emitter.send(DISCONNECTED.sseEventBuilder());
        } catch (Exception _) {
            //emitter.complete();
        } finally {
            emitter.complete(); //Include automaticamente unsubscribe
        }
    }

    private void configureEmitter(@NotNull SseClient<?> client) {
        final SseEmitter emitter = client.emitter();
        final String sessionId = client.sessionId();
        final ChannelEnum channel = client.channel();
        try {
            // Invia al client l'evento "CONNECTED", includendo anche il tempo di reconnect
            // consigliato (usato dal browser per tentare la riconnessione automatica).
            emitter.send(CONNECTED.sseEventBuilder().reconnectTime(properties.clientRetryMs()));
        } catch (Exception e) {
            // Se l'invio iniziale fallisce, l’emitter viene chiuso con errore.
            emitter.completeWithError(e);
        }
        log.info("Client with session-id '{}' has started streaming", sessionId);

        // Task programmato per inviare heartbeat periodici
        ScheduledFuture<?> heartbeatTask = null;
        // Se l’heartbeat è abilitato e ha un intervallo valido…
        if (properties.heartbeatEnabled() && properties.heartbeatIntervalMs() > 0) {
            // Programma un task periodico che invia un evento HEARTBEAT al client
            heartbeatTask = taskScheduler.scheduleAtFixedRate(() -> {
                try {
                    emitter.send(HEARTBEAT.sseEventBuilder());
                } catch (Exception _) {
                    // Se l’invio fallisce significa che la connessione è rotta, quindi l’emitter viene chiuso
                    emitter.complete();
                }
            }, Duration.ofMillis(properties.heartbeatIntervalMs()));
        }
        // Salvataggio in variabile final per usarlo nelle lambda di unsubscribe
        final ScheduledFuture<?> finalHeartbeatTask = heartbeatTask;

        // Funzione di unsubscribe comune ai tre callback dell’emitter
        final Consumer<String> unsubscribe = message -> {
            //Interruzione heartbeat (se impostato)
            if (finalHeartbeatTask != null) finalHeartbeatTask.cancel(true);
            client.setStatus(DISABLED);
            log.info("{}. Unsubscribing client with session-id '{}' on channel '{}'..",
                    message, sessionId, channel.topic());
            registry.unsubscribeClient(channel, sessionId);
            log.info("Unsubscribed.");
        };

        // Callback invocato quando l’emitter completa normalmente lo stream
        emitter.onCompletion(() -> unsubscribe.accept("Transmission complete"));

        // Callback invocato quando scade il timeout dell’emitter
        emitter.onTimeout(() -> unsubscribe.accept("Transmission timed-out"));

        // Callback invocato quando avviene un errore nello stream SSE
        emitter.onError(e -> unsubscribe.accept(e.getMessage()));
    }
}
