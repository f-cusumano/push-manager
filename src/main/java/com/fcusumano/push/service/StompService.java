package com.fcusumano.push.service;

import com.fcusumano.push.channel.ChannelEnum;
import com.fcusumano.push.client.StompClient;
import com.fcusumano.push.client.StompSubscription;
import com.fcusumano.push.controller.StompErrorResponse;
import com.fcusumano.push.controller.StompSubscriptionResponse;
import com.fcusumano.push.event.StompPushEvent;
import com.fcusumano.push.exception.InvalidFilterException;
import com.fcusumano.push.exception.NoSuchClientException;
import com.fcusumano.push.exception.SubscriptionInUseException;
import com.fcusumano.push.filter.FilteringOptions;
import com.fcusumano.push.filter.FilteringRequest;
import com.fcusumano.push.filter.StreamingFilterRequest;
import com.fcusumano.push.registry.StompRegistry;
import com.fcusumano.push.util.StompDestinations;
import jakarta.validation.ConstraintViolation;
import jakarta.validation.ValidationException;
import jakarta.validation.Validator;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.StringUtils;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.json.JsonMapper;

import java.util.ArrayList;
import java.util.List;

import static com.fcusumano.push.event.EventType.CONNECTED;
import static com.fcusumano.push.event.EventType.ERROR;
import static com.fcusumano.push.filter.FilteringRequest.nullEquivalent;
import static com.fcusumano.push.util.PushMapper.mapToFilteringOptions;


/**
 * Servizio che gestisce il ciclo di vita dei client STOMP, controparte di {@link SseService}:
 * sottoscrizione con filtri iniziali, modifica dei filtri, disiscrizione e chiusura delle sessioni.
 * <p>
 * I metodi vengono chiamati per i frame STOMP del client e non sollevano mai eccezioni: gli errori
 * arrivano al client come messaggi {@code ERROR} sulla sottoscrizione, mai come frame STOMP {@code ERROR},
 * che chiuderebbe la connessione. Niente {@code @Transactional}, come per {@code SseService.startStreaming}:
 * una transazione impegnerebbe una connessione al database senza motivo.
 * </p>
 *
 * @author Francesco Cusumano
 */
public class StompService extends PushService<StompPushEvent<?>, StompRegistry> {

    private static final Logger log = LoggerFactory.getLogger(StompService.class);
    private static final List<String> CONTENT_FILTER_FIELDS = List.of("fieldPath", "check"); //Campi di StreamingFilterRequest con vincoli propri

    private final JsonMapper jsonMapper;
    private final Validator validator; //Opzionale: senza validator i filtri non sono validati con Jakarta
    private final StompDestinations destinations;


    /**
     * @param jsonMapper mapper dell'applicazione, usato per leggere i filtri e convertirne i valori
     * @param validator validator Jakarta dei filtri (opzionale)
     * @param destinations destinazioni riservate alla libreria
     */
    public StompService(JsonMapper jsonMapper, Validator validator, StompDestinations destinations) {
        this.jsonMapper = jsonMapper;
        this.validator = validator;
        this.destinations = destinations;
    }

    /** @return le destinazioni riservate alla libreria */
    public StompDestinations destinations() {
        return destinations;
    }

    /**
     * Apre la sessione STOMP: solo le sessioni connesse accettano sottoscrizioni.
     *
     * @param sessionId id della sessione
     */
    public void connect(@NotBlank String sessionId) {
        registry.openSession(sessionId);
        log.debug("STOMP session '{}' connected", sessionId);
    }

    /**
     * Sottoscrive il client al canale: se i filtri sono validi invia l'evento {@code CONNECTED},
     * avvia lo streaming e conferma con il {@code RECEIPT} richiesto, altrimenti rifiuta la sottoscrizione.
     *
     * @param subscription identità del client
     * @param channel canale sottoscritto
     * @param filters filtri iniziali, {@link FilteringRequest} in JSON (opzionale)
     * @param receipt id del {@code RECEIPT} richiesto dal client (opzionale)
     */
    @SuppressWarnings("unchecked")
    public void subscribe(@NotNull StompSubscription subscription, @NotNull ChannelEnum channel, String filters, String receipt) {
        final String destination = destinations.channel(channel);
        if (registry.isKnownSubscription(subscription)) {
            rejectDuplicate(subscription, destination);
            return;
        }
        StompClient<Object> client = null; //Valorizzato alla registrazione: se poi qualcosa fallisce va rimosso
        try {
            //Filtri preparati prima della registrazione: se non sono validi il client non viene registrato
            final FilteringOptions<Object> filteringOptions =
                    mapToFilteringOptions(toFilteringRequest(filters), (Class<Object>) channel.payloadType(), jsonMapper);
            client = registry.subscribeClient(subscription, channel, destination, filteringOptions);
            if (client == null) {
                log.warn("Subscription {} ignored: session not connected", subscription);
                return;
            }
            registry.send(client, CONNECTED, StompSubscriptionResponse.of(client),
                    properties.clientRetryMs() > 0 ? properties.clientRetryMs() : null);
            client.activate(); //Dopo CONNECTED: nessun evento viene accodato prima
            registry.sendReceipt(subscription.sessionId(), receipt);
            log.info("Client {} has started streaming on channel '{}'", subscription, channel.topic());
        } catch (SubscriptionInUseException e) { //SUBSCRIBE concorrente con lo stesso id, registrata per prima
            rejectDuplicate(subscription, destination);
        } catch (InvalidFilterException e) {
            reject(subscription, destination, e.getMessage());
        } catch (RuntimeException e) { //Il client deve comunque ricevere una risposta e la sua UNSUBSCRIBE va trattenuta
            log.error("Subscription {} failed", subscription, e);
            if (client != null) registry.unsubscribeClient(subscription); //Già registrato: rifiutato, non deve restare tra i client
            reject(subscription, destination, "Subscription failed");
        }
    }

    /**
     * Rifiuta la sottoscrizione: l'id resta della libreria, così la sua {@code UNSUBSCRIBE} non raggiunge il broker.
     *
     * @param subscription identità del client
     * @param destination destinazione della sottoscrizione
     * @param reason motivo, inviato al client nel messaggio {@code ERROR}
     */
    public void reject(@NotNull StompSubscription subscription, String destination, String reason) {
        log.warn("Subscription {} to '{}' rejected: {}", subscription, destination, reason);
        registry.orphanSubscription(subscription);
        registry.send(subscription, destination, ERROR, StompErrorResponse.of(subscription, destination, reason), null);
    }

    /**
     * Sostituisce i filtri del client, come {@code PUT /sse/filter}: nessun filtro per rimuoverli.
     * Se i filtri non sono validi restano in vigore i precedenti e il client riceve un messaggio {@code ERROR}.
     *
     * @param subscription identità del client
     * @param filters nuovi filtri, {@link FilteringRequest} in JSON (opzionale)
     * @param receipt id del {@code RECEIPT} richiesto dal client (opzionale)
     */
    public void updateFilters(@NotNull StompSubscription subscription, String filters, String receipt) {
        final StompClient<Object> client;
        try {
            client = registry.selectClient(subscription);
        } catch (NoSuchClientException e) {
            log.warn("Filters update for {} rejected: {}", subscription, e.getMessage());
            final String destination = destinations.filter(subscription.subscriptionId());
            registry.send(subscription, destination, ERROR, StompErrorResponse.of(subscription, destination, e.getMessage()), null);
            return;
        }
        try {
            final FilteringRequest request = toFilteringRequest(filters);
            if (!(nullEquivalent(request) && client.filteringOptions() == null)) {
                final FilteringOptions<Object> filteringOptions = mapToFilteringOptions(request, client.payloadType(), jsonMapper);
                if (client.filteringOptions() == null && !nullEquivalent(request)) {
                    log.info("Adding filters to client {}", subscription);
                } else if (client.filteringOptions() != null && nullEquivalent(request)) {
                    log.info("Removing filters from client {}", subscription);
                } else {
                    log.info("Updating filters for client {}", subscription);
                }
                client.setFilteringOptions(filteringOptions);
            }
            registry.sendReceipt(subscription.sessionId(), receipt);
        } catch (InvalidFilterException e) { //I filtri precedenti restano in vigore
            log.warn("Filters update for {} rejected: {}", subscription, e.getMessage());
            registry.send(client, ERROR, StompErrorResponse.of(subscription, client.destination(), e.getMessage()), null);
        } catch (RuntimeException e) { //Es. validator in errore: il client deve comunque ricevere una risposta, i filtri precedenti restano in vigore
            log.error("Filters update for {} failed", subscription, e);
            registry.send(client, ERROR, StompErrorResponse.of(subscription, client.destination(), "Filters update failed"), null);
        }
    }

    /**
     * Disiscrive il client, o dimentica la sottoscrizione rifiutata, e conferma con il {@code RECEIPT} richiesto.
     *
     * @param subscription identità del client
     * @param receipt id del {@code RECEIPT} richiesto dal client (opzionale)
     * @return {@code true} se la sottoscrizione apparteneva alla libreria, {@code false} se è dell'applicazione
     */
    public boolean unsubscribe(@NotNull StompSubscription subscription, String receipt) {
        if (!registry.unsubscribeClient(subscription)) return false;
        log.info("Unsubscribed client {}", subscription);
        registry.sendReceipt(subscription.sessionId(), receipt);
        return true;
    }

    /**
     * Chiude la sessione STOMP disiscrivendo tutti i suoi client; è idempotente.
     *
     * @param sessionId id della sessione
     */
    public void disconnect(@NotBlank String sessionId) {
        final int removed = registry.closeSession(sessionId);
        if (removed > 0) log.info("Session '{}' closed: unsubscribed {} clients", sessionId, removed);
    }

    //Id già della libreria: la sottoscrizione esistente resta valida, nessun orfano
    private void rejectDuplicate(StompSubscription subscription, String destination) {
        log.warn("Subscription {} to '{}' rejected: id already in use", subscription, destination);
        registry.send(subscription, destination, ERROR, StompErrorResponse.of(subscription, destination,
                "Subscription id '" + subscription.subscriptionId() + "' already in use"), null);
    }

    //Legge i filtri JSON e li valida con le stesse regole di @Valid in SseController
    private FilteringRequest toFilteringRequest(String filters) {
        if (!StringUtils.hasText(filters)) return null;
        final FilteringRequest request;
        try {
            request = jsonMapper.readValue(filters, FilteringRequest.class);
        } catch (JacksonException e) {
            throw new InvalidFilterException("Filters are not a valid FilteringRequest: %s", e.getOriginalMessage());
        }
        if (request == null || validator == null) return request;
        final List<String> violations = violations(request);
        if (violations.isEmpty()) return request;
        throw new InvalidFilterException(String.join("; ", violations));
    }

    //Violazioni Jakarta nel formato 'percorso: messaggio', ordinate
    private List<String> violations(FilteringRequest request) {
        try {
            return validator.validate(request).stream()
                    .map(v -> v.getPropertyPath() + ": " + v.getMessage())
                    .sorted()
                    .toList();
        } catch (ValidationException e) { //Senza 'check' il validator di StreamingFilterRequest fallisce (HV000028) invece di segnalare la violazione
            final List<String> violations = contentFilterViolations(request.contentFilters());
            return violations.isEmpty() ? List.of("Filters could not be validated: " + e.getMessage()) : violations;
        }
    }

    //Violazioni dei campi di ogni filtro sul contenuto, senza il vincolo di classe
    private List<String> contentFilterViolations(List<StreamingFilterRequest> contentFilters) {
        final List<String> violations = new ArrayList<>();
        for (int i = 0; contentFilters != null && i < contentFilters.size(); i++) {
            final StreamingFilterRequest filter = contentFilters.get(i);
            if (filter == null) continue;
            for (String field : CONTENT_FILTER_FIELDS) {
                for (ConstraintViolation<StreamingFilterRequest> violation : validator.validateProperty(filter, field)) {
                    violations.add("contentFilters[" + i + "]." + violation.getPropertyPath() + ": " + violation.getMessage());
                }
            }
        }
        return violations.stream().sorted().toList();
    }
}
