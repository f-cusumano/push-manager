package com.fcusumano.push.registry;

import com.fcusumano.push.channel.ChannelEnum;
import com.fcusumano.push.client.StompClient;
import com.fcusumano.push.client.StompSubscription;
import com.fcusumano.push.event.EventType;
import com.fcusumano.push.event.StompPushEvent;
import com.fcusumano.push.exception.NoSuchClientException;
import com.fcusumano.push.exception.SubscriptionInUseException;
import com.fcusumano.push.filter.FieldSelector;
import com.fcusumano.push.filter.FilteringOptions;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.simp.SimpMessageHeaderAccessor;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Supplier;

import static com.fcusumano.push.client.ClientStatus.STREAMING;
import static com.fcusumano.push.util.StompMapper.toMessage;
import static com.fcusumano.push.util.StompMapper.toReceipt;


/**
 * Registro dei client STOMP per canale e per sessione, responsabile della consegna degli eventi
 * {@link StompPushEvent}: controparte di {@link SseRegistry}.
 * <p>
 * Ogni sottoscrizione STOMP è un {@link StompClient}; gli eventi vengono filtrati e proiettati per client
 * con le stesse regole di SSE e scritti direttamente sul {@code clientOutboundChannel} di Spring.
 * Lo stato di ogni sessione è modificato solo sotto il suo monitor, così una sottoscrizione concorrente
 * alla chiusura della sessione viene registrata e poi rimossa, oppure ignorata. Una sessione chiusa resta nota
 * per un intervallo di tempo, così un'apertura che arriva dopo la chiusura non la riapre.
 * </p>
 *
 * @author Francesco Cusumano
 */
public final class StompRegistry implements EventRegistry<StompPushEvent<?>> {

    private static final Logger log = LoggerFactory.getLogger(StompRegistry.class);
    private static final Duration CLOSED_SESSION_RETENTION = Duration.ofMinutes(1);

    private final Map<ChannelEnum, Set<StompClient<?>>> channel2clients = new ConcurrentHashMap<>(); //Set mai rimossi: i canali sono le costanti di un enum
    private final Map<String, SessionSubscriptions> session2subscriptions = new ConcurrentHashMap<>(); //Sessioni STOMP connesse e quelle chiuse da poco
    private final Queue<ClosedSession> closedSessions = new ConcurrentLinkedQueue<>(); //In ordine di chiusura: rimosse dalla mappa scaduto il tempo
    private final JsonMapper jsonMapper;
    private final Supplier<MessageChannel> clientOutboundChannel;
    private final long closedSessionRetentionNanos;


    /**
     * @param jsonMapper mapper dell'applicazione, usato per serializzare i payload e selezionarne i campi
     * @param clientOutboundChannel canale di Spring verso i client WebSocket, risolto al primo invio
     */
    public StompRegistry(JsonMapper jsonMapper, Supplier<MessageChannel> clientOutboundChannel) {
        this(jsonMapper, clientOutboundChannel, CLOSED_SESSION_RETENTION);
    }

    /**
     * @param jsonMapper mapper dell'applicazione, usato per serializzare i payload e selezionarne i campi
     * @param clientOutboundChannel canale di Spring verso i client WebSocket, risolto al primo invio
     * @param closedSessionRetention per quanto tempo una sessione chiusa resta nota (default 1 minuto)
     */
    public StompRegistry(JsonMapper jsonMapper, Supplier<MessageChannel> clientOutboundChannel, Duration closedSessionRetention) {
        this.jsonMapper = jsonMapper;
        this.clientOutboundChannel = clientOutboundChannel;
        this.closedSessionRetentionNanos = closedSessionRetention.toNanos();
    }

    /**
     * Registra una sessione STOMP connessa: solo le sessioni aperte accettano sottoscrizioni.
     * Una sessione già chiusa resta chiusa: la chiusura può arrivare prima dell'apertura.
     *
     * @param sessionId id della sessione
     */
    public void openSession(@NotBlank String sessionId) {
        removeExpiredSessions();
        session2subscriptions.putIfAbsent(sessionId, new SessionSubscriptions());
    }

    /**
     * Registra un nuovo client in stato {@code SUBSCRIBED} con i suoi filtri.
     *
     * @param subscription identità del client
     * @param channel canale sottoscritto
     * @param destination destinazione della sottoscrizione
     * @param filteringOptions filtri del client (opzionali)
     * @return il client, {@code null} se la sessione non è connessa o è già chiusa
     * @throws SubscriptionInUseException se l'id appartiene già alla libreria (client registrato o sottoscrizione rifiutata)
     */
    public StompClient<Object> subscribeClient(@NotNull StompSubscription subscription, @NotNull ChannelEnum channel,
                                               @NotBlank String destination, FilteringOptions<Object> filteringOptions) {
        final SessionSubscriptions session = session2subscriptions.get(subscription.sessionId());
        if (session == null) return null;
        final StompClient<Object> client = new StompClient<>(subscription, channel, destination);
        client.setFilteringOptions(filteringOptions);
        synchronized (session) {
            if (session.closed) return null;
            //Controllo e registrazione sotto lo stesso monitor: due SUBSCRIBE concorrenti con lo stesso id (es. SockJS)
            if (session.owns(subscription.subscriptionId())) {
                throw new SubscriptionInUseException("Subscription id '%s' already in use in session '%s'",
                        subscription.subscriptionId(), subscription.sessionId());
            }
            session.subscription2client.put(subscription.subscriptionId(), client);
            channel2clients.computeIfAbsent(channel, _ -> ConcurrentHashMap.newKeySet()).add(client);
        }
        log.debug("New client {} registered. Total on channel (subscribed/streaming): {}", client, countClients(channel));
        return client;
    }

    /**
     * Registra l'id di una sottoscrizione rifiutata: la sua {@code UNSUBSCRIBE} appartiene alla libreria.
     * L'id di un client registrato resta del client.
     *
     * @param subscription sottoscrizione rifiutata
     */
    public void orphanSubscription(@NotNull StompSubscription subscription) {
        final SessionSubscriptions session = session2subscriptions.get(subscription.sessionId());
        if (session == null) return;
        synchronized (session) {
            if (!session.closed && !session.subscription2client.containsKey(subscription.subscriptionId())) {
                session.orphanSubscriptionIds.add(subscription.subscriptionId());
            }
        }
    }

    /**
     * @param subscription sottoscrizione da verificare
     * @return {@code true} se l'id appartiene alla libreria (client registrato o sottoscrizione rifiutata)
     */
    public boolean isKnownSubscription(@NotNull StompSubscription subscription) {
        final SessionSubscriptions session = session2subscriptions.get(subscription.sessionId());
        if (session == null) return false;
        synchronized (session) {
            return session.owns(subscription.subscriptionId());
        }
    }

    /**
     * @param subscription identità del client
     * @return il client
     * @throws NoSuchClientException se nessun client ha la sottoscrizione indicata
     */
    @SuppressWarnings("unchecked")
    public StompClient<Object> selectClient(@NotNull StompSubscription subscription) {
        final SessionSubscriptions session = session2subscriptions.get(subscription.sessionId());
        StompClient<?> client = null;
        if (session != null) {
            synchronized (session) {
                client = session.subscription2client.get(subscription.subscriptionId());
            }
        }
        if (client == null) {
            throw new NoSuchClientException("No client with subscription '%s' in session '%s'",
                    subscription.subscriptionId(), subscription.sessionId());
        }
        return (StompClient<Object>) client;
    }

    /**
     * Disabilita e rimuove il client, oppure dimentica la sottoscrizione rifiutata.
     *
     * @param subscription identità del client
     * @return {@code true} se l'id apparteneva alla libreria, {@code false} per id o sessioni sconosciuti
     */
    public boolean unsubscribeClient(@NotNull StompSubscription subscription) {
        final SessionSubscriptions session = session2subscriptions.get(subscription.sessionId());
        if (session == null) return false;
        synchronized (session) {
            final StompClient<?> client = session.subscription2client.remove(subscription.subscriptionId());
            if (client == null) return session.orphanSubscriptionIds.remove(subscription.subscriptionId());
            removeClient(client);
            return true;
        }
    }

    /**
     * Chiude la sessione rimuovendo tutti i suoi client e le sottoscrizioni rifiutate; è idempotente.
     * Anche una sessione non ancora aperta viene ricordata come chiusa.
     *
     * @param sessionId id della sessione
     * @return il numero di client rimossi
     */
    public int closeSession(@NotBlank String sessionId) {
        removeExpiredSessions();
        final SessionSubscriptions session = session2subscriptions.computeIfAbsent(sessionId, _ -> new SessionSubscriptions());
        final int removed;
        synchronized (session) {
            if (session.closed) return 0;
            session.closed = true;
            for (StompClient<?> client : session.subscription2client.values()) {
                removeClient(client);
            }
            removed = session.subscription2client.size();
            session.subscription2client.clear();
            session.orphanSubscriptionIds.clear();
        }
        closedSessions.add(new ClosedSession(sessionId, session, System.nanoTime() + closedSessionRetentionNanos));
        return removed;
    }

    /**
     * @param channel canale
     * @return il numero di client del canale (sottoscritti e in streaming)
     */
    public int countClients(@NotNull ChannelEnum channel) {
        final Set<StompClient<?>> clients = channel2clients.get(channel);
        return clients == null ? 0 : clients.size();
    }

    /**
     * Invia l'evento a tutti i client in streaming sul canale dell'evento che lo accettano con i propri filtri,
     * con il payload intero o con i soli campi selezionati; i client con i campi selezionati tutti vuoti
     * non ricevono l'evento. Se il filtraggio o l'invio a un client falliscono si prosegue con gli altri.
     *
     * @param event un evento pubblicato su un canale
     * @param <T> il tipo di payload del canale
     * @return il numero di messaggi accettati dal {@code clientOutboundChannel}
     */
    public <T> int broadcast(@NotNull StompPushEvent<T> event) {
        final Set<StompClient<?>> clients = channel2clients.get(event.getChannel());
        if (CollectionUtils.isEmpty(clients)) return 0;
        byte[] fullPayload = null; //Condiviso dai client senza selezione dei campi
        JsonNode payloadTree = null; //Calcolato una volta sola, se almeno un client seleziona dei campi
        int delivered = 0;
        for (StompClient<?> rawClient : clients) { //Iterazione debolmente consistente: nessuna copia del set
            if (rawClient.status() != STREAMING) continue;
            @SuppressWarnings("unchecked") final StompClient<T> client = (StompClient<T>) rawClient;
            final FilteringOptions<T> filteringOptions = client.filteringOptions(); //Letto una volta: può cambiare
            final byte[] payload;
            try {
                if (filteringOptions != null && !filteringOptions.accepts(event)) continue; //Tipo (non muteable sempre ammessi) e contenuto
                final FieldSelector fieldSelector = filteringOptions == null ? null : filteringOptions.fieldSelector();
                if (fieldSelector == null) {
                    if (fullPayload == null) fullPayload = jsonMapper.writeValueAsBytes(event.getData());
                    payload = fullPayload;
                } else {
                    if (payloadTree == null) payloadTree = jsonMapper.valueToTree(event.getData());
                    final JsonNode selectedFields = fieldSelector.select(payloadTree);
                    if (selectedFields == null) continue; //Campi selezionati tutti vuoti: evento non inviato a questo client
                    payload = jsonMapper.writeValueAsBytes(selectedFields);
                }
            } catch (Exception e) {
                log.warn("Filtering failed for client {} on channel '{}'. Message skipped.", client, event.getChannel(), e);
                continue;
            }
            if (send(toMessage(client, event, payload))) delivered++;
        }
        return delivered;
    }

    /**
     * Invia al client un messaggio di sistema (es. {@code CONNECTED}, {@code ERROR}).
     *
     * @param client destinatario
     * @param eventType tipo dell'evento
     * @param body corpo del messaggio, serializzato in JSON
     * @param retryMs tempo di riconnessione (opzionale)
     * @return {@code true} se il messaggio è stato accettato dal {@code clientOutboundChannel}
     */
    public boolean send(@NotNull StompClient<?> client, @NotNull EventType eventType, Object body, Long retryMs) {
        return send(client.subscription(), client.destination(), eventType, body, retryMs);
    }

    /**
     * Invia un messaggio di sistema a una sottoscrizione, anche senza client registrato (es. rifiutata).
     *
     * @param subscription destinatario
     * @param destination destinazione riportata nel messaggio
     * @param eventType tipo dell'evento
     * @param body corpo del messaggio, serializzato in JSON
     * @param retryMs tempo di riconnessione (opzionale)
     * @return {@code true} se il messaggio è stato accettato dal {@code clientOutboundChannel}
     */
    public boolean send(@NotNull StompSubscription subscription, @NotBlank String destination, @NotNull EventType eventType,
                        Object body, Long retryMs) {
        return send(toMessage(subscription, destination, eventType, UUID.randomUUID().toString(), retryMs,
                jsonMapper.writeValueAsBytes(body)));
    }

    /**
     * Conferma un frame del client con un {@code RECEIPT}, se richiesto.
     *
     * @param sessionId sessione destinataria
     * @param receiptId valore dell'header {@code receipt} del frame (opzionale)
     * @return {@code true} se il {@code RECEIPT} è stato accettato dal {@code clientOutboundChannel}
     */
    public boolean sendReceipt(@NotBlank String sessionId, String receiptId) {
        if (!StringUtils.hasText(receiptId)) return false;
        return send(toReceipt(sessionId, receiptId));
    }

    private boolean send(Message<byte[]> message) {
        try {
            return clientOutboundChannel.get().send(message);
        } catch (RuntimeException e) { //Es. executor saturo: gli altri client proseguono
            log.warn("Message to session '{}' not sent", SimpMessageHeaderAccessor.getSessionId(message.getHeaders()), e);
            return false;
        }
    }

    //Sessioni chiuse da più di closedSessionRetention, rimosse in ordine di chiusura: nessuna apertura tardiva è più attesa
    private void removeExpiredSessions() {
        final long now = System.nanoTime();
        for (ClosedSession closed = closedSessions.peek(); closed != null && now - closed.expiresAtNanos() >= 0; closed = closedSessions.peek()) {
            if (closedSessions.remove(closed)) session2subscriptions.remove(closed.sessionId(), closed.session());
        }
    }

    //Chiamato sotto il monitor della sessione
    private void removeClient(StompClient<?> client) {
        client.disable();
        final Set<StompClient<?>> clients = channel2clients.get(client.channel());
        if (clients != null) clients.remove(client);
        log.debug("Client {} unregistered. Total on channel: {}", client, countClients(client.channel()));
    }

    private static final class SessionSubscriptions { //Stato di una sessione: accesso sempre sincronizzato sull'istanza

        private final Map<String, StompClient<?>> subscription2client = new HashMap<>();
        private final Set<String> orphanSubscriptionIds = new HashSet<>(); //Della libreria ma senza client (rifiutate): la loro UNSUBSCRIBE va trattenuta
        private boolean closed;

        //Id della libreria: client registrato o sottoscrizione rifiutata
        private boolean owns(String subscriptionId) {
            return subscription2client.containsKey(subscriptionId) || orphanSubscriptionIds.contains(subscriptionId);
        }
    }

    private record ClosedSession(String sessionId, SessionSubscriptions session, long expiresAtNanos) {} //Sessione chiusa in attesa di rimozione
}
