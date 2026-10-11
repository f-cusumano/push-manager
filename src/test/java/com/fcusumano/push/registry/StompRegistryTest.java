package com.fcusumano.push.registry;

import com.fcusumano.push.CapturingChannel;
import com.fcusumano.push.TestPayloads.UserDto;
import com.fcusumano.push.client.StompClient;
import com.fcusumano.push.client.StompSubscription;
import com.fcusumano.push.event.EventType;
import com.fcusumano.push.event.StompPushEvent;
import com.fcusumano.push.exception.NoSuchClientException;
import com.fcusumano.push.exception.SubscriptionInUseException;
import com.fcusumano.push.filter.FilteringOptions;
import com.fcusumano.push.filter.FilteringRequest;
import com.fcusumano.push.util.PushMapper;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageDeliveryException;
import org.springframework.messaging.simp.SimpMessageHeaderAccessor;
import tools.jackson.databind.json.JsonMapper;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static com.fcusumano.push.CapturingChannel.body;
import static com.fcusumano.push.CapturingChannel.eventType;
import static com.fcusumano.push.CapturingChannel.json;
import static com.fcusumano.push.TestPayloads.TestChannels.USERS;
import static com.fcusumano.push.TestPayloads.anonymous;
import static com.fcusumano.push.TestPayloads.user1;
import static com.fcusumano.push.client.ClientStatus.DISABLED;
import static com.fcusumano.push.client.ClientStatus.STREAMING;

class StompRegistryTest {

    private static final JsonMapper MAPPER = JsonMapper.builder().build();
    private static final String DESTINATION = "/push/USERS";
    private static final StompSubscription SUB_0 = StompSubscription.of("SESSION-1", "sub-0");
    private static final StompSubscription SUB_1 = StompSubscription.of("SESSION-1", "sub-1");

    private final CapturingChannel channel = new CapturingChannel();
    private final StompRegistry registry = new StompRegistry(MAPPER, () -> channel);

    @Test
    void subscriptionsOfSameSessionGetTheirOwnProjection() {
        streamingClient(SUB_0, fields("name"));
        streamingClient(SUB_1, null);
        Assertions.assertEquals(2, registry.broadcast(StompPushEvent.of(user1(), EventType.CREATED, USERS)));
        Assertions.assertEquals(json("{\"name\":\"FOO-1\"}"), body(channel.messages("sub-0").getFirst()));
        Assertions.assertEquals(json(MAPPER.writeValueAsString(user1())), body(channel.messages("sub-1").getFirst()));
        Assertions.assertEquals("CREATED", eventType(channel.messages("sub-0").getFirst()));
        Assertions.assertEquals(DESTINATION, SimpMessageHeaderAccessor.getDestination(channel.messages("sub-1").getFirst().getHeaders()));
    }

    @Test
    void emptySelectionIsNotSent() {
        streamingClient(SUB_0, fields("name", "address.city"));
        Assertions.assertEquals(0, registry.broadcast(StompPushEvent.of(anonymous(), EventType.CREATED, USERS)));
        Assertions.assertTrue(channel.isEmpty());
    }

    @Test
    void notRequiredEventTypeIsNotSent() {
        streamingClient(SUB_0, eventTypes(EventType.CREATED));
        Assertions.assertEquals(0, registry.broadcast(StompPushEvent.of(user1(), EventType.UPDATED, USERS)));
        Assertions.assertTrue(channel.isEmpty());
        Assertions.assertEquals(1, registry.broadcast(StompPushEvent.of(user1(), EventType.CREATED, USERS)));
    }

    @Test
    void nonMuteableEventsAlwaysPass() {
        streamingClient(SUB_0, eventTypes(EventType.CREATED));
        for (EventType eventType : EventType.values()) {
            Assertions.assertEquals(switch (eventType) {
                case CONNECTED, DISCONNECTED, HEARTBEAT, ERROR -> 1; //Non muteable: mai esclusi dai filtri
                case CREATED -> 1;
                case MESSAGE, UPDATED, DELETED -> 0;
            }, registry.broadcast(StompPushEvent.of(user1(), eventType, USERS)), eventType.name());
        }
    }

    @Test
    void notStreamingClientsAreSkipped() {
        final StompPushEvent<UserDto> event = StompPushEvent.of(user1(), EventType.CREATED, USERS);
        registry.openSession("SESSION-1");
        final StompClient<Object> client = registry.subscribeClient(SUB_0, USERS, DESTINATION, null);
        //Sottoscritto ma CONNECTED non ancora inviato
        Assertions.assertEquals(0, registry.broadcast(event));
        Assertions.assertTrue(client.activate());
        Assertions.assertEquals(1, registry.broadcast(event));
        client.disable();
        Assertions.assertEquals(0, registry.broadcast(event));
        Assertions.assertFalse(client.activate()); //Un client disabilitato non torna in streaming
    }

    @Test
    void subscribeWithoutOpenSessionReturnsNull() {
        Assertions.assertNull(registry.subscribeClient(SUB_0, USERS, DESTINATION, null));
        Assertions.assertEquals(0, registry.countClients(USERS));
        registry.orphanSubscription(SUB_1);
        Assertions.assertFalse(registry.isKnownSubscription(SUB_0));
        Assertions.assertFalse(registry.isKnownSubscription(SUB_1));
    }

    @Test
    void subscribeAfterCloseReturnsNull() {
        registry.openSession("SESSION-1");
        registry.closeSession("SESSION-1");
        Assertions.assertNull(registry.subscribeClient(SUB_0, USERS, DESTINATION, null));
        Assertions.assertEquals(0, registry.countClients(USERS));
    }

    @Test
    void unsubscribeRemovesClientOrOrphanOnce() {
        final StompClient<Object> client = streamingClient(SUB_0, null);
        registry.orphanSubscription(SUB_1);
        Assertions.assertTrue(registry.isKnownSubscription(SUB_0));
        Assertions.assertTrue(registry.isKnownSubscription(SUB_1));

        Assertions.assertTrue(registry.unsubscribeClient(SUB_0));
        Assertions.assertFalse(registry.unsubscribeClient(SUB_0));
        Assertions.assertEquals(DISABLED, client.status());
        Assertions.assertEquals(0, registry.countClients(USERS));
        //Sottoscrizione rifiutata: nessun client, ma l'id è della libreria fino alla UNSUBSCRIBE
        Assertions.assertTrue(registry.unsubscribeClient(SUB_1));
        Assertions.assertFalse(registry.unsubscribeClient(SUB_1));
        Assertions.assertFalse(registry.isKnownSubscription(SUB_0));
        Assertions.assertFalse(registry.isKnownSubscription(SUB_1));
        Assertions.assertFalse(registry.unsubscribeClient(StompSubscription.of("SESSION-9", "sub-0")));
    }

    @Test
    void closeSessionRemovesAllAndIsIdempotent() {
        final StompClient<Object> first = streamingClient(SUB_0, null);
        final StompClient<Object> second = streamingClient(SUB_1, fields("name"));
        final StompClient<Object> otherSession = streamingClient(StompSubscription.of("SESSION-2", "sub-0"), null);
        final StompSubscription orphan = StompSubscription.of("SESSION-1", "sub-2");
        registry.orphanSubscription(orphan);

        Assertions.assertEquals(2, registry.closeSession("SESSION-1"));
        Assertions.assertEquals(0, registry.closeSession("SESSION-1"));
        Assertions.assertEquals(DISABLED, first.status());
        Assertions.assertEquals(DISABLED, second.status());
        Assertions.assertFalse(registry.isKnownSubscription(orphan));
        //L'altra sessione non è toccata
        Assertions.assertEquals(STREAMING, otherSession.status());
        Assertions.assertEquals(1, registry.countClients(USERS));
        Assertions.assertEquals(1, registry.broadcast(StompPushEvent.of(user1(), EventType.CREATED, USERS)));
    }

    @Test
    void duplicateIdIsRejectedUnderTheSessionMonitor() {
        final StompClient<Object> client = streamingClient(SUB_0, null);
        registry.orphanSubscription(SUB_1);
        //Anche dopo il controllo preventivo del service: due SUBSCRIBE concorrenti con lo stesso id
        for (StompSubscription subscription : List.of(SUB_0, SUB_1)) {
            try {
                registry.subscribeClient(subscription, USERS, DESTINATION, null);
                Assertions.fail("Subscription " + subscription + " should be in use");
            } catch (SubscriptionInUseException e) {
                Assertions.assertEquals("Subscription id '" + subscription.subscriptionId() + "' already in use in session 'SESSION-1'", e.getMessage());
            }
        }
        Assertions.assertEquals(1, registry.countClients(USERS));
        Assertions.assertSame(client, registry.selectClient(SUB_0));
        //L'id di un client registrato non diventa orfano: la sua UNSUBSCRIBE è della libreria una volta sola
        registry.orphanSubscription(SUB_0);
        Assertions.assertTrue(registry.unsubscribeClient(SUB_0));
        Assertions.assertFalse(registry.unsubscribeClient(SUB_0));
    }

    @Test
    void openAfterCloseKeepsTheSessionClosed() {
        //La chiusura può arrivare prima dell'apertura, es. CONNECTED non scritto su un altro thread
        Assertions.assertEquals(0, registry.closeSession("SESSION-1"));
        registry.openSession("SESSION-1");
        Assertions.assertNull(registry.subscribeClient(SUB_0, USERS, DESTINATION, null));
        registry.orphanSubscription(SUB_1);
        Assertions.assertFalse(registry.isKnownSubscription(SUB_1));
        Assertions.assertEquals(0, registry.countClients(USERS));
        Assertions.assertEquals(0, registry.closeSession("SESSION-1"));
    }

    @Test
    void closedSessionsAreForgottenAfterRetention() {
        final StompRegistry registryWithoutRetention = new StompRegistry(MAPPER, () -> channel, Duration.ZERO);
        registryWithoutRetention.openSession("SESSION-1");
        registryWithoutRetention.closeSession("SESSION-1");
        //Scaduto il tempo la sessione chiusa viene rimossa dalla mappa: lo stesso id torna a essere nuovo
        registryWithoutRetention.openSession("SESSION-1");
        Assertions.assertNotNull(registryWithoutRetention.subscribeClient(SUB_0, USERS, DESTINATION, null));
    }

    @Test
    void unknownClientIsNotFound() {
        registry.openSession("SESSION-1");
        registry.orphanSubscription(SUB_1); //Le sottoscrizioni rifiutate non hanno client
        for (StompSubscription subscription : List.of(SUB_0, SUB_1, StompSubscription.of("SESSION-9", "sub-0"))) {
            try {
                registry.selectClient(subscription);
                Assertions.fail("Subscription " + subscription + " should not be found");
            } catch (NoSuchClientException e) {
                Assertions.assertEquals("No client with subscription '" + subscription.subscriptionId() + "' in session '"
                                        + subscription.sessionId() + "'", e.getMessage());
            }
        }
    }

    @Test
    void failedSendDoesNotStopBroadcast() {
        //Invio rifiutato per una sola sessione, es. executor saturo
        final MessageChannel failing = (message, timeout) -> {
            if ("SESSION-1".equals(SimpMessageHeaderAccessor.getSessionId(message.getHeaders()))) {
                throw new MessageDeliveryException(message, "Executor rejected the message");
            }
            return channel.send(message, timeout);
        };
        final StompRegistry failingRegistry = new StompRegistry(MAPPER, () -> failing);
        streamingClient(failingRegistry, SUB_0, null);
        streamingClient(failingRegistry, StompSubscription.of("SESSION-2", "sub-0"), null);

        Assertions.assertEquals(1, failingRegistry.broadcast(StompPushEvent.of(user1(), EventType.CREATED, USERS)));
        Assertions.assertEquals("SESSION-2", SimpMessageHeaderAccessor.getSessionId(channel.singleMessage().getHeaders()));
        Assertions.assertFalse(failingRegistry.sendReceipt("SESSION-1", "r-1"));
        Assertions.assertTrue(failingRegistry.sendReceipt("SESSION-2", "r-1"));
    }

    @Test
    void receiptOnlyWhenRequested() {
        Assertions.assertFalse(registry.sendReceipt("SESSION-1", null));
        Assertions.assertFalse(registry.sendReceipt("SESSION-1", " "));
        Assertions.assertTrue(channel.isEmpty());
        Assertions.assertTrue(registry.sendReceipt("SESSION-1", "r-1"));
        Assertions.assertEquals(List.of("r-1"), channel.receiptIds());
    }

    @Test
    void concurrentSubscribeAndCloseLeaveNoClient() throws Exception {
        try (ExecutorService executor = Executors.newFixedThreadPool(2)) {
            for (int i = 0; i < 1000; i++) {
                final String sessionId = "SESSION-" + i;
                registry.openSession(sessionId);
                final CountDownLatch start = new CountDownLatch(1);
                final Future<StompClient<Object>> subscribed = executor.submit(() -> {
                    start.await();
                    return registry.subscribeClient(StompSubscription.of(sessionId, "sub-0"), USERS, DESTINATION, null);
                });
                final Future<Integer> closed = executor.submit(() -> {
                    start.await();
                    return registry.closeSession(sessionId);
                });
                start.countDown();
                final StompClient<Object> client = subscribed.get(5, TimeUnit.SECONDS);
                //Registrato e poi rimosso dalla chiusura, oppure ignorato
                Assertions.assertEquals(client == null ? 0 : 1, closed.get(5, TimeUnit.SECONDS));
                if (client != null) Assertions.assertEquals(DISABLED, client.status());
            }
        }
        Assertions.assertEquals(0, registry.countClients(USERS));
    }

    private StompClient<Object> streamingClient(StompSubscription subscription, FilteringRequest filters) {
        return streamingClient(registry, subscription, filters);
    }

    private static StompClient<Object> streamingClient(StompRegistry registry, StompSubscription subscription, FilteringRequest filters) {
        registry.openSession(subscription.sessionId());
        final StompClient<Object> client = registry.subscribeClient(subscription, USERS, DESTINATION, options(filters));
        Assertions.assertTrue(client.activate());
        return client;
    }

    @SuppressWarnings("unchecked")
    private static FilteringOptions<Object> options(FilteringRequest filters) {
        return PushMapper.mapToFilteringOptions(filters, (Class<Object>) USERS.payloadType(), MAPPER);
    }

    private static FilteringRequest fields(String... fieldsPath) {
        return new FilteringRequest(null, null, List.of(fieldsPath));
    }

    private static FilteringRequest eventTypes(EventType... eventTypes) {
        return new FilteringRequest(List.of(eventTypes), null, null);
    }
}
