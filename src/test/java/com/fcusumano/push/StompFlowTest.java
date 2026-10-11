package com.fcusumano.push;

import com.fcusumano.push.StompTestClient.Frame;
import com.fcusumano.push.StompTestClient.PushSubscription;
import com.fcusumano.push.StompTestClient.Recorder;
import com.fcusumano.push.TestPayloads.Address;
import com.fcusumano.push.TestPayloads.TestChannels;
import com.fcusumano.push.TestPayloads.UserDto;
import com.fcusumano.push.annotation.EnablePushMessaging;
import com.fcusumano.push.controller.StompController;
import com.fcusumano.push.event.EventType;
import com.fcusumano.push.event.SseEventManager;
import com.fcusumano.push.event.SsePushEvent;
import com.fcusumano.push.event.StompEventManager;
import com.fcusumano.push.event.StompPushEvent;
import com.fcusumano.push.registry.StompRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.context.annotation.Bean;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHandler;
import org.springframework.messaging.SubscribableChannel;
import org.springframework.messaging.simp.SimpMessageHeaderAccessor;
import org.springframework.messaging.simp.SimpMessageType;
import org.springframework.messaging.simp.stomp.StompHeaders;
import org.springframework.messaging.simp.stomp.StompSession;
import org.springframework.stereotype.Controller;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static com.fcusumano.push.StompTestClient.TIMEOUT_MS;
import static com.fcusumano.push.StompTestClient.await;
import static com.fcusumano.push.StompTestClient.send;
import static com.fcusumano.push.StompTestClient.subscribe;
import static com.fcusumano.push.StompTestClient.subscribeAccepted;
import static com.fcusumano.push.TestPayloads.CREATED_AT;
import static com.fcusumano.push.TestPayloads.TestChannels.USERS;
import static com.fcusumano.push.TestPayloads.user1;
import static java.nio.charset.StandardCharsets.UTF_8;

//Flusso STOMP completo su server reale con WebSocketStompClient: sottoscrizione, filtri, ricevute, errori e chiusura
@ExtendWith(OutputCaptureExtension.class)
@SpringBootTest(classes = StompFlowTest.TestApplication.class, webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {"push-manager.heartbeat-enabled=false", "push-manager.client-retry-ms=2000"})
class StompFlowTest {

    private static final JsonMapper MAPPER = JsonMapper.builder().build();
    private static final HttpClient HTTP = HttpClient.newHttpClient();
    private static final String USERS_DESTINATION = "/push/USERS";
    private static final String NAME_ONLY = "{\"requiredFieldsPath\":[\"name\"]}";
    private static final String INVALID_FIELD = "{\"requiredFieldsPath\":[\"address.street\"]}";

    @LocalServerPort
    private int port;
    @Autowired
    private StompEventManager stompEventManager;
    @Autowired
    private SseEventManager sseEventManager;
    @Autowired
    private TransactionTemplate transactionTemplate;
    @Autowired
    private StompRegistry pushStompRegistry;
    @Autowired
    @Qualifier("clientInboundChannel")
    private SubscribableChannel clientInboundChannel;

    private final StompTestClient client = new StompTestClient();

    @AfterEach
    void disconnect() throws InterruptedException {
        client.close();
        Assertions.assertTrue(await(() -> pushStompRegistry.countClients(USERS) == 0), "Clients left after disconnect");
    }

    @Test
    void streamWithSelectedFields() throws Exception {
        final PushSubscription users = subscribe(connect(), USERS_DESTINATION, "sub-0",
                "{\"requiredEventTypes\":[\"CREATED\"],\"requiredFieldsPath\":[\"name\",\"address.city\"]}");
        final Frame connected = users.next();
        Assertions.assertEquals("CONNECTED", connected.eventType());
        Assertions.assertEquals("2000", connected.retryMs());
        Assertions.assertEquals("sub-0", connected.subscription());
        Assertions.assertEquals(USERS_DESTINATION, connected.destination());
        Assertions.assertEquals(json("{\"subscriptionId\":\"sub-0\",\"channel\":\"USERS\",\"streamedObject\":\"UserDto\"}"), connected.json());
        Assertions.assertTrue(users.receipt().await(TIMEOUT_MS, TimeUnit.MILLISECONDS));

        //Campi selezionati tutti null: evento scartato
        publish(new UserDto(1L, 0, null, null, CREATED_AT, null, new Address(null, "ZIP-1"), null, null), EventType.CREATED);
        //Tipo di evento non richiesto: evento scartato
        publish(user1(), EventType.UPDATED);
        publish(user1(), EventType.CREATED);

        final Frame created = users.next();
        Assertions.assertEquals("CREATED", created.eventType());
        Assertions.assertEquals(json("{\"name\":\"FOO-1\",\"address\":{\"city\":\"CITY-1\"}}"), created.json());
        Assertions.assertNull(created.retryMs());
        //Nessun altro evento: i due scartati non arrivano
        Assertions.assertNull(users.next(1500));
    }

    @Test
    void subscriptionsOfSameSessionGetTheirOwnProjection() throws Exception {
        final StompSession session = connect();
        final PushSubscription names = subscribeAccepted(session, USERS_DESTINATION, "sub-0", NAME_ONLY);
        final PushSubscription full = subscribeAccepted(session, USERS_DESTINATION, "sub-1", null);
        publish(user1(), EventType.CREATED);

        Assertions.assertEquals(json("{\"name\":\"FOO-1\"}"), names.next().json());
        final JsonNode payload = full.next().json();
        Assertions.assertEquals(7, payload.get("id").asInt());
        Assertions.assertEquals("ZIP-1", payload.get("address").get("zip").asString());
        Assertions.assertEquals(2, payload.get("orders").size());
    }

    @Test
    void invalidFiltersAreRejectedOnTheSubscription() throws Exception {
        final StompSession session = connect();
        final PushSubscription rejected = subscribe(session, USERS_DESTINATION, "sub-0", INVALID_FIELD);
        final Frame error = rejected.next();
        Assertions.assertEquals("ERROR", error.eventType());
        Assertions.assertEquals("sub-0", error.json().get("subscriptionId").asString());
        Assertions.assertTrue(error.json().get("message").asString().contains("'address.street'"), error.body());
        Assertions.assertFalse(rejected.receipt().await(500, TimeUnit.MILLISECONDS)); //Nessun RECEIPT per le sottoscrizioni rifiutate
        //Nessun frame STOMP ERROR: la connessione resta aperta e accetta nuove sottoscrizioni
        Assertions.assertTrue(session.isConnected());
        rejected.subscription().unsubscribe();
        subscribeAccepted(session, USERS_DESTINATION, "sub-1", NAME_ONLY);
        Assertions.assertTrue(session.isConnected());
    }

    @Test
    void unknownChannelIsRejected() throws Exception {
        final StompSession session = connect();
        final Frame error = subscribe(session, "/push/NOPE", "sub-0", null).next();
        Assertions.assertEquals("ERROR", error.eventType());
        Assertions.assertEquals("/push/NOPE", error.destination());
        Assertions.assertEquals("No push channel for destination '/push/NOPE'", error.json().get("message").asString());
        Assertions.assertTrue(session.isConnected());
    }

    @Test
    void filtersUpdateIsConfirmedByReceipt() throws Exception {
        final StompSession session = connect();
        final PushSubscription users = subscribeAccepted(session, USERS_DESTINATION, "sub-0", null);
        Assertions.assertTrue(send(session, "/push/filter/sub-0", NAME_ONLY, "r-1").await(TIMEOUT_MS, TimeUnit.MILLISECONDS));
        //I nuovi filtri valgono dall'evento successivo alla ricevuta
        publish(user1(), EventType.CREATED);
        Assertions.assertEquals(json("{\"name\":\"FOO-1\"}"), users.next().json());
    }

    @Test
    void invalidFiltersUpdateKeepsPreviousFilters() throws Exception {
        final StompSession session = connect();
        final PushSubscription users = subscribeAccepted(session, USERS_DESTINATION, "sub-0", NAME_ONLY);
        final CountDownLatch updated = send(session, "/push/filter/sub-0", INVALID_FIELD, "r-1");
        final Frame error = users.next();
        Assertions.assertEquals("ERROR", error.eventType());
        Assertions.assertEquals(USERS_DESTINATION, error.destination());
        Assertions.assertFalse(updated.await(1, TimeUnit.SECONDS));
        publish(user1(), EventType.CREATED);
        Assertions.assertEquals(json("{\"name\":\"FOO-1\"}"), users.next().json());
    }

    @Test
    void unsubscribeRemovesClient() throws Exception {
        final int baseline = pushStompRegistry.countClients(USERS);
        final PushSubscription users = subscribeAccepted(connect(), USERS_DESTINATION, "sub-0", null);
        Assertions.assertEquals(baseline + 1, pushStompRegistry.countClients(USERS));
        users.subscription().unsubscribe();
        Assertions.assertTrue(await(() -> pushStompRegistry.countClients(USERS) == baseline));
        publish(user1(), EventType.CREATED);
        Assertions.assertNull(users.next(1000));
    }

    @Test
    void disconnectRemovesAllClientsOfSession() throws Exception {
        final int baseline = pushStompRegistry.countClients(USERS);
        final StompSession session = connect();
        subscribeAccepted(session, USERS_DESTINATION, "sub-0", null);
        subscribeAccepted(session, USERS_DESTINATION, "sub-1", NAME_ONLY);
        Assertions.assertEquals(baseline + 2, pushStompRegistry.countClients(USERS));
        session.disconnect();
        Assertions.assertTrue(await(() -> pushStompRegistry.countClients(USERS) == baseline));
    }

    @Test
    void pushFramesNeverReachTheBroker() throws Exception {
        //Il recorder è un subscriber del clientInboundChannel come il broker semplice o un relay
        final List<Message<?>> reachedBroker = new CopyOnWriteArrayList<>();
        final MessageHandler recorder = reachedBroker::add;
        clientInboundChannel.subscribe(recorder);
        try {
            final StompSession sessionA = connect();
            final PushSubscription listener = subscribeAccepted(connect(), USERS_DESTINATION, "sub-b", null);
            final PushSubscription accepted = subscribeAccepted(sessionA, USERS_DESTINATION, "sub-a", null);
            final PushSubscription rejected = subscribe(sessionA, USERS_DESTINATION, "sub-rejected", INVALID_FIELD);
            Assertions.assertEquals("ERROR", rejected.next().eventType());
            Assertions.assertTrue(send(sessionA, "/push/filter/sub-a", NAME_ONLY, "r-1").await(TIMEOUT_MS, TimeUnit.MILLISECONDS));
            //Iniezione: un client prova a pubblicare sul canale push
            sessionA.send(USERS_DESTINATION, "{\"id\":1,\"name\":\"FAKE-1\"}".getBytes(UTF_8));
            accepted.subscription().unsubscribe();
            rejected.subscription().unsubscribe();
            awaitMarker(sessionA, reachedBroker);
            Assertions.assertTrue(await(() -> pushStompRegistry.countClients(USERS) == 1)); //Resta solo sub-b
            Assertions.assertNull(listener.next(1000)); //B non riceve nulla dalla SEND di A
        } finally {
            clientInboundChannel.unsubscribe(recorder);
        }
        final List<String> libraryIds = List.of("sub-a", "sub-b", "sub-rejected");
        for (Message<?> message : reachedBroker) {
            final String destination = destination(message);
            Assertions.assertFalse(destination != null && destination.startsWith("/push"), "Push frame reached the broker: " + message);
            Assertions.assertFalse(SimpMessageHeaderAccessor.getMessageType(message.getHeaders()) == SimpMessageType.UNSUBSCRIBE
                                   && libraryIds.contains(SimpMessageHeaderAccessor.getSubscriptionId(message.getHeaders())),
                    "Push UNSUBSCRIBE reached the broker: " + message);
        }
        //Controllo non vuoto: i frame non della libreria arrivano al recorder
        Assertions.assertTrue(reachedBroker.stream().anyMatch(m -> SimpMessageHeaderAccessor.getMessageType(m.getHeaders()) == SimpMessageType.CONNECT));
    }

    @Test
    void foreignDestinationsAreClosedWithoutBrokerConfiguration() throws Exception {
        //Senza configurazione del broker quello di default di Spring accetterebbe ogni destinazione: relay anonimo tra i client
        final List<Message<?>> reachedBroker = new CopyOnWriteArrayList<>();
        final MessageHandler recorder = reachedBroker::add;
        clientInboundChannel.subscribe(recorder);
        try {
            final StompSession sessionA = connect();
            final StompSession sessionB = connect();
            final Recorder chat = new Recorder();
            sessionA.subscribe("/chat", chat);
            sessionA.subscribe("/**", chat);
            awaitMarker(sessionA, reachedBroker);
            sessionB.send("/chat", "HELLO-1".getBytes(UTF_8));
            awaitMarker(sessionB, reachedBroker);
            Assertions.assertNull(chat.next(1000)); //A non riceve la SEND di B
        } finally {
            clientInboundChannel.unsubscribe(recorder);
        }
        //Né le SUBSCRIBE né la SEND arrivano al broker
        for (Message<?> message : reachedBroker) {
            final SimpMessageType type = SimpMessageHeaderAccessor.getMessageType(message.getHeaders());
            Assertions.assertFalse(type == SimpMessageType.SUBSCRIBE || type == SimpMessageType.MESSAGE, "Frame reached the broker: " + message);
        }
    }

    @Test
    void protocolsDoNotCrossTalk(CapturedOutput output) throws Exception {
        final String sseSessionId = sseSubscribe();
        final BlockingQueue<String> sseLines = openSseStream(sseSessionId);
        Assertions.assertEquals("event:CONNECTED", nextSseEvent(sseLines, TIMEOUT_MS));
        final PushSubscription stomp = subscribeAccepted(connect(), USERS_DESTINATION, "sub-0", null);

        //Tipi di evento diversi per riconoscere il protocollo di provenienza
        transactionTemplate.executeWithoutResult(_ -> {
            sseEventManager.publishEvent(SsePushEvent.of(user1(), EventType.CREATED, USERS));
            stompEventManager.publishEvent(StompPushEvent.of(user1(), EventType.UPDATED, USERS));
        });

        Assertions.assertEquals("UPDATED", stomp.next().eventType());
        Assertions.assertEquals("event:CREATED", nextSseEvent(sseLines, TIMEOUT_MS));
        Assertions.assertNull(stomp.next(1500));
        Assertions.assertNull(nextSseEvent(sseLines, 500));
        //Senza la guardia di EventManager ogni manager riceverebbe anche gli eventi dell'altro protocollo
        Assertions.assertFalse(output.getAll().contains("ClassCastException"));
        Assertions.assertEquals(200, sseRequest("DELETE", "/api/sse/unsubscribe/" + sseSessionId).statusCode());
    }

    private StompSession connect() throws Exception {
        return client.connect("ws://localhost:" + port + "/push-ws");
    }

    //UNSUBSCRIBE dell'applicazione come marcatore: quando arriva al recorder, i frame precedenti della sessione sono già passati dagli interceptor
    //(senza configurazione del broker le SUBSCRIBE fuori da '/push' vengono trattenute)
    private static void awaitMarker(StompSession session, List<Message<?>> reachedBroker) throws InterruptedException {
        final StompHeaders headers = new StompHeaders();
        headers.setDestination("/topic/marker");
        headers.setId("marker-" + UUID.randomUUID());
        session.subscribe(headers, new Recorder()).unsubscribe();
        Assertions.assertTrue(await(() -> reachedBroker.stream().anyMatch(m -> SimpMessageHeaderAccessor.getMessageType(m.getHeaders()) == SimpMessageType.UNSUBSCRIBE
                                                                              && headers.getId().equals(SimpMessageHeaderAccessor.getSubscriptionId(m.getHeaders())))),
                "Marker not received");
    }

    private void publish(UserDto user, EventType eventType) {
        transactionTemplate.executeWithoutResult(_ -> stompEventManager.publishEvent(StompPushEvent.of(user, eventType, USERS)));
    }

    private String sseSubscribe() throws Exception {
        final HttpResponse<String> response = sseRequest("POST", "/api/sse/subscribe/USERS");
        Assertions.assertEquals(200, response.statusCode(), response.body());
        return MAPPER.readTree(response.body()).get("sessionId").asString();
    }

    private HttpResponse<String> sseRequest(String method, String path) throws Exception {
        final HttpRequest request = HttpRequest.newBuilder(URI.create("http://localhost:" + port + path))
                .header("Content-Type", "application/json")
                .method(method, HttpRequest.BodyPublishers.noBody())
                .build();
        return HTTP.send(request, HttpResponse.BodyHandlers.ofString());
    }

    //Le righe dello stream SSE finiscono in coda, lette da un thread separato
    private BlockingQueue<String> openSseStream(String sessionId) {
        final BlockingQueue<String> lines = new LinkedBlockingQueue<>();
        final HttpRequest request = HttpRequest.newBuilder(URI.create("http://localhost:" + port + "/api/sse/stream/" + sessionId)).GET().build();
        HTTP.sendAsync(request, HttpResponse.BodyHandlers.ofLines()).thenAccept(response -> response.body().forEach(lines::add));
        return lines;
    }

    private static String nextSseEvent(BlockingQueue<String> lines, long timeoutMs) throws InterruptedException {
        final long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            final String line = lines.poll(deadline - System.currentTimeMillis(), TimeUnit.MILLISECONDS);
            if (line != null && line.startsWith("event:")) return line;
        }
        return null;
    }

    private static String destination(Message<?> message) {
        return SimpMessageHeaderAccessor.getDestination(message.getHeaders());
    }

    private static JsonNode json(String content) {
        return MAPPER.readTree(content);
    }

    @SpringBootConfiguration
    @EnableAutoConfiguration
    @EnablePushMessaging(stomp = true)
    static class TestApplication {

        @Bean
        PlatformTransactionManager transactionManager() {
            return new SseFlowTest.NoOpTransactionManager();
        }

        @Bean
        TransactionTemplate transactionTemplate(PlatformTransactionManager transactionManager) {
            return new TransactionTemplate(transactionManager);
        }

        @Bean
        TestStompEndpoint testStompEndpoint() {
            return new TestStompEndpoint();
        }

        @Bean //SSE sulla stessa applicazione
        SseFlowTest.TestPushEndpoint testPushEndpoint() {
            return new SseFlowTest.TestPushEndpoint();
        }
    }

    //Vuoto, come lo scriverebbe un'applicazione
    @Controller
    static class TestStompEndpoint extends StompController<TestChannels> {}
}
