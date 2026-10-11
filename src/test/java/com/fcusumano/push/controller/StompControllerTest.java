package com.fcusumano.push.controller;

import com.fcusumano.push.CapturingChannel;
import com.fcusumano.push.TestPayloads.TestChannels;
import com.fcusumano.push.channel.ChannelEnum;
import com.fcusumano.push.client.StompSubscription;
import com.fcusumano.push.properties.PushProperties;
import com.fcusumano.push.registry.StompRegistry;
import com.fcusumano.push.service.StompService;
import com.fcusumano.push.util.StompDestinations;
import jakarta.validation.Validation;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.context.ApplicationEvent;
import org.springframework.context.annotation.AnnotationConfigUtils;
import org.springframework.context.event.DefaultEventListenerFactory;
import org.springframework.context.event.EventListener;
import org.springframework.context.event.EventListenerMethodProcessor;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.simp.SimpMessageHeaderAccessor;
import org.springframework.messaging.simp.stomp.StompCommand;
import org.springframework.messaging.simp.stomp.StompHeaderAccessor;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.messaging.SessionConnectEvent;
import org.springframework.web.socket.messaging.SessionDisconnectEvent;
import tools.jackson.databind.json.JsonMapper;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.fcusumano.push.CapturingChannel.body;
import static com.fcusumano.push.CapturingChannel.eventType;
import static com.fcusumano.push.TestPayloads.TestChannels.USERS;
import static com.fcusumano.push.util.StompMapper.FILTERS_HEADER;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.springframework.messaging.simp.stomp.StompCommand.CONNECT;
import static org.springframework.messaging.simp.stomp.StompCommand.SEND;
import static org.springframework.messaging.simp.stomp.StompCommand.SUBSCRIBE;
import static org.springframework.messaging.simp.stomp.StompCommand.UNSUBSCRIBE;
import static org.springframework.messaging.simp.stomp.StompHeaderAccessor.STOMP_ACK_HEADER;
import static org.springframework.messaging.simp.stomp.StompHeaderAccessor.STOMP_RECEIPT_HEADER;

//Interceptor del clientInboundChannel: frame costruiti come li decodifica StompSubProtocolHandler
class StompControllerTest {

    private static final JsonMapper MAPPER = JsonMapper.builder().build();
    private static final String SESSION_ID = "SESSION-1";
    private static final String INVALID_FIELD = "{\"requiredFieldsPath\":[\"address.street\"]}";
    private static final MessageChannel INBOUND = (_, _) -> true; //clientInboundChannel: non usato dal controller

    private final CapturingChannel outbound = new CapturingChannel();
    private final StompRegistry registry = new StompRegistry(MAPPER, () -> outbound);
    private final TestStompController controller = new TestStompController();

    @BeforeEach
    void connect() {
        controller.setService(new TestStompService(registry));
        controller.connect(new SessionConnectEvent(this, frame(CONNECT, SESSION_ID, null, null, Map.of(), "")));
    }

    @Test
    void pushSubscribeIsConsumed() {
        final Message<byte[]> subscribe = subscribe("sub-0", "/push/USERS",
                Map.of(FILTERS_HEADER, "{\"requiredFieldsPath\":[\"name\"]}", STOMP_RECEIPT_HEADER, "r-sub-0"));
        Assertions.assertNull(controller.preSend(subscribe, INBOUND));
        Assertions.assertEquals("CONNECTED", eventType(outbound.singleMessage()));
        Assertions.assertEquals(List.of("r-sub-0"), outbound.receiptIds());
        Assertions.assertEquals(1, registry.countClients(USERS));
        //Filtri letti dall'header
        Assertions.assertNotNull(registry.selectClient(StompSubscription.of(SESSION_ID, "sub-0")).filteringOptions().fieldSelector());
    }

    @Test
    void applicationFramesPassThrough() {
        for (Message<byte[]> frame : List.of(
                subscribe("app-sub", "/topic/x", Map.of()),
                subscribe("app-sub-2", "/pushX/USERS", Map.of()), //Stesso inizio del prefisso, altro segmento
                send("/app/x", "TEXT-1", Map.of()),
                unsubscribe("app-sub", Map.of(STOMP_RECEIPT_HEADER, "r-1")))) {
            Assertions.assertSame(frame, controller.preSend(frame, INBOUND));
        }
        Assertions.assertTrue(outbound.isEmpty());
    }

    @Test
    void rejectedSubscribeAndItsUnsubscribeAreConsumed() {
        Assertions.assertNull(controller.preSend(subscribe("sub-0", "/push/USERS",
                Map.of(FILTERS_HEADER, INVALID_FIELD, STOMP_RECEIPT_HEADER, "r-sub-0")), INBOUND));
        Assertions.assertEquals("ERROR", eventType(outbound.singleMessage()));
        Assertions.assertTrue(outbound.receiptIds().isEmpty());
        //La UNSUBSCRIBE dell'id rifiutato non deve arrivare al broker: un relay risponderebbe con ERROR chiudendo la connessione
        Assertions.assertNull(controller.preSend(unsubscribe("sub-0", Map.of(STOMP_RECEIPT_HEADER, "r-unsubscribe")), INBOUND));
        Assertions.assertEquals(List.of("r-unsubscribe"), outbound.receiptIds());
        //Dopo la UNSUBSCRIBE l'id non è più della libreria
        final Message<byte[]> again = unsubscribe("sub-0", Map.of());
        Assertions.assertSame(again, controller.preSend(again, INBOUND));
    }

    @Test
    void sendToPushChannelIsDiscarded() {
        controller.preSend(subscribe("sub-0", "/push/USERS", Map.of()), INBOUND);
        outbound.clear();
        //Solo il server pubblica sui canali push
        for (String destination : List.of("/push/USERS", "/push", "/push/filter/", "/push/A/B")) {
            Assertions.assertNull(controller.preSend(send(destination, "{\"id\":1,\"name\":\"FAKE-1\"}", Map.of(STOMP_RECEIPT_HEADER, "r-1")), INBOUND), destination);
        }
        Assertions.assertTrue(outbound.isEmpty());
    }

    @Test
    void filterSendIsConsumed() {
        controller.preSend(subscribe("sub-0", "/push/USERS", Map.of()), INBOUND);
        outbound.clear();
        Assertions.assertNull(controller.preSend(send("/push/filter/sub-0", "{\"requiredFieldsPath\":[\"name\"]}", Map.of(STOMP_RECEIPT_HEADER, "r-1")), INBOUND));
        Assertions.assertEquals(List.of("r-1"), outbound.receiptIds());
        Assertions.assertTrue(outbound.messages().isEmpty());
        Assertions.assertNotNull(registry.selectClient(StompSubscription.of(SESSION_ID, "sub-0")).filteringOptions().fieldSelector());
        //Sottoscrizione sconosciuta: ERROR sulla destinazione dei filtri
        Assertions.assertNull(controller.preSend(send("/push/filter/sub-9", "{}", Map.of()), INBOUND));
        Assertions.assertEquals("/push/filter/sub-9", SimpMessageHeaderAccessor.getDestination(outbound.singleMessage().getHeaders()));
    }

    @Test
    void nonAutoAckIsRejected() {
        for (String ack : List.of("client", "client-individual")) {
            outbound.clear();
            Assertions.assertNull(controller.preSend(subscribe("sub-" + ack, "/push/USERS", Map.of(STOMP_ACK_HEADER, ack)), INBOUND));
            final Message<?> error = outbound.singleMessage();
            Assertions.assertEquals("ERROR", eventType(error));
            Assertions.assertEquals("Only 'ack:auto' is supported on push subscriptions, found 'ack:" + ack + "'", body(error).get("message").asString());
        }
        Assertions.assertEquals(0, registry.countClients(USERS));
        outbound.clear();
        Assertions.assertNull(controller.preSend(subscribe("sub-auto", "/push/USERS", Map.of(STOMP_ACK_HEADER, "auto")), INBOUND));
        Assertions.assertEquals("CONNECTED", eventType(outbound.singleMessage()));
    }

    @Test
    void unknownTopicIsRejected() {
        int index = 0;
        for (String destination : List.of("/push/NOPE", "/push/users", "/push/A/B", "/push/filter/x", "/push")) {
            outbound.clear();
            final String subscriptionId = "sub-" + index++;
            Assertions.assertNull(controller.preSend(subscribe(subscriptionId, destination, Map.of()), INBOUND), destination);
            final Message<?> error = outbound.singleMessage();
            Assertions.assertEquals("ERROR", eventType(error));
            Assertions.assertEquals(subscriptionId, SimpMessageHeaderAccessor.getSubscriptionId(error.getHeaders()));
            Assertions.assertEquals(destination, SimpMessageHeaderAccessor.getDestination(error.getHeaders()));
            Assertions.assertEquals("No push channel for destination '" + destination + "'", body(error).get("message").asString());
        }
        Assertions.assertEquals(0, registry.countClients(USERS));
    }

    @Test
    void subscribeWithoutIdIsConsumed() {
        //Senza id un MESSAGE non è indirizzabile: nessuna risposta, ma il frame non arriva al broker
        Assertions.assertNull(controller.preSend(subscribe(null, "/push/USERS", Map.of()), INBOUND));
        Assertions.assertTrue(outbound.isEmpty());
        Assertions.assertEquals(0, registry.countClients(USERS));
    }

    @Test
    void connectAndDisconnectFramesPassThrough() {
        final Message<byte[]> heartbeat = MessageBuilder.createMessage(new byte[0], heartbeatHeaders());
        for (Message<byte[]> frame : List.of(
                frame(CONNECT, SESSION_ID, null, null, Map.of(), ""),
                frame(StompCommand.DISCONNECT, SESSION_ID, null, null, Map.of(), ""),
                frame(StompCommand.ACK, SESSION_ID, null, null, Map.of(StompHeaderAccessor.STOMP_ID_HEADER, "ACK-1"), ""),
                heartbeat,
                frame(SEND, null, null, "/push/USERS", Map.of(), "{}"))) { //Messaggi interni senza sessione
            Assertions.assertSame(frame, controller.preSend(frame, INBOUND));
        }
        Assertions.assertTrue(outbound.isEmpty());
    }

    @Test
    void serviceFailureNeverEscapes() {
        controller.setService(new FailingStompService(registry));
        //Frame della libreria comunque trattenuti
        Assertions.assertNull(controller.preSend(subscribe("sub-0", "/push/USERS", Map.of()), INBOUND));
        Assertions.assertNull(controller.preSend(subscribe("sub-1", "/push/NOPE", Map.of()), INBOUND));
        Assertions.assertNull(controller.preSend(send("/push/filter/sub-0", "{}", Map.of()), INBOUND));
        //UNSUBSCRIBE non attribuibile alla libreria e frame dell'applicazione proseguono
        final Message<byte[]> unsubscribe = unsubscribe("sub-0", Map.of());
        Assertions.assertSame(unsubscribe, controller.preSend(unsubscribe, INBOUND));
        final Message<byte[]> appSubscribe = subscribe("app-sub", "/topic/x", Map.of());
        Assertions.assertSame(appSubscribe, controller.preSend(appSubscribe, INBOUND));
    }

    @Test
    @SuppressWarnings("rawtypes")
    void rawControllerFails() {
        try {
            new RawStompController();
            Assertions.fail("Raw controller should be rejected");
        } catch (IllegalStateException e) {
            Assertions.assertTrue(e.getMessage().contains("must extend StompController with the generated channel enum"), e.getMessage());
        }
        try {
            new GenericStompController<TestChannels>();
            Assertions.fail("Controller without concrete channel enum should be rejected");
        } catch (IllegalStateException _) {
        }
    }

    @Test
    void sessionEventsOpenAndCloseSession() {
        controller.connect(new SessionConnectEvent(this, frame(CONNECT, "SESSION-2", null, null, Map.of(), "")));
        final Message<byte[]> subscribe = frame(SUBSCRIBE, "SESSION-2", "sub-0", "/push/USERS", Map.of(), "");
        Assertions.assertNull(controller.preSend(subscribe, INBOUND));
        Assertions.assertEquals("CONNECTED", eventType(outbound.singleMessage()));
        Assertions.assertEquals(1, registry.countClients(USERS));

        controller.disconnect(new SessionDisconnectEvent(this, frame(StompCommand.DISCONNECT, "SESSION-2", null, null, Map.of(), ""),
                "SESSION-2", CloseStatus.NORMAL));
        Assertions.assertEquals(0, registry.countClients(USERS));
        //Dopo la chiusura la SUBSCRIBE resta della libreria ma viene ignorata
        outbound.clear();
        Assertions.assertNull(controller.preSend(subscribe, INBOUND));
        Assertions.assertTrue(outbound.isEmpty());
        Assertions.assertEquals(0, registry.countClients(USERS));
    }

    @Test
    void disconnectFrameClosesTheSession() {
        controller.preSend(subscribe("sub-0", "/push/USERS", Map.of()), INBOUND);
        Assertions.assertEquals(1, registry.countClients(USERS));
        //DISCONNECT del client o sintetico di fine sessione: chiude la sessione e prosegue verso il broker
        final Message<byte[]> disconnect = frame(StompCommand.DISCONNECT, SESSION_ID, null, null, Map.of(), "");
        Assertions.assertSame(disconnect, controller.preSend(disconnect, INBOUND));
        Assertions.assertEquals(0, registry.countClients(USERS));
        //L'evento di chiusura che segue non trova più nulla da chiudere
        controller.disconnect(new SessionDisconnectEvent(this, disconnect, SESSION_ID, CloseStatus.NORMAL));
        Assertions.assertEquals(0, registry.countClients(USERS));
    }

    @Test
    void sessionEventsAreHandledBeforeApplicationListeners() {
        //Listener dell'applicazione registrato prima del controller che fallisce: Spring non chiama i listener successivi
        try (GenericApplicationContext context = new GenericApplicationContext()) {
            context.registerBean(AnnotationConfigUtils.EVENT_LISTENER_PROCESSOR_BEAN_NAME, EventListenerMethodProcessor.class);
            context.registerBean(AnnotationConfigUtils.EVENT_LISTENER_FACTORY_BEAN_NAME, DefaultEventListenerFactory.class);
            context.registerBean(PresenceListener.class);
            context.registerBean(TestStompController.class, () -> controller);
            context.refresh();

            publishLikeSpring(context, new SessionConnectEvent(this, frame(CONNECT, "SESSION-2", null, null, Map.of(), "")));
            Assertions.assertNull(controller.preSend(frame(SUBSCRIBE, "SESSION-2", "sub-0", "/push/USERS", Map.of(), ""), INBOUND));
            Assertions.assertEquals("CONNECTED", eventType(outbound.singleMessage())); //Sessione aperta nonostante il listener fallito
            publishLikeSpring(context, new SessionDisconnectEvent(this, frame(StompCommand.DISCONNECT, "SESSION-2", null, null, Map.of(), ""),
                    "SESSION-2", CloseStatus.NORMAL));
            Assertions.assertEquals(0, registry.countClients(USERS));
        }
    }

    //Come StompSubProtocolHandler: l'eccezione di un listener viene solo registrata nei log
    private static void publishLikeSpring(GenericApplicationContext context, ApplicationEvent event) {
        try {
            context.publishEvent(event);
        } catch (NullPointerException _) {
            //Listener dell'applicazione fallito
        }
    }

    private static Message<byte[]> subscribe(String subscriptionId, String destination, Map<String, String> nativeHeaders) {
        return frame(SUBSCRIBE, SESSION_ID, subscriptionId, destination, nativeHeaders, "");
    }

    private static Message<byte[]> unsubscribe(String subscriptionId, Map<String, String> nativeHeaders) {
        return frame(UNSUBSCRIBE, SESSION_ID, subscriptionId, null, nativeHeaders, "");
    }

    private static Message<byte[]> send(String destination, String body, Map<String, String> nativeHeaders) {
        return frame(SEND, SESSION_ID, null, destination, nativeHeaders, body);
    }

    private static Message<byte[]> frame(StompCommand command, String sessionId, String subscriptionId, String destination,
                                         Map<String, String> nativeHeaders, String body) {
        final StompHeaderAccessor accessor = StompHeaderAccessor.create(command);
        accessor.setSessionId(sessionId);
        accessor.setSubscriptionId(subscriptionId);
        accessor.setDestination(destination);
        nativeHeaders.forEach(accessor::setNativeHeader);
        accessor.setLeaveMutable(true);
        return MessageBuilder.createMessage(body.getBytes(UTF_8), accessor.getMessageHeaders());
    }

    private static MessageHeaders heartbeatHeaders() {
        final StompHeaderAccessor accessor = StompHeaderAccessor.createForHeartbeat();
        accessor.setSessionId(SESSION_ID);
        return accessor.getMessageHeaders();
    }

    static class TestStompController extends StompController<TestChannels> {}

    //Presenza degli utenti: fallisce per le sessioni anonime, senza Principal
    static final class PresenceListener {

        private final List<String> online = new ArrayList<>();

        @EventListener
        public void online(SessionConnectEvent event) {
            online.add(event.getUser().getName());
        }

        @EventListener
        public void offline(SessionDisconnectEvent event) {
            online.remove(event.getUser().getName());
        }
    }

    @SuppressWarnings("rawtypes")
    static class RawStompController extends StompController {}

    static class GenericStompController<C extends ChannelEnum> extends StompController<C> {}

    //Servizio reale, configurato come farebbe Spring con i setter
    static class TestStompService extends StompService {

        TestStompService(StompRegistry registry) {
            super(MAPPER, Validation.buildDefaultValidatorFactory().getValidator(), StompDestinations.of("/push"));
            setProperties(new PushProperties(0, false, 0, 0));
            setRegistry(registry);
        }
    }

    static final class FailingStompService extends TestStompService {

        FailingStompService(StompRegistry registry) {
            super(registry);
        }

        @Override
        public void subscribe(StompSubscription subscription, ChannelEnum channel, String filters, String receipt) {
            throw new IllegalStateException("Subscribe failure");
        }

        @Override
        public void reject(StompSubscription subscription, String destination, String reason) {
            throw new IllegalStateException("Reject failure");
        }

        @Override
        public void updateFilters(StompSubscription subscription, String filters, String receipt) {
            throw new IllegalStateException("Update failure");
        }

        @Override
        public boolean unsubscribe(StompSubscription subscription, String receipt) {
            throw new IllegalStateException("Unsubscribe failure");
        }
    }
}
