package com.fcusumano.push;

import com.fcusumano.push.StompTestClient.Frame;
import com.fcusumano.push.StompTestClient.PushSubscription;
import com.fcusumano.push.StompTestClient.Recorder;
import com.fcusumano.push.annotation.EnablePushMessaging;
import com.fcusumano.push.event.EventType;
import com.fcusumano.push.event.StompEventManager;
import com.fcusumano.push.event.StompPushEvent;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.context.annotation.Bean;
import org.springframework.messaging.handler.annotation.MessageMapping;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.messaging.simp.annotation.support.SimpAnnotationMethodMessageHandler;
import org.springframework.messaging.simp.broker.SimpleBrokerMessageHandler;
import org.springframework.messaging.simp.config.MessageBrokerRegistry;
import org.springframework.messaging.simp.stomp.StompHeaders;
import org.springframework.messaging.simp.stomp.StompSession;
import org.springframework.stereotype.Controller;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.util.MimeTypeUtils;
import org.springframework.web.socket.config.annotation.EnableWebSocketMessageBroker;
import org.springframework.web.socket.config.annotation.StompEndpointRegistry;
import org.springframework.web.socket.config.annotation.WebSocketMessageBrokerConfigurer;

import java.util.List;
import java.util.concurrent.ExecutionException;

import static com.fcusumano.push.StompTestClient.subscribeAccepted;
import static com.fcusumano.push.TestPayloads.TestChannels.USERS;
import static com.fcusumano.push.TestPayloads.user1;
import static java.nio.charset.StandardCharsets.UTF_8;

//Applicazione con configurazione WebSocket propria: la libreria usa i suoi endpoint senza toccare broker e prefissi
@SpringBootTest(classes = StompCoexistenceTest.TestApplication.class, webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {"push-manager.stomp.endpoint=", "push-manager.heartbeat-enabled=false"})
class StompCoexistenceTest {

    @LocalServerPort
    private int port;
    @Autowired
    private StompEventManager stompEventManager;
    @Autowired
    private TransactionTemplate transactionTemplate;
    @Autowired
    private SimpleBrokerMessageHandler simpleBrokerMessageHandler;
    @Autowired
    private SimpAnnotationMethodMessageHandler simpAnnotationMethodMessageHandler;

    private final StompTestClient client = new StompTestClient();

    @AfterEach
    void disconnect() {
        client.close();
    }

    @Test
    void pushWorksOnApplicationEndpoint() throws Exception {
        final PushSubscription users = subscribeAccepted(client.connect(url("/ws")), "/push/USERS", "sub-0", null);
        transactionTemplate.executeWithoutResult(_ -> stompEventManager.publishEvent(StompPushEvent.of(user1(), EventType.CREATED, USERS)));
        final Frame created = users.next();
        Assertions.assertEquals("CREATED", created.eventType());
        Assertions.assertEquals("FOO-1", created.json().get("name").asString());
    }

    @Test
    void applicationBrokerStillWorks() throws Exception {
        final StompSession session = client.connect(url("/ws"));
        final Recorder echoes = new Recorder();
        session.subscribe("/topic/echo", echoes);
        //Il broker semplice non conferma le SUBSCRIBE: si ripete l'invio finché la sottoscrizione è registrata
        Frame echo = null;
        for (int attempt = 0; attempt < 10 && echo == null; attempt++) {
            final StompHeaders headers = new StompHeaders();
            headers.setDestination("/app/echo");
            headers.setContentType(MimeTypeUtils.TEXT_PLAIN);
            session.send(headers, "ECHO-1".getBytes(UTF_8));
            echo = echoes.next(500);
        }
        Assertions.assertNotNull(echo, "No echo received on '/topic/echo'");
        Assertions.assertEquals("ECHO-1", echo.body());
        Assertions.assertEquals("/topic/echo", echo.destination());
    }

    @Test
    void libraryDoesNotChangeBrokerPrefixes() {
        Assertions.assertEquals(List.of("/topic"), List.copyOf(simpleBrokerMessageHandler.getDestinationPrefixes()));
        Assertions.assertEquals(List.of("/app/"), List.copyOf(simpAnnotationMethodMessageHandler.getDestinationPrefixes()));
    }

    @Test
    void libraryEndpointIsNotRegistered() throws Exception {
        try {
            client.connect(url("/push-ws"));
            Assertions.fail("Endpoint '/push-ws' should not be registered with an empty push-manager.stomp.endpoint");
        } catch (ExecutionException _) {
            //Handshake rifiutato: nessun handler per il percorso
        }
    }

    private String url(String endpoint) {
        return "ws://localhost:" + port + endpoint;
    }

    @SpringBootConfiguration
    @EnableAutoConfiguration
    @EnablePushMessaging(stomp = true)
    @EnableWebSocketMessageBroker
    static class TestApplication implements WebSocketMessageBrokerConfigurer {

        @Override
        public void registerStompEndpoints(StompEndpointRegistry registry) {
            registry.addEndpoint("/ws");
        }

        @Override
        public void configureMessageBroker(MessageBrokerRegistry registry) {
            registry.setApplicationDestinationPrefixes("/app");
            registry.enableSimpleBroker("/topic");
        }

        @Bean
        PlatformTransactionManager transactionManager() {
            return new SseFlowTest.NoOpTransactionManager();
        }

        @Bean
        TransactionTemplate transactionTemplate(PlatformTransactionManager transactionManager) {
            return new TransactionTemplate(transactionManager);
        }

        @Bean
        StompFlowTest.TestStompEndpoint testStompEndpoint() {
            return new StompFlowTest.TestStompEndpoint();
        }

        @Bean
        EchoController echoController() {
            return new EchoController();
        }
    }

    @Controller
    static class EchoController {

        @MessageMapping("/echo")
        @SendTo("/topic/echo")
        String echo(@Payload String text) {
            return text;
        }
    }
}
