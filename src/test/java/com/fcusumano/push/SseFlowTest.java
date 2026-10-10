package com.fcusumano.push;

import com.fcusumano.push.TestPayloads.Address;
import com.fcusumano.push.TestPayloads.TestChannels;
import com.fcusumano.push.TestPayloads.UserDto;
import com.fcusumano.push.annotation.EnablePushMessaging;
import com.fcusumano.push.controller.SseController;
import com.fcusumano.push.event.EventType;
import com.fcusumano.push.event.SseEventManager;
import com.fcusumano.push.event.SsePushEvent;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.context.annotation.Bean;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.AbstractPlatformTransactionManager;
import org.springframework.transaction.support.DefaultTransactionStatus;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import tools.jackson.databind.json.JsonMapper;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static com.fcusumano.push.TestPayloads.CREATED_AT;
import static com.fcusumano.push.TestPayloads.mario;

//Flusso completo su server reale: sottoscrizione, stream SSE, pubblicazione in transazione, filtri ed errori
@SpringBootTest(classes = SseFlowTest.TestApplication.class, webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {"push-manager.heartbeat-enabled=false", "push-manager.client-retry-ms=2000"})
class SseFlowTest {

    private static final JsonMapper MAPPER = JsonMapper.builder().build();
    private static final HttpClient HTTP = HttpClient.newHttpClient();

    @LocalServerPort
    private int port;
    @Autowired
    private SseEventManager sseEventManager;
    @Autowired
    private TransactionTemplate transactionTemplate;

    @Test
    void streamWithSelectedFields() throws Exception {
        final String sessionId = subscribe("{\"requiredEventTypes\":[\"CREATED\"],\"requiredFieldsPath\":[\"name\",\"address.city\"]}");
        final BlockingQueue<String> lines = openStream(sessionId);
        Assertions.assertEquals("event:CONNECTED", nextEvent(lines));

        //Campi selezionati tutti null: evento scartato
        publish(new UserDto(1L, 0, null, null, CREATED_AT, null, new Address(null, "00100"), null, null), EventType.CREATED);
        //Tipo di evento non richiesto: evento scartato
        publish(mario(), EventType.UPDATED);
        publish(mario(), EventType.CREATED);

        Assertions.assertEquals("event:CREATED", nextEvent(lines));
        final String data = nextLineStartingWith(lines, "data:");
        Assertions.assertEquals(MAPPER.readTree("{\"name\":\"Mario\",\"address\":{\"city\":\"Roma\"}}"),
                MAPPER.readTree(data.substring("data:".length())));
        //Nessun altro evento: i due scartati non arrivano
        Assertions.assertNull(nextLineStartingWith(lines, "event:", 1500));

        Assertions.assertEquals(200, request("DELETE", "/api/sse/unsubscribe/" + sessionId, null).statusCode());
    }

    @Test
    void unknownSessionIsNotFound() throws Exception {
        Assertions.assertEquals(404, request("PUT", "/api/sse/filter/unknown", "{}").statusCode());
        Assertions.assertEquals(404, request("DELETE", "/api/sse/unsubscribe/unknown", null).statusCode());
    }

    @Test
    void invalidFiltersAreBadRequest() throws Exception {
        Assertions.assertEquals(400, request("POST", "/api/sse/subscribe/USERS",
                "{\"requiredFieldsPath\":[\"address.street\"]}").statusCode());
        Assertions.assertEquals(400, request("POST", "/api/sse/subscribe/USERS",
                "{\"contentFilters\":[{\"fieldPath\":\"id\",\"check\":\"GREATER_THEN\",\"value\":\"abc\"}]}").statusCode());
    }

    @Test
    void unsubscribeWithoutStreamingRemovesClient() throws Exception {
        final String sessionId = subscribe(null);
        Assertions.assertEquals(200, request("DELETE", "/api/sse/unsubscribe/" + sessionId, null).statusCode());
        Assertions.assertEquals(404, request("PUT", "/api/sse/filter/" + sessionId, "{}").statusCode());
    }

    private String subscribe(String filters) throws Exception {
        final HttpResponse<String> response = request("POST", "/api/sse/subscribe/USERS", filters);
        Assertions.assertEquals(200, response.statusCode(), response.body());
        return MAPPER.readTree(response.body()).get("sessionId").asString();
    }

    private void publish(UserDto user, EventType eventType) {
        transactionTemplate.executeWithoutResult(_ ->
                sseEventManager.publishEvent(SsePushEvent.of(user, eventType, TestChannels.USERS)));
    }

    private HttpResponse<String> request(String method, String path, String body) throws Exception {
        final HttpRequest request = HttpRequest.newBuilder(uri(path))
                .header("Content-Type", "application/json")
                .method(method, body == null ? HttpRequest.BodyPublishers.noBody() : HttpRequest.BodyPublishers.ofString(body))
                .build();
        return HTTP.send(request, HttpResponse.BodyHandlers.ofString());
    }

    //Le righe dello stream finiscono in coda, lette da un thread separato
    private BlockingQueue<String> openStream(String sessionId) {
        final BlockingQueue<String> lines = new LinkedBlockingQueue<>();
        final HttpRequest request = HttpRequest.newBuilder(uri("/api/sse/stream/" + sessionId)).GET().build();
        HTTP.sendAsync(request, HttpResponse.BodyHandlers.ofLines())
                .thenAccept(response -> response.body().forEach(lines::add));
        return lines;
    }

    private static String nextEvent(BlockingQueue<String> lines) throws InterruptedException {
        return nextLineStartingWith(lines, "event:");
    }

    private static String nextLineStartingWith(BlockingQueue<String> lines, String prefix) throws InterruptedException {
        final String line = nextLineStartingWith(lines, prefix, 5000);
        Assertions.assertNotNull(line, "No line starting with '" + prefix + "' received");
        return line;
    }

    private static String nextLineStartingWith(BlockingQueue<String> lines, String prefix, long timeoutMs)
            throws InterruptedException {
        final long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            final String line = lines.poll(deadline - System.currentTimeMillis(), TimeUnit.MILLISECONDS);
            if (line != null && line.startsWith(prefix)) return line;
        }
        return null;
    }

    private URI uri(String path) {
        return URI.create("http://localhost:" + port + path);
    }

    @SpringBootConfiguration
    @EnableAutoConfiguration
    @EnablePushMessaging
    static class TestApplication {

        @Bean
        PlatformTransactionManager transactionManager() {
            return new NoOpTransactionManager();
        }

        @Bean
        TransactionTemplate transactionTemplate(PlatformTransactionManager transactionManager) {
            return new TransactionTemplate(transactionManager);
        }

        @Bean
        TestPushEndpoint testPushEndpoint() {
            return new TestPushEndpoint();
        }
    }

    @RestController
    @RequestMapping("/api")
    static class TestPushEndpoint extends SseController<TestChannels> {}

    //Transazioni senza risorse: bastano a far scattare gli eventi AFTER_COMMIT
    static class NoOpTransactionManager extends AbstractPlatformTransactionManager {

        @Override
        protected Object doGetTransaction() {
            return new Object();
        }

        @Override
        protected void doBegin(Object transaction, TransactionDefinition definition) {
        }

        @Override
        protected void doCommit(DefaultTransactionStatus status) {
        }

        @Override
        protected void doRollback(DefaultTransactionStatus status) {
        }
    }
}
