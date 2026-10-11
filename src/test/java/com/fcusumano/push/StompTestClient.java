package com.fcusumano.push;

import org.junit.jupiter.api.Assertions;
import org.springframework.messaging.simp.stomp.StompFrameHandler;
import org.springframework.messaging.simp.stomp.StompHeaders;
import org.springframework.messaging.simp.stomp.StompSession;
import org.springframework.messaging.simp.stomp.StompSessionHandlerAdapter;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.web.socket.client.standard.StandardWebSocketClient;
import org.springframework.web.socket.messaging.WebSocketStompClient;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import java.lang.reflect.Type;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BooleanSupplier;

import static com.fcusumano.push.util.StompMapper.EVENT_TYPE_HEADER;
import static com.fcusumano.push.util.StompMapper.FILTERS_HEADER;
import static com.fcusumano.push.util.StompMapper.RETRY_MS_HEADER;
import static java.nio.charset.StandardCharsets.UTF_8;

//Client STOMP reale dei test end-to-end: WebSocketStompClient con lo scheduler richiesto dalle ricevute
final class StompTestClient implements AutoCloseable {

    static final long TIMEOUT_MS = 5000;
    private static final JsonMapper MAPPER = JsonMapper.builder().build();

    private final ThreadPoolTaskScheduler scheduler = new ThreadPoolTaskScheduler();
    private final WebSocketStompClient client = new WebSocketStompClient(new StandardWebSocketClient());
    private final List<StompSession> sessions = new CopyOnWriteArrayList<>();

    StompTestClient() {
        scheduler.initialize();
        client.setTaskScheduler(scheduler); //Senza scheduler le ricevute non vengono tracciate
    }

    StompSession connect(String url) throws InterruptedException, ExecutionException, TimeoutException {
        final StompSession session = client.connectAsync(url, new StompSessionHandlerAdapter() {}).get(TIMEOUT_MS, TimeUnit.MILLISECONDS);
        sessions.add(session);
        return session;
    }

    //Chiude le sessioni ancora aperte
    @Override
    public void close() {
        sessions.stream().filter(StompSession::isConnected).forEach(StompSession::disconnect);
        scheduler.shutdown();
    }

    //SUBSCRIBE con ricevuta 'r-{id}' e filtri opzionali
    static PushSubscription subscribe(StompSession session, String destination, String id, String filters) {
        final StompHeaders headers = new StompHeaders();
        headers.setDestination(destination);
        headers.setId(id);
        headers.setReceipt("r-" + id);
        if (filters != null) headers.add(FILTERS_HEADER, filters);
        final Recorder recorder = new Recorder();
        final StompSession.Subscription subscription = session.subscribe(headers, recorder);
        final CountDownLatch receipt = new CountDownLatch(1);
        subscription.addReceiptTask(receipt::countDown);
        return new PushSubscription(subscription, recorder, receipt);
    }

    //Sottoscrizione accettata: CONNECTED e RECEIPT, inviato quando il client è già in streaming
    static PushSubscription subscribeAccepted(StompSession session, String destination, String id, String filters)
            throws InterruptedException {
        final PushSubscription subscription = subscribe(session, destination, id, filters);
        Assertions.assertEquals("CONNECTED", subscription.next().eventType());
        Assertions.assertTrue(subscription.receipt().await(TIMEOUT_MS, TimeUnit.MILLISECONDS), "No RECEIPT for subscription '" + id + "'");
        return subscription;
    }

    //SEND con ricevuta: il latch scatta all'arrivo del RECEIPT
    static CountDownLatch send(StompSession session, String destination, String body, String receipt) {
        final StompHeaders headers = new StompHeaders();
        headers.setDestination(destination);
        headers.setReceipt(receipt);
        final CountDownLatch received = new CountDownLatch(1);
        session.send(headers, body.getBytes(UTF_8)).addReceiptTask(received::countDown);
        return received;
    }

    static boolean await(BooleanSupplier condition) throws InterruptedException {
        final long deadline = System.currentTimeMillis() + TIMEOUT_MS;
        while (!condition.getAsBoolean()) {
            if (System.currentTimeMillis() > deadline) return false;
            Thread.sleep(50);
        }
        return true;
    }

    record Frame(String eventType, String subscription, String destination, String retryMs, String body) {

        JsonNode json() {
            return MAPPER.readTree(body);
        }
    }

    //Frame MESSAGE ricevuti su una sottoscrizione, in coda
    static final class Recorder implements StompFrameHandler {

        private final BlockingQueue<Frame> frames = new LinkedBlockingQueue<>();

        @Override
        public Type getPayloadType(StompHeaders headers) {
            return byte[].class;
        }

        @Override
        public void handleFrame(StompHeaders headers, Object payload) {
            frames.add(new Frame(headers.getFirst(EVENT_TYPE_HEADER), headers.getSubscription(), headers.getDestination(),
                    headers.getFirst(RETRY_MS_HEADER), new String((byte[]) payload, UTF_8)));
        }

        Frame next() throws InterruptedException {
            final Frame frame = next(TIMEOUT_MS);
            Assertions.assertNotNull(frame, "No frame received");
            return frame;
        }

        Frame next(long timeoutMs) throws InterruptedException {
            return frames.poll(timeoutMs, TimeUnit.MILLISECONDS);
        }
    }

    record PushSubscription(StompSession.Subscription subscription, Recorder recorder, CountDownLatch receipt) {

        Frame next() throws InterruptedException {
            return recorder.next();
        }

        Frame next(long timeoutMs) throws InterruptedException {
            return recorder.next(timeoutMs);
        }
    }
}
