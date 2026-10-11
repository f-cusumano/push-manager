package com.fcusumano.push;

import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.simp.SimpMessageHeaderAccessor;
import org.springframework.messaging.simp.SimpMessageType;
import org.springframework.messaging.simp.stomp.StompCommand;
import org.springframework.messaging.simp.stomp.StompHeaderAccessor;
import org.springframework.messaging.support.NativeMessageHeaderAccessor;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static com.fcusumano.push.util.StompMapper.EVENT_TYPE_HEADER;

//Canale verso i client che trattiene i messaggi, al posto del clientOutboundChannel di Spring
public final class CapturingChannel implements MessageChannel {

    private static final JsonMapper MAPPER = JsonMapper.builder().build();

    private final List<Message<?>> sent = new CopyOnWriteArrayList<>();

    @Override
    public boolean send(Message<?> message, long timeout) {
        return sent.add(message);
    }

    //MESSAGE inviati alle sottoscrizioni: CONNECTED, ERROR ed eventi
    public List<Message<?>> messages() {
        return sent.stream().filter(m -> SimpMessageHeaderAccessor.getMessageType(m.getHeaders()) == SimpMessageType.MESSAGE).toList();
    }

    public List<Message<?>> messages(String subscriptionId) {
        return messages().stream().filter(m -> subscriptionId.equals(SimpMessageHeaderAccessor.getSubscriptionId(m.getHeaders()))).toList();
    }

    //L'unico MESSAGE inviato
    public Message<?> singleMessage() {
        final List<Message<?>> messages = messages();
        if (messages.size() != 1) throw new AssertionError("Expected one message, found " + messages.size() + ": " + messages);
        return messages.getFirst();
    }

    public List<String> receiptIds() {
        return sent.stream()
                .map(StompHeaderAccessor::wrap)
                .filter(accessor -> accessor.getCommand() == StompCommand.RECEIPT)
                .map(StompHeaderAccessor::getReceiptId)
                .toList();
    }

    public boolean isEmpty() {
        return sent.isEmpty();
    }

    public void clear() {
        sent.clear();
    }

    public static String eventType(Message<?> message) {
        return header(message, EVENT_TYPE_HEADER);
    }

    public static String header(Message<?> message, String name) {
        return NativeMessageHeaderAccessor.getFirstNativeHeader(name, message.getHeaders());
    }

    public static JsonNode body(Message<?> message) {
        return MAPPER.readTree((byte[]) message.getPayload());
    }

    public static JsonNode json(String content) {
        return MAPPER.readTree(content);
    }
}
