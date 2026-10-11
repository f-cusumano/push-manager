package com.fcusumano.push.util;

import com.fcusumano.push.TestPayloads.TestChannels;
import com.fcusumano.push.TestPayloads.UserDto;
import com.fcusumano.push.client.StompClient;
import com.fcusumano.push.client.StompSubscription;
import com.fcusumano.push.event.EventType;
import com.fcusumano.push.event.StompPushEvent;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.messaging.Message;
import org.springframework.messaging.simp.stomp.StompCommand;
import org.springframework.messaging.simp.stomp.StompHeaderAccessor;
import org.springframework.messaging.support.MessageHeaderAccessor;

import static com.fcusumano.push.TestPayloads.user1;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.springframework.messaging.simp.stomp.StompHeaderAccessor.STOMP_CONTENT_TYPE_HEADER;
import static org.springframework.messaging.simp.stomp.StompHeaderAccessor.STOMP_DESTINATION_HEADER;
import static org.springframework.messaging.simp.stomp.StompHeaderAccessor.STOMP_MESSAGE_ID_HEADER;
import static org.springframework.messaging.simp.stomp.StompHeaderAccessor.STOMP_SUBSCRIPTION_HEADER;

class StompMapperTest {

    private static final StompSubscription SUBSCRIPTION = StompSubscription.of("SESSION-1", "sub-0");
    private static final byte[] PAYLOAD = "{\"id\":7}".getBytes(UTF_8);

    @Test
    void messageHeadersAreThoseOfTheSubscription() {
        final Message<byte[]> message = StompMapper.toMessage(SUBSCRIPTION, "/push/USERS", EventType.CREATED, "EVENT-1", null, PAYLOAD);
        //Come StompSubProtocolHandler prima della codifica del frame
        final StompHeaderAccessor accessor = StompHeaderAccessor.wrap(message);
        accessor.updateStompCommandAsServerMessage();
        Assertions.assertEquals(StompCommand.MESSAGE, accessor.getCommand());
        Assertions.assertEquals("SESSION-1", accessor.getSessionId());
        Assertions.assertEquals("sub-0", accessor.getFirstNativeHeader(STOMP_SUBSCRIPTION_HEADER));
        Assertions.assertTrue(accessor.getFirstNativeHeader(STOMP_MESSAGE_ID_HEADER).startsWith("SESSION-1-"));
        Assertions.assertEquals("/push/USERS", accessor.getFirstNativeHeader(STOMP_DESTINATION_HEADER));
        Assertions.assertEquals("application/json", accessor.getFirstNativeHeader(STOMP_CONTENT_TYPE_HEADER));
        Assertions.assertEquals("CREATED", accessor.getFirstNativeHeader(StompMapper.EVENT_TYPE_HEADER));
        Assertions.assertEquals("EVENT-1", accessor.getFirstNativeHeader(StompMapper.EVENT_ID_HEADER));
        Assertions.assertNull(accessor.getFirstNativeHeader(StompMapper.RETRY_MS_HEADER));
        Assertions.assertArrayEquals(PAYLOAD, message.getPayload());
    }

    @Test
    void retryOnlyWhenSet() {
        final StompClient<UserDto> client = new StompClient<>(SUBSCRIPTION, TestChannels.USERS, "/push/USERS");
        final StompPushEvent<UserDto> event = StompPushEvent.of(user1(), EventType.CREATED, TestChannels.USERS);
        final Message<byte[]> withoutRetry = StompMapper.toMessage(client, event, PAYLOAD);
        Assertions.assertNull(nativeHeader(withoutRetry, StompMapper.RETRY_MS_HEADER));
        //Id dell'evento applicativo, non casuale
        Assertions.assertEquals(event.getMessageId(), nativeHeader(withoutRetry, StompMapper.EVENT_ID_HEADER));

        final Message<byte[]> withRetry = StompMapper.toMessage(client, StompPushEvent.of(user1(), EventType.CREATED, TestChannels.USERS, 5000L), PAYLOAD);
        Assertions.assertEquals("5000", nativeHeader(withRetry, StompMapper.RETRY_MS_HEADER));
    }

    @Test
    void receiptCarriesReceiptIdAndSession() {
        final Message<byte[]> receipt = StompMapper.toReceipt("SESSION-1", "r-1");
        final StompHeaderAccessor accessor = StompHeaderAccessor.wrap(receipt);
        Assertions.assertEquals(StompCommand.RECEIPT, accessor.getCommand());
        Assertions.assertEquals("r-1", accessor.getReceiptId());
        Assertions.assertEquals("SESSION-1", accessor.getSessionId());
        Assertions.assertEquals(0, receipt.getPayload().length);
        //StompSubProtocolHandler usa così com'è l'accessor STOMP del messaggio
        Assertions.assertInstanceOf(StompHeaderAccessor.class, MessageHeaderAccessor.getAccessor(receipt, MessageHeaderAccessor.class));
    }

    private static String nativeHeader(Message<?> message, String name) {
        return StompHeaderAccessor.wrap(message).getFirstNativeHeader(name);
    }
}
