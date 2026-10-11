package com.fcusumano.push.event;

import com.fcusumano.push.TestPayloads.TestChannels;
import com.fcusumano.push.TestPayloads.UserDto;
import com.fcusumano.push.channel.ChannelEnum;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static com.fcusumano.push.TestPayloads.user1;

class StompPushEventTest {

    @Test
    void payloadSubclassIsRejected() {
        //Stessa regola di SsePushEvent: la classe del payload deve essere esattamente quella del canale
        try {
            StompPushEvent.of(new SpecialPayload(), EventType.CREATED, PayloadChannels.PAYLOADS);
            Assertions.fail("Payload subclass should be rejected");
        } catch (IllegalArgumentException e) {
            Assertions.assertEquals("Payload type 'SpecialPayload' not allowed on channel 'PAYLOADS'", e.getMessage());
        }
        try {
            new StompPushEvent<>(new SpecialPayload(), EventType.CREATED, PayloadChannels.PAYLOADS, 1000L);
            Assertions.fail("Payload subclass should be rejected");
        } catch (IllegalArgumentException _) {
        }
        Assertions.assertEquals(PayloadChannels.PAYLOADS, StompPushEvent.of(new Payload(), EventType.MESSAGE, PayloadChannels.PAYLOADS).getChannel());
    }

    @Test
    void factoriesKeepEventData() {
        final StompPushEvent<UserDto> event = StompPushEvent.of(user1(), EventType.CREATED, TestChannels.USERS);
        Assertions.assertEquals(user1(), event.getData());
        Assertions.assertEquals(EventType.CREATED, event.getEventType());
        Assertions.assertEquals(TestChannels.USERS, event.getChannel());
        Assertions.assertEquals("UserDto", event.getDataType());
        Assertions.assertNotNull(event.getMessageId());
        Assertions.assertNull(event.getRetryMs());

        final StompPushEvent<UserDto> withRetry = StompPushEvent.of(user1(), EventType.UPDATED, TestChannels.USERS, 5000L);
        Assertions.assertEquals(EventType.UPDATED, withRetry.getEventType());
        Assertions.assertEquals(TestChannels.USERS, withRetry.getChannel());
        Assertions.assertEquals(5000L, withRetry.getRetryMs());
        //Ogni evento ha il suo id
        Assertions.assertNotEquals(event.getMessageId(), withRetry.getMessageId());
    }

    static class Payload {}

    static final class SpecialPayload extends Payload {}

    enum PayloadChannels implements ChannelEnum {
        PAYLOADS;

        @Override
        public String topic() {
            return name();
        }

        @Override
        public Class<?> payloadType() {
            return Payload.class;
        }
    }
}
