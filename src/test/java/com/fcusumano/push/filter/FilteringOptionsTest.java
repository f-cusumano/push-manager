package com.fcusumano.push.filter;

import com.fcusumano.push.TestPayloads.TestChannels;
import com.fcusumano.push.TestPayloads.UserDto;
import com.fcusumano.push.event.EventType;
import com.fcusumano.push.event.PushEvent;
import com.fcusumano.push.event.StompPushEvent;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

import static com.fcusumano.push.TestPayloads.user1;

class FilteringOptionsTest {

    @Test
    void requiredTypesNeverExcludeNonMuteableEvents() {
        final FilteringOptions<UserDto> options = new FilteringOptions<>(List.of(EventType.CREATED), null, null);
        for (EventType eventType : EventType.values()) {
            Assertions.assertEquals(switch (eventType) {
                case CONNECTED, DISCONNECTED, HEARTBEAT, ERROR -> true; //Non muteable: sempre ammessi
                case CREATED -> true;                                   //Richiesto
                case MESSAGE, UPDATED, DELETED -> false;
            }, options.accepts(event(eventType)), eventType.name());
        }
    }

    @Test
    void noRequiredTypesMeansAllTypes() {
        final FilteringOptions<UserDto> nullTypes = new FilteringOptions<>(null, null, null);
        final FilteringOptions<UserDto> emptyTypes = new FilteringOptions<>(List.of(), null, null);
        for (EventType eventType : EventType.values()) {
            Assertions.assertTrue(nullTypes.accepts(event(eventType)), eventType.name());
            Assertions.assertTrue(emptyTypes.accepts(event(eventType)), eventType.name());
        }
    }

    @Test
    void conditionIsAppliedToContent() {
        Assertions.assertTrue(new FilteringOptions<UserDto>(null, null, null).accepts(event(EventType.CREATED)));
        Assertions.assertTrue(new FilteringOptions<UserDto>(null, user -> "FOO-1".equals(user.name()), null).accepts(event(EventType.CREATED)));
        Assertions.assertFalse(new FilteringOptions<UserDto>(null, user -> user.age() > 50, null).accepts(event(EventType.CREATED)));
        //Tipo richiesto ma contenuto escluso
        Assertions.assertFalse(new FilteringOptions<UserDto>(List.of(EventType.CREATED), _ -> false, null).accepts(event(EventType.CREATED)));
        //Il contenuto vale anche per gli eventi non muteable, come in SseRegistry
        Assertions.assertFalse(new FilteringOptions<UserDto>(List.of(EventType.CREATED), _ -> false, null).accepts(event(EventType.ERROR)));
    }

    private static PushEvent<UserDto> event(EventType eventType) {
        return StompPushEvent.of(user1(), eventType, TestChannels.USERS);
    }
}
