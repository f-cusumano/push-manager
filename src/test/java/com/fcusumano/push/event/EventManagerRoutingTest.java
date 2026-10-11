package com.fcusumano.push.event;

import com.fcusumano.push.CapturingChannel;
import com.fcusumano.push.TestPayloads.TestChannels;
import com.fcusumano.push.registry.SseRegistry;
import com.fcusumano.push.registry.StompRegistry;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.json.JsonMapper;

import java.util.ArrayList;
import java.util.List;

import static com.fcusumano.push.TestPayloads.user1;

//Regressione della guardia di EventManager: Spring consegna ogni PushEvent a ogni manager, qualunque sia il protocollo
class EventManagerRoutingTest {

    private static final JsonMapper MAPPER = JsonMapper.builder().build();

    private final List<PushEvent<?>> sseHandled = new ArrayList<>();
    private final List<PushEvent<?>> stompHandled = new ArrayList<>();
    private final SseEventManager sseManager = new SseEventManager(new SseRegistry(MAPPER)) {
        @Override
        public void handle(SsePushEvent<?> event) {
            sseHandled.add(event);
        }
    };
    private final StompEventManager stompManager = new StompEventManager(new StompRegistry(MAPPER, CapturingChannel::new)) {
        @Override
        public void handle(StompPushEvent<?> event) {
            stompHandled.add(event);
        }
    };

    @Test
    @SuppressWarnings({"rawtypes", "unchecked"})
    void stompEventIsIgnoredBySseManager() {
        //Chiamata raw come quella di Spring: senza guardia il bridge di handle lancerebbe ClassCastException
        ((EventManager) sseManager).onEvent(StompPushEvent.of(user1(), EventType.CREATED, TestChannels.USERS));
        Assertions.assertTrue(sseHandled.isEmpty());
    }

    @Test
    @SuppressWarnings({"rawtypes", "unchecked"})
    void sseEventIsIgnoredByStompManager() {
        ((EventManager) stompManager).onEvent(SsePushEvent.of(user1(), EventType.CREATED, TestChannels.USERS));
        Assertions.assertTrue(stompHandled.isEmpty());
    }

    @Test
    void ownEventsAreHandled() {
        final SsePushEvent<?> sseEvent = SsePushEvent.of(user1(), EventType.CREATED, TestChannels.USERS);
        final StompPushEvent<?> stompEvent = StompPushEvent.of(user1(), EventType.UPDATED, TestChannels.USERS);
        sseManager.onEvent(sseEvent);
        stompManager.onEvent(stompEvent);
        Assertions.assertEquals(List.of(sseEvent), sseHandled);
        Assertions.assertEquals(List.of(stompEvent), stompHandled);
    }
}
