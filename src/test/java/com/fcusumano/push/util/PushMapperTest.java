package com.fcusumano.push.util;

import com.fcusumano.push.TestPayloads.TestChannels;
import com.fcusumano.push.TestPayloads.UserDto;
import com.fcusumano.push.event.EventType;
import com.fcusumano.push.event.SsePushEvent;
import com.fcusumano.push.exception.InvalidFilterException;
import com.fcusumano.push.filter.CheckType;
import com.fcusumano.push.filter.StreamingFilterRequest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter.SseEventBuilder;
import tools.jackson.databind.json.JsonMapper;

import java.util.stream.Collectors;

import static com.fcusumano.push.TestPayloads.user1;
import static com.fcusumano.push.filter.CheckType.EXACT_LENGTH;
import static com.fcusumano.push.filter.CheckType.GREATER_THEN;
import static com.fcusumano.push.filter.CheckType.LESS_THEN;
import static com.fcusumano.push.filter.CheckType.STARTS_WITH;

class PushMapperTest {

    private static final JsonMapper MAPPER = JsonMapper.builder().build();

    @Test
    void valuesAreConvertedToFieldType() {
        //Intero JSON su campo Long
        Assertions.assertTrue(test(new StreamingFilterRequest("id", GREATER_THEN, 1, null)));
        Assertions.assertFalse(test(new StreamingFilterRequest("id", GREATER_THEN, 100, null)));
        //Stringa ISO su campo OffsetDateTime
        Assertions.assertTrue(test(new StreamingFilterRequest("createdAt", LESS_THEN, "2030-01-01T00:00:00Z", null)));
        //Intero su campo primitivo int
        Assertions.assertTrue(test(new StreamingFilterRequest("age", GREATER_THEN, 18, null)));
        //Check tra tipi diversi: lunghezza di una stringa
        Assertions.assertTrue(test(new StreamingFilterRequest("name", EXACT_LENGTH, 5, null)));
    }

    @Test
    void fieldsOfSameTypeAreCompared() {
        Assertions.assertTrue(test(new StreamingFilterRequest("createdAt", LESS_THEN, null, "updatedAt")));
    }

    @Test
    void inconsistentFiltersAreRejected() {
        rejected(new StreamingFilterRequest("age", LESS_THEN, null, "id"));         //int con Long
        rejected(new StreamingFilterRequest("id", STARTS_WITH, "7", null));         //check non definito per Long
        rejected(new StreamingFilterRequest("id", GREATER_THEN, "abc", null));      //valore non convertibile
        rejected(new StreamingFilterRequest("surname", CheckType.FIELD_PRESENT, null, null)); //campo inesistente
    }

    @Test
    void reconnectTimeOnlyWhenSet() {
        Assertions.assertFalse(render(SsePushEvent.of(user1(), EventType.CREATED, TestChannels.USERS)).contains("retry:"));
        Assertions.assertTrue(render(SsePushEvent.of(user1(), EventType.CREATED, TestChannels.USERS, 5000L)).contains("retry:5000"));
    }

    private static boolean test(StreamingFilterRequest filter) {
        return PushMapper.mapToPredicate(filter, UserDto.class, MAPPER).test(user1());
    }

    private static void rejected(StreamingFilterRequest filter) {
        try {
            PushMapper.mapToPredicate(filter, UserDto.class, MAPPER);
            Assertions.fail("Filter " + filter + " should be rejected");
        } catch (InvalidFilterException _) {
        }
    }

    private static String render(SsePushEvent<?> event) {
        final SseEventBuilder builder = PushMapper.toEventBuilder(event, "data");
        return builder.build().stream().map(d -> String.valueOf(d.getData())).collect(Collectors.joining());
    }
}
