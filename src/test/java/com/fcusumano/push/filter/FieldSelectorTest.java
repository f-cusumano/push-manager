package com.fcusumano.push.filter;

import com.fcusumano.push.TestPayloads.Address;
import com.fcusumano.push.TestPayloads.UserDto;
import com.fcusumano.push.exception.InvalidFilterException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import java.util.List;

import static com.fcusumano.push.TestPayloads.CREATED_AT;
import static com.fcusumano.push.TestPayloads.anonymous;
import static com.fcusumano.push.TestPayloads.user1;

class FieldSelectorTest {

    private static final JsonMapper MAPPER = JsonMapper.builder().build();

    @Test
    void noFieldsMeansWholePayload() {
        Assertions.assertNull(FieldSelector.of(UserDto.class, null));
        Assertions.assertNull(FieldSelector.of(UserDto.class, List.of()));
    }

    @Test
    void onlySelectedFieldsAreKept() {
        final JsonNode result = select(user1(), "name", "address.city");
        Assertions.assertEquals(json("{\"name\":\"FOO-1\",\"address\":{\"city\":\"CITY-1\"}}"), result);
    }

    @Test
    void selectedFieldBringsAllSubfields() {
        final JsonNode expected = json("{\"address\":{\"city\":\"CITY-1\",\"zip\":\"ZIP-1\"}}");
        //Il campo padre vince sul sottocampo, in qualsiasi ordine
        Assertions.assertEquals(expected, select(user1(), "address"));
        Assertions.assertEquals(expected, select(user1(), "address", "address.city"));
        Assertions.assertEquals(expected, select(user1(), "address.city", "address"));
    }

    @Test
    void pathThroughCollectionAppliesToEachElement() {
        final JsonNode result = select(user1(), "orders.amount");
        Assertions.assertEquals(json("{\"orders\":[{\"amount\":10.5},{\"amount\":20.0}]}"), result);
    }

    @Test
    void allSelectedFieldsEmptyMeansDiscarded() {
        //Campi null, oggetti null, liste vuote
        Assertions.assertNull(select(anonymous(), "name", "address.city", "tags", "orders.amount"));
        //Oggetto valorizzato ma con il solo sottocampo selezionato null
        final UserDto noCity = new UserDto(1L, 0, null, null, CREATED_AT, null, new Address(null, "ZIP-1"), null, null);
        Assertions.assertNull(select(noCity, "name", "address.city"));
        //Basta un campo valorizzato per inviare l'evento, con gli altri campi a null
        Assertions.assertEquals(json("{\"name\":null,\"createdAt\":\"2026-01-01T10:00:00Z\"}"),
                select(anonymous(), "name", "createdAt"));
    }

    @Test
    void zeroFalseAndEmptyStringAreData() {
        final UserDto user = new UserDto(1L, 0, "", false, CREATED_AT, null, null, null, null);
        Assertions.assertEquals(json("{\"age\":0}"), select(user, "age"));
        Assertions.assertEquals(json("{\"name\":\"\"}"), select(user, "name"));
        Assertions.assertEquals(json("{\"active\":false}"), select(user, "active"));
    }

    @Test
    void invalidPathsAreRejected() {
        for (String path : List.of("surname", "address.street", "name.value", "orders.unknown", "tags.length")) {
            try {
                FieldSelector.of(UserDto.class, List.of(path));
                Assertions.fail("Path '" + path + "' should be rejected");
            } catch (InvalidFilterException e) {
                Assertions.assertTrue(e.getMessage().contains("'"), e.getMessage());
            }
        }
    }

    private static JsonNode select(UserDto user, String... fieldsPath) {
        return FieldSelector.of(UserDto.class, List.of(fieldsPath)).select(MAPPER.valueToTree(user));
    }

    private static JsonNode json(String content) {
        return MAPPER.readTree(content);
    }
}
