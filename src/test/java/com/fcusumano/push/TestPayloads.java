package com.fcusumano.push;

import com.fcusumano.push.channel.ChannelEnum;

import java.time.OffsetDateTime;
import java.util.List;

//Payload e canali usati dai test, come li scriverebbe un'applicazione
public final class TestPayloads {

    private TestPayloads() {
    }

    public enum TestChannels implements ChannelEnum {
        USERS(UserDto.class);

        private final Class<?> payloadType;

        TestChannels(Class<?> payloadType) {
            this.payloadType = payloadType;
        }

        @Override
        public String topic() {
            return name();
        }

        @Override
        public Class<?> payloadType() {
            return payloadType;
        }
    }

    public record Address(String city, String zip) {}

    public record Order(String code, Double amount) {}

    public record UserDto(Long id, int age, String name, Boolean active, OffsetDateTime createdAt, OffsetDateTime updatedAt,
                   Address address, List<String> tags, List<Order> orders) {}

    public static final OffsetDateTime CREATED_AT = OffsetDateTime.parse("2026-01-01T10:00:00Z");

    public static UserDto mario() {
        return new UserDto(7L, 30, "Mario", true, CREATED_AT, CREATED_AT.plusDays(1),
                new Address("Roma", "00100"), List.of("vip"),
                List.of(new Order("A1", 10.5), new Order("A2", 20.0)));
    }

    public static UserDto anonymous() {
        return new UserDto(8L, 0, null, null, CREATED_AT, null, null, List.of(), List.of());
    }
}
