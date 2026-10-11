package com.fcusumano.push.event;

import com.fcusumano.push.channel.ChannelEnum;
import jakarta.validation.constraints.NotNull;

import java.util.Objects;


/**
 * Evento da consegnare via STOMP ai client sottoscritti a un canale.
 * <p>
 * Ha la stessa API di {@link SsePushEvent}: il payload deve essere della classe esatta dichiarata dal canale.
 * </p>
 * <pre>{@code
 * stompEventManager.publishEvent(StompPushEvent.of(user, EventType.CREATED, Channels.USERS));
 * }</pre>
 *
 * @param <T> tipo del payload
 *
 * @author Francesco Cusumano
 */
public class StompPushEvent<T> extends PushEvent<T> {

    private final @NotNull ChannelEnum channel;

    /**
     * @param obj payload dell'evento, della classe esatta dichiarata dal canale
     * @param eventType tipo dell'evento
     * @param channel canale su cui pubblicare l'evento
     * @param retryMs tempo di riconnessione suggerito al client, inviato nell'header {@code retry-ms} (opzionale)
     * @throws IllegalArgumentException se la classe del payload non è quella del canale
     */
    public StompPushEvent(@NotNull T obj, @NotNull EventType eventType, @NotNull ChannelEnum channel, Long retryMs) {
        if (!Objects.equals(obj.getClass(), channel.payloadType())) {
            throw new IllegalArgumentException("Payload type '" + obj.getClass().getSimpleName() +
                                               "' not allowed on channel '" + channel.topic() + "'");
        }
        super(obj, eventType, retryMs);
        this.channel = channel;
    }

    /** Come {@link #StompPushEvent(Object, EventType, ChannelEnum, Long)}, senza retry. */
    public StompPushEvent(@NotNull T obj, @NotNull EventType eventType, @NotNull ChannelEnum channel) {
        this(obj, eventType, channel, null);
    }

    public static <D> StompPushEvent<D> of(@NotNull D obj, @NotNull EventType eventType, @NotNull ChannelEnum channel, Long retryMs) {
        return new StompPushEvent<>(obj, eventType, channel, retryMs);
    }

    public static <D> StompPushEvent<D> of(@NotNull D obj, @NotNull EventType eventType, @NotNull ChannelEnum channel) {
        return of(obj, eventType, channel, null);
    }

    public ChannelEnum getChannel() {
        return channel;
    }
}
