package com.fcusumano.push.event;

import com.fcusumano.push.channel.ChannelEnum;
import jakarta.validation.constraints.NotNull;

import java.util.Objects;


/**
 * Rappresenta un evento SSE specifico per un determinato canale di messaggistica.
 * {@code SsePushEvent} estende {@link PushEvent} aggiungendo il riferimento
 * al canale {@link ChannelEnum} sul quale l'evento deve essere pubblicato.
 *
 * <p>
 * Questa classe costituisce il payload standard utilizzato dal sistema di Push Messaging
 * per inviare notifiche verso i client tramite Server-Sent Events (SSE).
 * Ogni evento contiene:
 * </p>
 * <ul>
 *     <li>il payload applicativo {@code T};</li>
 *     <li>il tipo di evento {@link EventType};</li>
 *     <li>il canale di destinazione, che definisce il tipo ammesso di payload;</li>
 *     <li>eventuali informazioni di retry;</li>
 *     <li>tutti i metadati ereditati da {@code PushEvent}, inclusi id univoco e timestamp UTC.</li>
 * </ul>
 *
 * <h2>Validazione del payload</h2>
 * <p>Il costruttore effettua una validazione rigorosa assicurando che:</p>
 * <ul>
 *     <li>il tipo del payload {@code obj} corrisponda esattamente al tipo dichiarato
 *         dal canale tramite {@link ChannelEnum#payloadType()};</li>
 *     <li>il canale non accetti payload non conformi;</li>
 *     <li>eventuali errori vengano segnalati tramite {@link IllegalArgumentException}.</li>
 * </ul>
 *
 * <h2>Retry degli eventi</h2>
 * <p>
 * Il costruttore esteso permette di indicare una finestra di retry (in millisecondi)
 * utilizzata dal client SSE per tentare la riconnessione o la riacquisizione dell’evento
 * in caso di errori. Questo valore è opzionale e può essere configurato anche in seguito.
 * </p>
 *
 * <h2>Costruzione tramite factory method</h2>
 * <p>Per comodità, la classe espone due metodi statici:</p>
 * <ul>
 *     <li>{@link #of(Object, EventType, ChannelEnum, Long)} per costruire un evento con retry;</li>
 *     <li>{@link #of(Object, EventType, ChannelEnum)} per costruire un evento senza retry.</li>
 * </ul>
 *
 * <h2>Esempio di utilizzo</h2>
 * <pre>{@code
 * User payload = new User("Mario", "Rossi");
 * SsePushEvent<User> event = SsePushEvent.of(payload, EventType.CREATED, MyChannels.ANGRAFICA_USER);
 * sseEventManager.publishEvent(event);
 * }</pre>
 *
 * <h2>Note</h2>
 * <ul>
 *     <li>La classe è immutabile ad eccezione del parametro {@code retryMs},
 *         modificabile tramite apposito setter ereditato.</li>
 *     <li>Il controllo sul tipo del payload garantisce coerenza e aiuta a evitare errori
 *         di serializzazione lato client.</li>
 *     <li>È progettata per essere istanziata esclusivamente tramite costruttori o factory method.</li>
 * </ul>
 *
 * @author Francesco Cusumano
 */
public class SsePushEvent<T> extends PushEvent<T> {

    private final @NotNull ChannelEnum channel;

    public SsePushEvent(@NotNull T obj, @NotNull EventType eventType, @NotNull ChannelEnum channel, Long retryMs) {
        if (!Objects.equals(obj.getClass(), channel.payloadType())) {
            throw new IllegalArgumentException("Payload type '" + obj.getClass().getSimpleName() +
                                               "' not allowed on channel '" + channel.topic() + "'");
        }
        super(obj, eventType, retryMs);
        this.channel = channel;
    }

    public SsePushEvent(@NotNull T obj, @NotNull EventType eventType, @NotNull ChannelEnum channel) {
        this(obj, eventType, channel, null);
    }

    public static <D> SsePushEvent<D> of(@NotNull D obj, @NotNull EventType eventType, @NotNull ChannelEnum channel, Long retryMs) {
        return new SsePushEvent<>(obj, eventType, channel, retryMs);
    }

    public static <D> SsePushEvent<D> of(@NotNull D obj, @NotNull EventType eventType, @NotNull ChannelEnum channel) {
        return of(obj, eventType, channel, null);
    }

    public ChannelEnum getChannel() {
        return channel;
    }
}
