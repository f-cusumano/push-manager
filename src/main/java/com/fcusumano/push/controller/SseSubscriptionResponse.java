package com.fcusumano.push.controller;

import com.fcusumano.push.channel.ChannelEnum;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;


/**
 * Rappresenta la risposta inviata al client in seguito a una richiesta di
 * sottoscrizione a un canale Server-Sent Events (SSE).
 *
 * <p>
 * La risposta contiene il riepilogo delle informazioni essenziali riguardanti
 * la sessione appena creata, inclusi:
 * </p>
 * <ul>
 *     <li>il <b>sessionId</b> generato per il client;</li>
 *     <li>il <b>canale</b> sottoscritto, rappresentato da un {@link com.fcusumano.push.channel.ChannelEnum};</li>
 *     <li>il <b>nome del tipo di payload</b> trasmesso dal canale, utile ai client
 *         per comprendere la natura degli eventi che riceveranno.</li>
 * </ul>
 *
 * <h2>Scopo</h2>
 * <p>
 * Questa classe viene tipicamente serializzata e ritornata al client come
 * risposta HTTP al momento della sottoscrizione iniziale. Il client utilizza
 * tali informazioni per:
 * </p>
 * <ul>
 *     <li>identificare univocamente la sessione SSE;</li>
 *     <li>associare la connessione al canale corretto;</li>
 *     <li>determinare, tramite {@code streamedObject}, il tipo di oggetto
 *         che verrà inviato tramite eventi SSE.</li>
 * </ul>
 *
 * <h2>Calcolo del campo streamedObject</h2>
 * <p>
 * Il campo {@code streamedObject} viene popolato automaticamente recuperando il
 * tipo di payload dichiarato dal canale tramite
 * {@link com.fcusumano.push.channel.ChannelEnum#payloadType()} e
 * utilizzando il suo simple name.
 * Ad esempio, per un canale che trasmette payload del tipo
 * <code>User</code>, il valore sarà:
 * </p>
 *
 * <pre><code>
 * "User"
 * </code></pre>
 *
 * <h2>Thread-safety</h2>
 * <p>
 * La classe è immutabile: tutti i campi sono final e non esiste alcun metodo
 * setter, rendendola sicura per l'utilizzo concorrente.
 * </p>
 *
 * @author Francesco Cusumano
 */
public class SseSubscriptionResponse {

    private final @NotBlank String sessionId;
    private final @NotNull ChannelEnum channel;
    private final String streamedObject;

    public SseSubscriptionResponse(@NotBlank String sessionId, @NotNull ChannelEnum channel) {
        this.sessionId = sessionId;
        this.channel = channel;
        this.streamedObject = channel.payloadType().getSimpleName();
    }

    public String getSessionId() {
        return sessionId;
    }

    public ChannelEnum getChannel() {
        return channel;
    }

    public String getStreamedObject() {
        return streamedObject;
    }
}
