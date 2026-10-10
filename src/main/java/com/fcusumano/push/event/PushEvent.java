package com.fcusumano.push.event;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import org.springframework.util.ClassUtils;

import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.UUID;


/**
 * Classe astratta che rappresenta un evento push generato dal sistema di
 * messaggistica. Un {@code PushEvent} incapsula le informazioni fondamentali
 * associate all'invio di un messaggio, inclusi identificativo, timestamp
 * e contenuto applicativo.
 *
 * <p>
 * Ogni evento contiene un payload parametrizzato tramite il generics {@code T},
 * che rappresenta il tipo dell'informazione trasportata. La classe fornisce
 * inoltre metadati utili per il tracciamento, la diagnostica e la gestione
 * di eventuali retry.
 * </p>
 *
 * <h2>Informazioni contenute nell'evento</h2>
 * <ul>
 *     <li><b>messageId</b>: identificativo univoco dell'evento, generato in formato UUID.</li>
 *     <li><b>messageCreationUTC</b>: timestamp della creazione dell'evento espresso in UTC.</li>
 *     <li><b>eventType</b>: tipologia dell’evento, rappresentata da {@link EventType}.</li>
 *     <li><b>data</b>: contenuto dell’evento, di tipo generico {@code T}.</li>
 *     <li><b>dataType</b>: semplice nome della classe del payload, utile per debug e serializzazione.</li>
 *     <li><b>retryMs</b>: valore opzionale che indica tra quanti millisecondi l'evento può essere ritentato.</li>
 * </ul>
 *
 * <h2>Costruzione dell'evento</h2>
 * <p>
 * La classe fornisce due costruttori protetti:
 * </p>
 *
 * <ul>
 *     <li>
 *         {@code PushEvent(T obj, EventType eventType)}
 *         inizializza l'evento generando automaticamente:
 *         <ul>
 *             <li>un {@code messageId} univoco;</li>
 *             <li>il {@code messageCreationUTC} corrente in UTC;</li>
 *             <li>il {@code dataType} basato sul tipo runtime dell'oggetto {@code obj}.</li>
 *         </ul>
 *     </li>
 *     <li>
 *         {@code PushEvent(T obj, EventType eventType, Long retryMs)}
 *         estende il costruttore principale aggiungendo la configurazione del retry.
 *     </li>
 * </ul>
 *
 * <h2>Comportamento del metodo description()</h2>
 * <p>
 * Il metodo {@link #description()} produce una rappresentazione testuale dell'evento,
 * utile per logging e debugging. Include:
 * </p>
 * <ul>
 *     <li>il nome della classe concreta dell'evento;</li>
 *     <li>il message-id;</li>
 *     <li>la data di creazione in UTC;</li>
 *     <li>il tipo di evento;</li>
 *     <li>il tipo del payload e il suo contenuto.</li>
 * </ul>
 *
 * <h2>Estendibilità</h2>
 * <p>
 * Le classi concrete devono estendere {@code PushEvent<T>} specificando:
 * </p>
 * <ul>
 *     <li>il tipo del payload trasportato;</li>
 *     <li>l’eventType associato;</li>
 *     <li>eventuali campi o metodi aggiuntivi specifici del dominio applicativo.</li>
 * </ul>
 *
 * <h2>Esempio di implementazione concreta</h2>
 * <pre>{@code
 * public class SsePushEvent<T> extends PushEvent<T> {
 *     ...
 * }
 * }</pre>
 * ... e poi nel codice
 * <pre>{@code
 * SsePushEvent<User> userEvent = new SsePushEvent<>(...);
 * }</pre>
 *
 * <h2>Note</h2>
 * <ul>
 *     <li>La classe non è immutabile, poiché il campo {@code retryMs} è modificabile.</li>
 *     <li>Il timestamp è sempre calcolato in UTC mediante {@link ZoneOffset#UTC}.</li>
 *     <li>Il campo {@code dataType} viene valorizzato "Void" se {@code obj} è nullo.</li>
 * </ul>
 *
 * @author Francesco Cusumano
 */
public abstract class PushEvent<T> {

    protected final @NotBlank String messageId;
    protected final @NotNull OffsetDateTime messageCreationUTC;
    protected final @NotNull EventType eventType;
    protected final T data;
    protected final @NotBlank String dataType;
    protected Long retryMs;

    protected PushEvent(T obj, @NotNull EventType eventType) {
        messageId = UUID.randomUUID().toString();
        messageCreationUTC = OffsetDateTime.now(ZoneOffset.UTC);
        this.eventType = eventType;
        data = obj;
        dataType = obj == null ? "Void" : obj.getClass().getSimpleName();
    }

    protected PushEvent(T obj, @NotNull EventType eventType, Long retryMs) {
        this(obj, eventType);
        this.retryMs = retryMs;
    }

    public String getMessageId() {
        return messageId;
    }

    public OffsetDateTime getMessageCreationUTC() {
        return messageCreationUTC;
    }

    public EventType getEventType() {
        return eventType;
    }

    public T getData() {
        return data;
    }

    public String getDataType() {
        return dataType;
    }

    public Long getRetryMs() {
        return retryMs;
    }

    public void setRetryMs(Long retryMs) {
        this.retryMs = retryMs;
    }

    /** @return descrizione leggibile dell'evento, pensata per i log */
    public String description() {
        return ClassUtils.getUserClass(this).getSimpleName() + " with message-id '" + messageId +
               "' created at " + messageCreationUTC + " (UTC), event-type '" + eventType +
               "' containing data-type '" + dataType + "' (" + data + ")";
    }
}
