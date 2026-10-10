package com.fcusumano.push.client;

import com.fcusumano.push.channel.ChannelEnum;
import com.fcusumano.push.filter.FilteringOptions;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import static com.fcusumano.push.client.ClientStatus.SUBSCRIBED;


/**
 * Rappresenta un client connesso tramite Server-Sent Events (SSE) all'interno
 * del sistema di Push Messaging. La classe incapsula tutte le informazioni
 * necessarie per la gestione della sessione, del canale, dello stato corrente
 * e delle opzioni di filtraggio applicate ai messaggi.
 *
 * <p>
 * Ogni istanza di {@code SseClient} identifica una sessione SSE univoca,
 * associata a un canale (implementato tramite {@link com.fcusumano.push.channel.ChannelEnum})
 * e caratterizzata da uno stato definito da {@link com.fcusumano.push.client.ClientStatus}.
 * </p>
 *
 * <h2>Componenti principali</h2>
 * <ul>
 *     <li><b>sessionId</b> – Identificativo univoco della sessione SSE.</li>
 *     <li><b>channel</b> – Canale sottoscritto dal client, utilizzato per determinare
 *         il tipo di payload e instradare correttamente i messaggi.</li>
 *     <li><b>status</b> – Stato corrente del client (es. SUBSCRIBED, STREAMING, DISABLED).</li>
 *     <li><b>emitter</b> – Oggetto {@link org.springframework.web.servlet.mvc.method.annotation.SseEmitter}
 *         responsabile della comunicazione unidirezionale verso il client.</li>
 *     <li><b>filteringOptions</b> – Opzioni di filtraggio specifiche per il client,
 *         applicate ai payload del canale parametrizzati tramite il generics {@code T}.</li>
 * </ul>
 *
 * <h2>Funzionamento</h2>
 * <p>
 * Quando un client si connette a un canale SSE, viene creata un'istanza di questa classe
 * che contiene il riferimento all'emitter e al tipo di payload. Il metodo {@link #payloadType()}
 * permette di recuperare il tipo del payload direttamente dal canale sottoscritto.
 * </p>
 *
 * <h2>Costruzione</h2>
 * <p>
 * Il costruttore richiede:
 * </p>
 * <ul>
 *     <li>un <b>sessionId</b> valido e non vuoto,</li>
 *     <li>un <b>channel</b> non nullo,</li>
 *     <li>un <b>SseEmitter</b> non nullo.</li>
 * </ul>
 * <p>
 * Lo stato iniziale del client è impostato automaticamente su {@code SUBSCRIBED}.
 * </p>
 *
 * <h2>Utilizzo</h2>
 * <pre>{@code
 * SseEmitter emitter = new SseEmitter(properties.timeoutMs());
 * SseClient<MyPayload> client = new SseClient<>(
 *     "session-123",
 *     MyChannels.CONNECTED_USERS,
 *     emitter
 * );
 *
 * client.setStatus(ClientStatus.STREAMING);
 * client.setFilteringOptions(new FilteringOptions<>(...));
 * }</pre>
 *
 * <h2>Note</h2>
 * <ul>
 *     <li>La classe non è thread-safe: eventuali sincronizzazioni devono essere
 *         gestite dal chiamante in contesti concorrenti.</li>
 *     <li>Le opzioni di filtraggio sono opzionali e possono essere configurate
 *         in un secondo momento.</li>
 *     <li>Il tipo {@code T} è dedotto dal payload associato al canale via
 *         {@link com.fcusumano.push.channel.ChannelEnum#payloadType()}.</li>
 * </ul>
 *
 * @author Francesco Cusumano
 */
public class SseClient<T> {

    private final @NotBlank String sessionId;
    private final @NotNull ChannelEnum channel;
    private @NotNull ClientStatus status;
    private final @NotNull SseEmitter emitter;
    private FilteringOptions<T> filteringOptions;


    public SseClient(@NotBlank String sessionId,
                     @NotNull ChannelEnum channel,
                     @NotNull SseEmitter emitter) {
        this.sessionId = sessionId;
        this.channel = channel;
        this.status = SUBSCRIBED;
        this.emitter = emitter;
    }

    public String sessionId() {
        return sessionId;
    }

    public ChannelEnum channel() {
        return channel;
    }

    public ClientStatus status() {
        return status;
    }

    public void setStatus(@NotNull ClientStatus status) {
        this.status = status;
    }

    public SseEmitter emitter() {
        return emitter;
    }

    public FilteringOptions<T> filteringOptions() {
        return filteringOptions;
    }

    public Class<T> payloadType() {
        return (Class<T>) channel.payloadType();
    }

    public void setFilteringOptions(FilteringOptions<T> filteringOptions) {
        this.filteringOptions = filteringOptions;
    }
}
