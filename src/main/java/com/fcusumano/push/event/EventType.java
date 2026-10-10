package com.fcusumano.push.event;

import io.swagger.v3.oas.annotations.media.Schema;
import org.springframework.util.StringUtils;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.util.UUID;


/**
 * Rappresenta la tipologia di evento generato nel sistema di Push Messaging e
 * utilizzato per notificare i client connessi tramite Server-Sent Events (SSE).
 *
 * <p>
 * Ogni costante dell'enum identifica un evento specifico all'interno del ciclo di vita
 * della connessione o delle operazioni sul dato applicativo, come la creazione,
 * aggiornamento o eliminazione di un'entità. L'enum è inoltre annotato con
 * {@link io.swagger.v3.oas.annotations.media.Schema} per consentire a Swagger/OpenAPI
 * di rappresentare i valori come riferimento enumerativo.
 * </p>
 *
 * <h2>Valori disponibili</h2>
 * <ul>
 *     <li><b>CONNECTED</b> – Notifica l'apertura della connessione SSE.</li>
 *     <li><b>DISCONNECTED</b> – Indica la chiusura della connessione SSE.</li>
 *     <li><b>HEARTBEAT</b> – Evento periodico utilizzato per mantenere attiva la connessione.</li>
 *     <li><b>MESSAGE</b> – Messaggio generico inviato al client.</li>
 *     <li><b>CREATED</b> – Indica la creazione di una nuova entità applicativa.</li>
 *     <li><b>UPDATED</b> – Indica la modifica di un'entità esistente.</li>
 *     <li><b>DELETED</b> – Indica l'eliminazione di un'entità applicativa.</li>
 *     <li><b>ERROR</b> – Segnala un errore verificatosi lato server.</li>
 * </ul>
 *
 * <h2>Proprietà dell'enum</h2>
 * <ul>
 *     <li><b>defaultObj</b> – Messaggio descrittivo di default associato all’evento,
 *         eventualmente utilizzato per popolare il contenuto SSE.</li>
 *     <li><b>muteable</b> – Indica se la trasmissione dell'evento può essere
 *         "silenziata" tramite appositi filtri; tendenzialmente per gli eventi di sistema
 *         dovrebbe essere sempre impostato il valore <b>false</b>.</li>
 * </ul>
 *
 * <h2>Costruzione</h2>
 * <p>
 * L’enum prevede due costruttori:
 * </p>
 * <ul>
 *     <li>
 *         {@code EventType(String defaultObj, boolean muteable)} – consente di specificare
 *         un messaggio descrittivo come oggetto di default dell'evento.
 *     </li>
 *     <li>
 *         {@code EventType(boolean muteable)} – utilizza un messaggio di default vuoto.
 *     </li>
 * </ul>
 *
 * <h2>Metodo sseEventBuilder()</h2>
 * <p>
 * Costruisce un {@link org.springframework.web.servlet.mvc.method.annotation.SseEmitter.SseEventBuilder}
 * configurato per il tipo di evento corrente.
 * L’evento SSE generato conterrà:
 * </p>
 * <ul>
 *     <li>un id univoco generato tramite UUID;</li>
 *     <li>il nome dell’evento (il nome della costante enum);</li>
 *     <li>il valore restituito da {@link #getDefaultObj()} come payload di default.</li>
 * </ul>
 *
 * <h2>Esempio di utilizzo</h2>
 * <pre>{@code
 * SseEmitter emitter = ...;
 * emitter.send(EventType.CREATED.sseEventBuilder());
 * }</pre>
 *
 * <h2>Note</h2>
 * <ul>
 *     <li>L'enum è annotato con {@code @Schema(enumAsRef = true)} per permettere a Swagger di
 *         documentare i valori come riferimento enumerativo.</li>
 *     <li>Gli eventi muteable possono essere arricchiti con payload tramite classi derivate
 *         da {@code PushEvent<T>}.</li>
 * </ul>
 *
 * @author Francesco Cusumano
 */
@Schema(enumAsRef = true)
public enum EventType {

    CONNECTED("Connection opened", false),
    DISCONNECTED("Connection closed", false),
    HEARTBEAT("Connection keep-alive", false),
    MESSAGE(true),
    CREATED(true),
    UPDATED(true),
    DELETED(true),
    ERROR(false);

    private final String defaultObj;
    private final boolean muteable;

    EventType(String defaultObj, boolean muteable) {
        this.defaultObj = defaultObj;
        this.muteable = muteable;
    }

    EventType(boolean muteable) {
        this(null, muteable);
    }

    /** @return messaggio di default dell'evento, o stringa vuota se assente */
    public String getDefaultObj() {
        return StringUtils.hasText(defaultObj) ? defaultObj : "";
    }

    /** @return {@code true} se l'evento può essere escluso dai filtri del client */
    public boolean isMuteable() {
        return muteable;
    }

    /** @return evento SSE con id casuale, nome della costante e messaggio di default come dato */
    public SseEmitter.SseEventBuilder sseEventBuilder() {
        return SseEmitter.event().id(UUID.randomUUID().toString()).name(this.name()).data(this.getDefaultObj());
    }
}
