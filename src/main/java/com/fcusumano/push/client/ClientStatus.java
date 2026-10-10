package com.fcusumano.push.client;


/**
 * Rappresenta lo stato di un client nel sistema di gestione delle connessioni
 * o delle sottoscrizioni push.
 *
 * <p>
 * Gli stati descrivono il livello di interazione attivo tra il client e
 * l'infrastruttura di messaggistica, permettendo al sistema di identificarne
 * il comportamento corrente.
 * </p>
 *
 * <h2>Stati disponibili</h2>
 * <ul>
 *     <li><b>SUBSCRIBED</b> – Il client è registrato, ma non abilitato a ricevere messaggi.</li>
 *     <li><b>STREAMING</b> – Il client ha una connessione attiva in streaming
 *         (ad esempio SSE, WebSocket o simili).</li>
 *     <li><b>DISABLED</b> – Il client è registrato ma temporaneamente disabilitato
 *         o non più autorizzato a ricevere messaggi.</li>
 * </ul>
 *
 * @author Francesco Cusumano
 */
public enum ClientStatus {

    SUBSCRIBED,
    STREAMING,
    DISABLED
}
