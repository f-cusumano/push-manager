package com.fcusumano.push.channel;


/**
 * Rappresenta un canale di messaggistica gestito dal sistema di Push Messaging.
 *
 * <p>
 * Questa interfaccia definisce il contratto che ogni enum generata tramite
 * l'annotation processor associato a
 * {@link com.fcusumano.push.annotation.EnablePushMessaging}
 * deve implementare. Ogni costante della enum rappresenta un canale univoco,
 * identificato da un topic e associato a un tipo di payload.
 * </p>
 *
 * <h2>Responsabilità principali</h2>
 * <ul>
 *     <li>Esporre il <b>topic</b> del canale, ovvero l'identificatore univoco
 *         utilizzato all'interno del sistema di messaggistica.</li>
 *     <li>Restituire il <b>tipo del payload</b> accettato o prodotto dal canale.</li>
 * </ul>
 *
 * <h2>Metodo topic()</h2>
 * <p>
 * Restituisce il nome del canale. Nelle enum generate, questo valore
 * corrisponde al nome della costante enum, tipicamente espresso in
 * formato UPPERCASE_SNAKE_CASE, come richiesto dal processor.
 * </p>
 *
 * <h2>Metodo payloadType()</h2>
 * <p>
 * Indica la classe del payload associato al canale. Questo tipo è definito
 * tramite {@link com.fcusumano.push.channel.ChannelDefinition}
 * e incorporato nella enum generata durante la compilazione.
 * </p>
 *
 * <h2>Esempio di implementazione (generata automaticamente)</h2>
 * <pre>{@code
 * public enum MyChannels implements ChannelEnum {
 *     ANGRAFICA_USER(User.class),
 *     CONNECTED_USERS(Integer.class);
 *
 *     private final Class<?> payloadType;
 *
 *     MyChannels(Class<?> payloadType) {
 *         this.payloadType = payloadType;
 *     }
 *
 *     @Override
 *     public String topic() {
 *         return name();
 *     }
 *
 *     @Override
 *     public Class<?> payloadType() {
 *         return payloadType;
 *     }
 * }
 * }</pre>
 *
 * <h2>Note</h2>
 * <ul>
 *     <li>L'interfaccia è progettata per essere semplice e minimale,
 *         facilitando la generazione automatica di enum da parte del processor.</li>
 *     <li>Le implementazioni dovrebbero essere immutabili e thread-safe,
 *         come tipico per le enum Java.</li>
 * </ul>
 *
 * @author Francesco Cusumano
 */
public interface ChannelEnum {

    String topic();
    Class<?> payloadType();
}
