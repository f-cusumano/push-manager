package com.fcusumano.push.channel;


import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;


/**
 * Definisce un canale di messaggistica utilizzato dal sistema di Push Messaging.
 *
 * <p>
 * Questa annotation viene utilizzata all’interno di
 * {@link com.fcusumano.push.annotation.EnablePushMessaging}
 * per dichiarare un canale che sarà poi elaborato dall'annotation processor
 * e trasformato in un valore dell'enum generato.
 * </p>
 *
 * <h2>Componenti principali</h2>
 * <ul>
 *     <li><b>topic</b> – Identificatore univoco del canale, espresso in formato
 *         UPPERCASE_SNAKE_CASE. Il topic viene usato come nome della costante enum
 *         generata e come riferimento logico del canale.</li>
 *     <li><b>payloadType</b> – Tipo della classe che rappresenta il payload del messaggio
 *         associato al canale. Questo tipo viene memorizzato nella enum generata dal
 *         processor.</li>
 * </ul>
 *
 * <h2>Vincoli</h2>
 * <ul>
 *     <li><b>topic</b> non può essere vuoto (annotato con {@link jakarta.validation.constraints.NotBlank}).</li>
 *     <li><b>payloadType</b> non può essere null (annotato con {@link jakarta.validation.constraints.NotNull}).</li>
 * </ul>
 *
 * <h2>Utilizzo</h2>
 * <pre>{@code
 * @EnablePushMessaging(
 *     channelEnumName = "MyChannels",
 *     channelCatalog = {
 *         @ChannelDefinition(topic = "ANGRAFICA_USER", payloadType = User.class),
 *         @ChannelDefinition(topic = "CONNECTED_USERS", payloadType = Integer.class)
 *     }
 * )
 * @SpringBootApplication
 * public class MyApplication {
 * }
 * }</pre>
 *
 * <h2>Note</h2>
 * <ul>
 *     <li>L'annotation è mantenuta solo a livello di sorgente
 *         ({@link java.lang.annotation.RetentionPolicy#SOURCE}) poiché viene
 *         consumata esclusivamente dall'annotation processor.</li>
 *     <li>Il valore di {@code topic} deve rispettare il pattern validato dal processor:
 *         <code>^[A-Z][A-Z0-9_]*$</code>.</li>
 * </ul>
 *
 * @author Francesco Cusumano
 */
@Retention(RetentionPolicy.SOURCE)
public @interface ChannelDefinition {

    @NotBlank String topic();
    @NotNull Class<?> payloadType();
}
