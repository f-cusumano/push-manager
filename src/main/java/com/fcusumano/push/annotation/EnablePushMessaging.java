package com.fcusumano.push.annotation;

import com.fcusumano.push.PushAutoConfig;
import com.fcusumano.push.channel.ChannelDefinition;
import org.springframework.context.annotation.Import;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;


/**
 * Abilita la funzionalità di Push Messaging all'interno dell'applicazione Spring.
 *
 * <p>
 * Questa annotation importa automaticamente la configurazione {@link PushAutoConfig}
 * e permette di definire un catalogo di canali di messaggistica, così come il nome
 * della enum che li rappresenta. Deve essere applicata a livello di type
 * (classi di configurazione o classi applicative).
 * </p>
 *
 * <h2>Funzionalità principali</h2>
 * <ul>
 *     <li>Attiva l'infrastruttura necessaria per il sistema di push messaging.</li>
 *     <li>Consente la configurazione personalizzata dei canali tramite
 *         {@link ChannelDefinition}.</li>
 *     <li>Permette di specificare il nome della enum che contiene l’elenco dei canali.</li>
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
 * <h2>Parametri</h2>
 * <ul>
 *     <li><b>channelEnumName</b> – Nome della enum che rappresenta i canali.
 *         Deve corrispondere a una enum presente nel classpath.
 *         <br>Default: <code>"Channels"</code>.
 *     </li>
 *
 *     <li><b>channelCatalog</b> – Elenco dei canali disponibili,
 *         definiti tramite {@link ChannelDefinition}. Usato per inizializzare o
 *         sovrascrivere il catalogo dei canali.
 *         <br>Default: array vuoto.
 *     </li>
 *
 *     <li><b>stomp</b> – Abilita anche la consegna via STOMP su WebSocket.
 *         <br>Default: <code>false</code>.
 *     </li>
 * </ul>
 *
 * <h2>Note</h2>
 * <ul>
 *     <li>L’annotation è mantenuta a runtime per consentire a Spring
 *         di leggerla tramite reflection.</li>
 *     <li>L’importazione di {@link PushAutoConfig} avviene automaticamente
 *         tramite {@link org.springframework.context.annotation.Import}.</li>
 * </ul>
 *
 * @author Francesco Cusumano
 * @see PushAutoConfig
 * @see ChannelDefinition
 */
@Target(ElementType.TYPE)
@Import({PushAutoConfig.class, StompImportSelector.class})
@Retention(RetentionPolicy.RUNTIME)
public @interface EnablePushMessaging {

    /**
     * Nome semplice dell'enum dei canali generato dall'annotation processor
     * nello stesso package della classe annotata.
     */
    String channelEnumName() default "Channels";
    /** Canali di trasmissione: ognuno diventa una costante dell'enum generato. */
    ChannelDefinition[] channelCatalog() default {};
    /** Abilita anche la consegna via STOMP su WebSocket: richiede spring-boot-starter-websocket e un controller che estende StompController. */
    boolean stomp() default false;
}
