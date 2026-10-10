# push-manager

Libreria Spring Boot per la messaggistica push in tempo reale:
- SSE (Server-Sent Events)
- STOMP (da implementare)

Richiede JDK 25 e Spring Boot 4.x.

## Cosa offre
- L'annotazione `@EnablePushMessaging`, che
  - inizializza nel contesto Spring tutto il necessario per la messaggistica push;
  - genera a compile-time un enum con i canali di trasmissione.
- L'annotazione `@ChannelDefinition` per comporre i canali dentro `@EnablePushMessaging`.
- Il controller astratto `SseController`, da estendere per ereditare gli EP:
  - `POST   /sse/subscribe/{channel}` per sottoscrivere il client a un canale;
  - `PUT    /sse/filter/{session-id}` per modificare i criteri di filtraggio degli eventi;
  - `GET    /sse/stream/{session-id}` per iniziare l'ascolto sul canale;
  - `DELETE /sse/unsubscribe/{session-id}` per interrompere lo streaming e rilasciare il canale.
- Un event manager per pubblicare eventi su un canale.
- Un registry che inoltra gli eventi ai client in ascolto, valutando gli eventuali filtri.
- Filtri sulla trasmissione dei dati per
  - tipologia di evento;
  - predicati su un singolo campo, confronti tra due campi o tra un campo e un valore.

## Utilizzo
1. Aggiungere la dipendenza:
   ```xml
   <dependency>
       <groupId>com.fcusumano.push</groupId>
       <artifactId>push-manager</artifactId>
       <version>${push-manager.version}</version>
   </dependency>
   ```
2. Attivare l'annotation processor nel `maven-compiler-plugin`:
   ```xml
   <plugin>
       <groupId>org.apache.maven.plugins</groupId>
       <artifactId>maven-compiler-plugin</artifactId>
       <configuration>
           <annotationProcessorPaths>
               <path>
                   <groupId>com.fcusumano.push</groupId>
                   <artifactId>push-manager</artifactId>
                   <version>${push-manager.version}</version>
               </path>
           </annotationProcessorPaths>
       </configuration>
   </plugin>
   ```
3. Annotare la classe di avvio dell'applicazione con `@EnablePushMessaging`, definendo almeno un canale:
   ```java
   @EnablePushMessaging(channelCatalog = {
           @ChannelDefinition(topic = "USERS", payloadType = User.class),
           @ChannelDefinition(topic = "CONNECTED_USERS", payloadType = Integer.class)
   })
   @SpringBootApplication
   public class MyApplication {}
   ```
   Alla prima compilazione viene generato in `target/generated-sources/annotations` l'enum `Channels`
   (nome modificabile con `channelEnumName`), nello stesso package della classe annotata.
4. Estendere il controller con l'enum generato:
   ```java
   @RestController
   public class MySseController extends SseController<Channels> {}
   ```
5. Configurare le proprietà nell'`application.yml`:
   ```yaml
   push-manager:
     timeout-ms: 0
     heartbeat-enabled: true
     heartbeat-interval-ms: 1500
     client-retry-ms: 2000
   ```

L'applicazione deve fornire a runtime un container servlet, Jackson e un'implementazione di Jakarta Validation
(es. `spring-boot-starter-web` e `spring-boot-starter-validation`).
