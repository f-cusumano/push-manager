# push-manager

Libreria Spring Boot per la messaggistica push in tempo reale verso i client web.

- **SSE** (Server-Sent Events): disponibile.
- **WebSocket/STOMP**: predisposto nelle astrazioni, da implementare.

Con un'annotazione sulla classe di avvio e un controller vuoto, l'applicazione espone gli endpoint per
sottoscriversi a un canale, ricevere gli eventi in streaming, filtrarli per tipo o per contenuto e
ricevere solo i campi di interesse.
L'applicazione pubblica gli eventi con una sola chiamata, la libreria li consegna ai client interessati.

## Indice
- [Requisiti](#requisiti)
- [Installazione](#installazione)
- [Avvio rapido](#avvio-rapido)
- [Configurazione](#configurazione)
- [Canali](#canali)
- [Pubblicare eventi](#pubblicare-eventi)
- [Endpoint REST](#endpoint-rest)
- [Formato degli eventi SSE](#formato-degli-eventi-sse)
- [Filtri](#filtri)
- [Esempio di client JavaScript](#esempio-di-client-javascript)
- [Ciclo di vita di un client](#ciclo-di-vita-di-un-client)
- [Architettura](#architettura)
- [Limitazioni note](#limitazioni-note)
- [Build](#build)

## Requisiti
- JDK 25
- Spring Boot 4.x con Spring MVC (stack servlet; WebFlux non è supportato)
- A runtime l'applicazione deve fornire:
  - un container servlet e Jackson, ad esempio con `spring-boot-starter-web`;
  - un'implementazione di Jakarta Validation, ad esempio con `spring-boot-starter-validation`;
  - un `PlatformTransactionManager` (presente con qualsiasi starter di accesso ai dati), perché gli eventi
    vengono consegnati dopo il commit della transazione (vedi [Pubblicare eventi](#pubblicare-eventi)).
- Opzionale: springdoc-openapi per vedere gli endpoint su Swagger UI. Le annotazioni OpenAPI sono già presenti.

## Installazione
Aggiungere la dipendenza:
```xml
<dependency>
    <groupId>com.fcusumano.push</groupId>
    <artifactId>push-manager</artifactId>
    <version>${push-manager.version}</version>
</dependency>
```

Attivare l'annotation processor, che genera l'enum dei canali in fase di compilazione:
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
Se il progetto usa già altri processor (es. Lombok, MapStruct), aggiungere questo `<path>` accanto agli altri.

## Avvio rapido
1. Annotare la classe di avvio con `@EnablePushMessaging` e dichiarare almeno un canale:
   ```java
   @EnablePushMessaging(channelCatalog = {
           @ChannelDefinition(topic = "USERS", payloadType = UserDto.class),
           @ChannelDefinition(topic = "CONNECTED_USERS", payloadType = Integer.class)
   })
   @SpringBootApplication
   public class MyApplication {

       public static void main(String[] args) {
           SpringApplication.run(MyApplication.class, args);
       }
   }
   ```
2. Compilare: in `target/generated-sources/annotations` compare l'enum `Channels`, nello stesso package della
   classe annotata.
3. Estendere il controller astratto con l'enum generato. Il corpo resta vuoto:
   ```java
   @RestController
   @RequestMapping("/api")
   public class PushEndpoint extends SseController<Channels> {}
   ```
4. Configurare le proprietà (vedi [Configurazione](#configurazione)).
5. Pubblicare gli eventi dai servizi applicativi:
   ```java
   @Service
   public class UserService {

       private final SseEventManager sseEventManager;

       public UserService(SseEventManager sseEventManager) {
           this.sseEventManager = sseEventManager;
       }

       @Transactional
       public UserDto create(UserDto user) {
           //...salvataggio
           sseEventManager.publishEvent(SsePushEvent.of(user, EventType.CREATED, Channels.USERS));
           return user;
       }
   }
   ```

## Configurazione
Le proprietà hanno prefisso `push-manager`:

| Proprietà | Tipo | Valore consigliato | Descrizione |
|---|---|---|---|
| `push-manager.timeout-ms` | long | `0` | Timeout dell'`SseEmitter` in millisecondi. `0` = nessun timeout. |
| `push-manager.heartbeat-enabled` | boolean | `true` | Invia periodicamente l'evento `HEARTBEAT` ai client in streaming, per tenere viva la connessione dietro proxy e load balancer. |
| `push-manager.heartbeat-interval-ms` | long | `1500` | Intervallo tra due heartbeat. Se non è positivo l'heartbeat non parte. |
| `push-manager.client-retry-ms` | long | `2000` | Tempo di riconnessione suggerito al browser, inviato con l'evento `CONNECTED`. |

```yaml
push-manager:
  timeout-ms: 0
  heartbeat-enabled: true
  heartbeat-interval-ms: 1500
  client-retry-ms: 2000
```

Una proprietà assente vale `0` o `false`: senza configurazione l'heartbeat resta spento.

### Bean registrati
`@EnablePushMessaging` importa `PushAutoConfig`, che registra:

| Bean | Tipo | Ruolo |
|---|---|---|
| `sseRegistry` | `SseRegistry` | Registro thread-safe dei client per canale. |
| `sseService` | `SseService` | Logica di sottoscrizione, filtro, streaming e disiscrizione. |
| `sseEventManager` | `SseEventManager` | Punto d'ingresso per pubblicare eventi. |
| `taskScheduler` | `ThreadPoolTaskScheduler` | Scheduler degli heartbeat (2 thread, prefisso `push-scheduler-`). |
| `pushTransactionalEventListenerFactory` | `TransactionalEventListenerFactory` | Attiva i `@TransactionalEventListener` anche senza `@EnableTransactionManagement`. |

`SseRegistry` e `SseService` usano il `JsonMapper` dell'applicazione (Jackson 3), se presente, per convertire i valori
dei filtri e selezionare i campi dei payload con la stessa configurazione usata per serializzarli.

`PushAutoConfig` abilita anche `@EnableAsync`. Il bean `taskScheduler` prende il posto di quello che Spring Boot
crea di default con lo stesso nome; se l'applicazione ne definisce già uno, va tenuto conto del conflitto di nomi.

La libreria non si registra in `AutoConfiguration.imports`: si attiva solo con `@EnablePushMessaging`.

## Canali
Un canale è un flusso di eventi con un payload di tipo fisso. Si dichiara con `@ChannelDefinition`:

| Attributo | Descrizione |
|---|---|
| `topic` | Nome del canale e della costante enum generata. Deve rispettare `^[A-Z][A-Z0-9_]*$`, altrimenti la compilazione fallisce. |
| `payloadType` | Classe esatta dei payload ammessi. Le sottoclassi vengono rifiutate. |

`@EnablePushMessaging` accetta:

| Attributo | Default | Descrizione |
|---|---|---|
| `channelCatalog` | `{}` | Elenco dei canali. |
| `channelEnumName` | `"Channels"` | Nome dell'enum generato. |

L'enum generato implementa `ChannelEnum` ed è annotato con `@Schema(enumAsRef = true)`, così Swagger mostra
i canali ammessi nel path dell'endpoint di sottoscrizione:
```java
@Schema(enumAsRef = true)
public enum Channels implements ChannelEnum {
    USERS(UserDto.class),
    CONNECTED_USERS(Integer.class);
    //topic() restituisce name(), payloadType() la classe dichiarata
}
```

## Pubblicare eventi
Gli eventi si creano con `SsePushEvent` e si pubblicano con `SseEventManager`:
```java
sseEventManager.publishEvent(SsePushEvent.of(user, EventType.UPDATED, Channels.USERS));

//Con tempo di riconnessione suggerito al client
sseEventManager.publishEvent(SsePushEvent.of(user, EventType.UPDATED, Channels.USERS, 5000L));
```

- Il costruttore verifica che la classe del payload sia esattamente quella del canale, altrimenti lancia
  `IllegalArgumentException`.
- **La pubblicazione deve avvenire dentro una transazione** (metodo `@Transactional`). L'evento viene
  consegnato da un `@TransactionalEventListener(phase = AFTER_COMMIT)`: senza transazione attiva Spring lo scarta,
  se la transazione va in rollback non parte.
- La consegna è asincrona (`@Async`): `publishEvent` torna subito e l'invio avviene su un altro thread.
  Eventi pubblicati a breve distanza possono quindi arrivare in ordine diverso da quello di pubblicazione.
- L'evento viene inviato a tutti i client in streaming sul canale che non lo escludono con i propri filtri.
- Per i client che selezionano dei campi il payload viene convertito in JSON una volta sola per evento, anche se
  i campi scelti sono diversi.

### Tipi di evento
| Tipo | Filtrabile | Uso |
|---|---|---|
| `CONNECTED` | no | Inviato dalla libreria all'apertura dello stream, con dato `"Connection opened"`. |
| `DISCONNECTED` | no | Inviato dalla libreria alla disiscrizione, con dato `"Connection closed"`. |
| `HEARTBEAT` | no | Inviato dalla libreria periodicamente, con dato `"Connection keep-alive"`. |
| `MESSAGE` | sì | Messaggio applicativo generico. |
| `CREATED` | sì | Entità creata. |
| `UPDATED` | sì | Entità modificata. |
| `DELETED` | sì | Entità eliminata. |
| `ERROR` | no | Errore lato server. |

I tipi non filtrabili arrivano sempre, anche se il client ha chiesto solo altri tipi.

## Endpoint REST
Il controller che estende `SseController<Channels>` eredita quattro endpoint, relativi al suo `@RequestMapping`:

| Metodo | Path | Body | Risposta |
|---|---|---|---|
| `POST` | `/sse/subscribe/{channel}` | `FilteringRequest` opzionale | `SseSubscriptionResponse` (JSON) |
| `PUT` | `/sse/filter/{session-id}` | `FilteringRequest` opzionale | vuota |
| `GET` | `/sse/stream/{session-id}` | – | stream `text/event-stream` |
| `DELETE` | `/sse/unsubscribe/{session-id}` | – | vuota |

Errori:
- `400` se la `FilteringRequest` non è valida o non è applicabile al payload del canale: campo inesistente,
  check non definito per il tipo del campo, valore non convertibile nel tipo del campo (`InvalidFilterException`);
- `404` se il `session-id` non corrisponde a nessun client (`NoSuchClientException`).

### Sottoscrizione
Registra il client sul canale e restituisce il `sessionId` da usare negli altri endpoint. Lo stream non parte
ancora: il client è in stato `SUBSCRIBED`. Se i filtri non sono validi il client non viene registrato.
```http
POST /api/sse/subscribe/USERS
Content-Type: application/json

{
  "requiredEventTypes": ["CREATED", "UPDATED"],
  "contentFilters": [
    { "fieldPath": "address.city", "check": "SAME_VALUE", "value": "Roma" }
  ],
  "requiredFieldsPath": ["id", "name", "address"]
}
```
```json
{
  "sessionId": "0b9f5c6e-6a3e-4c9e-9a51-3f3f0f6c1d2a",
  "channel": "USERS",
  "streamedObject": "UserDto"
}
```
`streamedObject` è il nome semplice della classe dei payload del canale.

### Modifica dei filtri
Sostituisce i filtri della sessione. Un body assente o senza filtri li rimuove. Si può chiamare anche durante lo
streaming: i nuovi filtri valgono dal primo evento successivo.

### Streaming
Apre lo stream SSE e invia subito l'evento `CONNECTED`. Se la sessione è già in streaming restituisce lo stesso
emitter senza riconfigurarlo. La risposta include gli header `X-Accel-Buffering: no` e `Cache-Control: no-cache`,
utili dietro nginx e altri proxy.

### Disiscrizione
Invia l'evento `DISCONNECTED`, chiude lo stream e rimuove il client dal registro. Funziona anche per i client che
non hanno mai aperto lo stream.

## Formato degli eventi SSE
Ogni evento applicativo arriva così:
```text
id:3c1e0b7a-...        (messageId dell'evento)
event:CREATED          (EventType)
retry:5000             (solo se l'evento ha retryMs)
data:{"id":42,"name":"Mario","address":{"city":"Roma"}}
```
Il dato è il payload serializzato in JSON dai message converter di Spring MVC, oppure solo i campi selezionati
dal client (vedi [Selezione dei campi](#selezione-dei-campi)). Gli eventi di sistema
(`CONNECTED`, `HEARTBEAT`, `DISCONNECTED`) hanno come dato il testo descritto in [Tipi di evento](#tipi-di-evento).

## Filtri
Ogni client può limitare gli eventi che riceve con una `FilteringRequest`, alla sottoscrizione o in seguito:

| Campo | Descrizione |
|---|---|
| `requiredEventTypes` | Tipi di evento da ricevere. Vuoto o assente = tutti. I tipi non filtrabili arrivano comunque. |
| `contentFilters` | Condizioni sul payload, tutte in AND. Vuoto o assente = nessun filtro. |
| `requiredFieldsPath` | Campi del payload da ricevere. Vuoto o assente = payload intero. |

Una `PUT /sse/filter/{session-id}` sostituisce tutti e tre i campi: quelli non inviati tornano al valore di default.

### Filtri sul contenuto
Ogni elemento di `contentFilters` è una `StreamingFilterRequest`:

| Campo | Obbligatorio | Descrizione |
|---|---|---|
| `fieldPath` | sì | Campo del payload da controllare. Per i campi annidati si usa il punto: `address.city`. Si legge il campo, anche privato o ereditato, non il getter. |
| `check` | sì | Tipo di controllo (vedi tabella sotto). |
| `value` | dipende | Valore costante con cui confrontare il campo. |
| `fieldPathValue` | dipende | Altro campo del payload con cui confrontare il primo. |

Per i check su un solo termine `value` e `fieldPathValue` devono essere assenti; per gli altri deve essercene
esattamente uno. Una richiesta che non rispetta queste regole viene rifiutata con `400`.

| Check | Termini | Tipi supportati |
|---|---|---|
| `FIELD_PRESENT` | 1 | qualsiasi: il campo non è null |
| `FIELD_ABSENT` | 1 | qualsiasi: il campo è null |
| `IS_IN_THE_PAST` | 1 | `OffsetDateTime`, `LocalDateTime`, `Instant` |
| `IS_IN_THE_FUTURE` | 1 | `OffsetDateTime`, `LocalDateTime`, `Instant` |
| `SAME_VALUE` | 2, stesso tipo | qualsiasi, confronto con `equals` |
| `LESS_THEN` | 2, stesso tipo | `Integer`, `Long`, `Double`, `OffsetDateTime`, `LocalDateTime`, `Instant` |
| `LESS_THEN_OR_EQUAL_TO` | 2, stesso tipo | come sopra |
| `GREATER_THEN` | 2, stesso tipo | come sopra |
| `GREATER_THEN_OR_EQUAL_TO` | 2, stesso tipo | come sopra |
| `STARTS_WITH`, `ENDS_WITH`, `CONTAINS` | 2, stesso tipo | `String` |
| `STARTS_WITH_IGNORE_CASE`, `ENDS_WITH_IGNORE_CASE`, `CONTAINS_IGNORE_CASE` | 2, stesso tipo | `String` |
| `EXACT_LENGTH`, `LENGTH_LESS_THAN`, `LENGTH_MORE_THAN` | 2, tipi diversi | campo `String`, valore `Integer` |

Regole sui valori null: un campo null soddisfa solo `FIELD_ABSENT` e, se anche il secondo termine è null,
`SAME_VALUE`; in tutti gli altri casi il check fallisce. Se un campo intermedio del percorso è null, il valore
letto è null.

Il `value` viene convertito una sola volta, quando arriva il filtro, nel tipo del campo: un numero JSON vale per campi
`Integer`, `Long` o `Double`, una stringa ISO-8601 per i campi data (es. `"2030-01-01T00:00:00Z"`). Nei confronti tra
due campi (`fieldPathValue`) i tipi devono essere compatibili. Le incoerenze vengono rifiutate subito con `400`, così
durante lo streaming i filtri non falliscono.

I filtri vengono tradotti in predicati una sola volta, quando arrivano; per ogni evento si esegue solo il predicato.

Esempi:
```json
{ "fieldPath": "name", "check": "STARTS_WITH_IGNORE_CASE", "value": "mar" }
{ "fieldPath": "age", "check": "GREATER_THEN_OR_EQUAL_TO", "value": 18 }
{ "fieldPath": "deletedAt", "check": "FIELD_ABSENT" }
{ "fieldPath": "updatedAt", "check": "GREATER_THEN", "fieldPathValue": "createdAt" }
{ "fieldPath": "code", "check": "EXACT_LENGTH", "value": 6 }
{ "fieldPath": "createdAt", "check": "GREATER_THEN", "value": "2026-01-01T00:00:00Z" }
```

### Selezione dei campi
Con `requiredFieldsPath` il client riceve solo i campi indicati invece del payload intero:

- i percorsi usano i nomi dei campi Java e il punto per i campi annidati (`address.city`);
- un campo selezionato porta con sé tutti i suoi sottocampi: `address` include `address.city` e `address.zip`;
- un percorso che attraversa una collezione o un array si applica a ogni elemento (`orders.amount`);
- i campi selezionati ma null arrivano come `null`;
- **se tutti i campi selezionati sono vuoti l'evento non viene inviato** a quel client. Vuoto significa null,
  collezione vuota o con soli elementi vuoti, oggetto con soli campi vuoti; `0`, `false` e `""` sono dati.

Con il payload `{"id":7,"name":"Mario","address":{"city":"Roma","zip":"00100"},"orders":[{"code":"A1","amount":10.5}]}`:

| `requiredFieldsPath` | Dato inviato |
|---|---|
| `["name", "address.city"]` | `{"name":"Mario","address":{"city":"Roma"}}` |
| `["address"]` | `{"address":{"city":"Roma","zip":"00100"}}` |
| `["orders.amount"]` | `{"orders":[{"amount":10.5}]}` |
| `["surname"]` | rifiutato con `400`: campo inesistente |

Se per lo stesso payload `name` e `address` fossero null, `["name", "address.city"]` farebbe scartare l'evento.

I percorsi vengono verificati sui campi della classe del payload alla sottoscrizione; dentro una `Map` o un tipo generico
non risolvibile i nomi non sono verificabili e vengono accettati.

## Esempio di client JavaScript
```js
const base = '/api/sse';

const { sessionId } = await fetch(`${base}/subscribe/USERS`, {
  method: 'POST',
  headers: { 'Content-Type': 'application/json' },
  body: JSON.stringify({ requiredEventTypes: ['CREATED', 'UPDATED'], requiredFieldsPath: ['id', 'name'] })
}).then(r => r.json());

const source = new EventSource(`${base}/stream/${sessionId}`);
source.addEventListener('CONNECTED', e => console.log(e.data));
source.addEventListener('CREATED', e => console.log('Nuovo utente', JSON.parse(e.data)));
source.addEventListener('UPDATED', e => console.log('Utente modificato', JSON.parse(e.data)));

//Modifica dei filtri a stream aperto
await fetch(`${base}/filter/${sessionId}`, {
  method: 'PUT',
  headers: { 'Content-Type': 'application/json' },
  body: JSON.stringify({ contentFilters: [{ fieldPath: 'address.city', check: 'SAME_VALUE', value: 'Roma' }] })
});

//Chiusura
window.addEventListener('beforeunload', () => {
  source.close();
  fetch(`${base}/unsubscribe/${sessionId}`, { method: 'DELETE', keepalive: true });
});
```
Gli eventi vanno ascoltati per nome con `addEventListener`: `onmessage` riceve solo gli eventi senza nome.

## Ciclo di vita di un client
```text
POST subscribe ──> SUBSCRIBED ──GET stream──> STREAMING ──> DISABLED (rimosso dal registro)
                                                  │
                         heartbeat periodico ─────┤
                         PUT filter (anche qui) ──┘
```
Il client passa a `DISABLED` e viene rimosso quando lo stream si chiude per qualsiasi motivo: disiscrizione,
timeout, errore di rete o chiusura del browser (rilevata al primo invio fallito, tipicamente l'heartbeat).
In quel momento l'heartbeat viene cancellato.

## Architettura
| Package | Contenuto |
|---|---|
| `annotation` | `@EnablePushMessaging` e l'annotation processor che genera l'enum dei canali. |
| `channel` | `@ChannelDefinition` e l'interfaccia `ChannelEnum`. |
| `client` | `SseClient` e il suo stato `ClientStatus`. |
| `controller` | `PushController` e `SseController`, da estendere. |
| `event` | `PushEvent`/`SsePushEvent`, `EventType`, `EventManager`/`SseEventManager`. |
| `exception` | `InvalidFilterException` (400) e `NoSuchClientException` (404). |
| `filter` | Richieste di filtraggio, `CheckType`, selezione dei campi (`FieldSelector`), validazione. |
| `properties` | `PushProperties`. |
| `registry` | `EventRegistry` e `SseRegistry`. |
| `service` | `PushService` e `SseService`. |

Ogni ruolo ha una base astratta generica (`PushController`, `PushService`, `EventManager`, `EventRegistry`,
`PushEvent`) e un'implementazione SSE. Un futuro protocollo, come STOMP, aggiunge le proprie implementazioni
senza cambiare le basi.

Il flusso di un evento:
```text
servizio @Transactional
  └─ SseEventManager.publishEvent(event)
       └─ ApplicationEventPublisher
            └─ (commit) EventManager.onEvent  @Async
                 └─ SseEventManager.handle
                      └─ SseRegistry.broadcast ── per ogni client in STREAMING sul canale:
                                                   filtro tipo evento → filtro contenuto →
                                                   selezione dei campi → SseEmitter.send
```

## Limitazioni note
- **Sottoscrizioni mai aperte**: un client che si sottoscrive ma non chiama mai `GET stream` né `DELETE unsubscribe`
  resta nel registro.
- **Proprietà JSON rinominate**: i percorsi si verificano sui nomi dei campi Java, mentre la selezione si applica
  al JSON del payload. Un campo il cui nome JSON è diverso (`@JsonProperty`, naming strategy) non si può selezionare:
  arriva sempre come `null`.
- **Un solo canale per sessione**: per seguire più canali il client apre più sottoscrizioni.
- **Istanza singola**: il registro dei client è in memoria. Con più istanze dell'applicazione ogni evento arriva
  solo ai client collegati all'istanza che lo pubblica.
- **STOMP/WebSocket**: non ancora implementato.

## Build
```bash
./mvnw clean verify
```
Esegue i test (unitari e un flusso SSE completo su server reale) e produce il jar della libreria, i sorgenti e la
javadoc in `target/`.
