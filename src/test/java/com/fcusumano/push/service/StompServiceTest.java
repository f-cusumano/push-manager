package com.fcusumano.push.service;

import com.fcusumano.push.CapturingChannel;
import com.fcusumano.push.TestPayloads.UserDto;
import com.fcusumano.push.client.StompClient;
import com.fcusumano.push.client.StompSubscription;
import com.fcusumano.push.controller.StompSubscriptionResponse;
import com.fcusumano.push.event.EventType;
import com.fcusumano.push.event.StompPushEvent;
import com.fcusumano.push.exception.InvalidFilterException;
import com.fcusumano.push.filter.FilteringOptions;
import com.fcusumano.push.filter.FilteringRequest;
import com.fcusumano.push.filter.StreamingFilterRequest;
import com.fcusumano.push.properties.PushProperties;
import com.fcusumano.push.registry.StompRegistry;
import com.fcusumano.push.util.PushMapper;
import com.fcusumano.push.util.StompDestinations;
import jakarta.validation.Validation;
import jakarta.validation.Validator;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.messaging.Message;
import org.springframework.messaging.simp.SimpMessageHeaderAccessor;
import tools.jackson.core.JsonGenerator;
import tools.jackson.databind.SerializationContext;
import tools.jackson.databind.ValueSerializer;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.module.SimpleModule;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static com.fcusumano.push.CapturingChannel.body;
import static com.fcusumano.push.CapturingChannel.eventType;
import static com.fcusumano.push.CapturingChannel.header;
import static com.fcusumano.push.CapturingChannel.json;
import static com.fcusumano.push.TestPayloads.TestChannels.USERS;
import static com.fcusumano.push.TestPayloads.user1;
import static com.fcusumano.push.client.ClientStatus.STREAMING;
import static com.fcusumano.push.util.StompMapper.RETRY_MS_HEADER;

class StompServiceTest {

    private static final JsonMapper MAPPER = JsonMapper.builder().build();
    private static final Validator VALIDATOR = Validation.buildDefaultValidatorFactory().getValidator();
    private static final StompSubscription SUB_0 = StompSubscription.of("SESSION-1", "sub-0");
    private static final StompSubscription SUB_1 = StompSubscription.of("SESSION-1", "sub-1");
    private static final String NAME_ONLY = "{\"requiredFieldsPath\":[\"name\"]}";
    private static final String INVALID_FIELD = "{\"requiredFieldsPath\":[\"address.street\"]}";

    private final CapturingChannel channel = new CapturingChannel();
    private final StompRegistry registry = new StompRegistry(MAPPER, () -> channel);
    private final StompService service = service(registry);

    @BeforeEach
    void connect() {
        service.connect("SESSION-1");
    }

    @Test
    void connectedCarriesSubscriptionResponseAndRetry() {
        service.subscribe(SUB_0, USERS, null, "r-sub-0");
        final Message<?> connected = channel.singleMessage();
        Assertions.assertEquals("CONNECTED", eventType(connected));
        Assertions.assertEquals("2000", header(connected, RETRY_MS_HEADER));
        Assertions.assertEquals("sub-0", SimpMessageHeaderAccessor.getSubscriptionId(connected.getHeaders()));
        Assertions.assertEquals("/push/USERS", SimpMessageHeaderAccessor.getDestination(connected.getHeaders()));
        Assertions.assertEquals(json("{\"subscriptionId\":\"sub-0\",\"channel\":\"USERS\",\"streamedObject\":\"UserDto\"}"), body(connected));
        Assertions.assertEquals(List.of("r-sub-0"), channel.receiptIds());
        Assertions.assertEquals(STREAMING, registry.selectClient(SUB_0).status());

        //Senza receipt richiesto: solo CONNECTED
        service.subscribe(SUB_1, USERS, "", null);
        Assertions.assertEquals(1, channel.messages("sub-1").size());
        Assertions.assertEquals(List.of("r-sub-0"), channel.receiptIds());

        //client-retry-ms a 0: nessun header retry-ms
        service.setProperties(new PushProperties(0, false, 0, 0));
        service.subscribe(StompSubscription.of("SESSION-1", "sub-2"), USERS, null, null);
        Assertions.assertNull(header(channel.messages("sub-2").getFirst(), RETRY_MS_HEADER));
    }

    @Test
    void malformedFiltersHeaderIsRejected() {
        for (String filters : List.of("{\"requiredFieldsPath\":", "[]", "{\"requiredEventTypes\":[\"NOPE\"]}")) {
            channel.clear();
            service.subscribe(SUB_0, USERS, filters, "r-sub-0");
            final Message<?> error = channel.singleMessage();
            Assertions.assertEquals("ERROR", eventType(error));
            Assertions.assertTrue(message(error).startsWith("Filters are not a valid FilteringRequest: "), message(error));
            Assertions.assertEquals("sub-0", body(error).get("subscriptionId").asString());
            Assertions.assertEquals("/push/USERS", body(error).get("destination").asString());
            Assertions.assertTrue(channel.receiptIds().isEmpty());
            Assertions.assertEquals(0, registry.countClients(USERS));
            //La UNSUBSCRIBE che segue è della libreria, una volta sola
            Assertions.assertTrue(service.unsubscribe(SUB_0, "r-unsubscribe"));
            Assertions.assertEquals(List.of("r-unsubscribe"), channel.receiptIds());
            Assertions.assertFalse(service.unsubscribe(SUB_0, null));
        }
    }

    @Test
    void jakartaViolationsAreRejected() {
        final String twoValuesForSingleObject = "{\"contentFilters\":[{\"fieldPath\":\"name\",\"check\":\"FIELD_PRESENT\",\"value\":\"x\"},"
                                                + "{\"fieldPath\":\"tags\",\"check\":\"FIELD_ABSENT\",\"fieldPathValue\":\"name\"}]}";
        service.subscribe(SUB_0, USERS, twoValuesForSingleObject, "r-sub-0");
        final Message<?> error = channel.singleMessage();
        Assertions.assertEquals("ERROR", eventType(error));
        //Violazioni ordinate, nel formato 'percorso: messaggio'
        Assertions.assertEquals("contentFilters[0]: CheckType 'SINGLE_OBJECT' doesn't need a second argument to check.; "
                                + "contentFilters[1]: CheckType 'SINGLE_OBJECT' doesn't need a second argument to check.", message(error));
        Assertions.assertTrue(channel.receiptIds().isEmpty());
        Assertions.assertEquals(0, registry.countClients(USERS));
        Assertions.assertTrue(registry.isKnownSubscription(SUB_0)); //Orfana: la sua UNSUBSCRIBE è della libreria
    }

    @Test
    void missingCheckIsRejected() {
        //Con 'check' assente il validator dei filtri fallisce (HV000028): il client riceve comunque la violazione, come per gli altri filtri
        final String missingCheck = "{\"contentFilters\":[{\"fieldPath\":\"name\"}]}";
        final String missingCheckViolation = "contentFilters[0].check: " + violation("name", "check");
        service.subscribe(SUB_0, USERS, missingCheck, "r-sub-0");
        Assertions.assertEquals("ERROR", eventType(channel.singleMessage()));
        Assertions.assertEquals(missingCheckViolation, message(channel.singleMessage()));
        Assertions.assertTrue(channel.receiptIds().isEmpty());
        Assertions.assertEquals(0, registry.countClients(USERS));
        Assertions.assertTrue(service.unsubscribe(SUB_0, null)); //Rifiutata come gli altri filtri non validi: orfana

        //Anche gli altri campi del filtro vengono validati, violazioni ordinate
        channel.clear();
        service.subscribe(SUB_0, USERS, "{\"contentFilters\":[{\"fieldPath\":\" \"}]}", null);
        Assertions.assertEquals("contentFilters[0].check: " + violation(" ", "check") + "; contentFilters[0].fieldPath: " + violation(" ", "fieldPath"),
                message(channel.singleMessage()));
        Assertions.assertTrue(service.unsubscribe(SUB_0, null));

        service.subscribe(SUB_1, USERS, NAME_ONLY, null);
        final FilteringOptions<Object> previous = registry.selectClient(SUB_1).filteringOptions();
        channel.clear();
        service.updateFilters(SUB_1, missingCheck, "r-1");
        final Message<?> error = channel.singleMessage();
        Assertions.assertEquals("ERROR", eventType(error));
        Assertions.assertEquals(missingCheckViolation, message(error));
        Assertions.assertEquals("/push/USERS", SimpMessageHeaderAccessor.getDestination(error.getHeaders()));
        Assertions.assertTrue(channel.receiptIds().isEmpty());
        Assertions.assertSame(previous, registry.selectClient(SUB_1).filteringOptions());
    }

    @Test
    void invalidFieldPathIsRejectedBeforeRegistration() {
        final String notConvertible = "{\"contentFilters\":[{\"fieldPath\":\"id\",\"check\":\"GREATER_THEN\",\"value\":\"abc\"}]}";
        for (String filters : List.of(INVALID_FIELD, notConvertible)) {
            channel.clear();
            service.subscribe(SUB_0, USERS, filters, "r-sub-0");
            //Stesso testo della risposta 400 di SSE
            Assertions.assertEquals(sseRejection(filters), message(channel.singleMessage()));
            Assertions.assertTrue(channel.receiptIds().isEmpty());
            Assertions.assertEquals(0, registry.countClients(USERS));
            Assertions.assertTrue(service.unsubscribe(SUB_0, null));
        }
    }

    @Test
    void duplicateSubscriptionIdIsRejectedWithoutOrphan() {
        service.subscribe(SUB_0, USERS, null, null);
        channel.clear();
        service.subscribe(SUB_0, USERS, NAME_ONLY, "r-duplicate");
        final Message<?> error = channel.singleMessage();
        Assertions.assertEquals("ERROR", eventType(error));
        Assertions.assertEquals("Subscription id 'sub-0' already in use", message(error));
        Assertions.assertTrue(channel.receiptIds().isEmpty());
        //La sottoscrizione esistente resta valida, con i suoi filtri
        final StompClient<Object> client = registry.selectClient(SUB_0);
        Assertions.assertEquals(STREAMING, client.status());
        Assertions.assertNull(client.filteringOptions());
        Assertions.assertEquals(1, registry.countClients(USERS));
        //Nessun orfano: dopo la UNSUBSCRIBE l'id non è più della libreria
        Assertions.assertTrue(service.unsubscribe(SUB_0, null));
        Assertions.assertFalse(service.unsubscribe(SUB_0, null));
    }

    @Test
    void concurrentDuplicateSubscribeRegistersOneClient() throws Exception {
        //Due SUBSCRIBE con lo stesso id elaborate in parallelo (es. SockJS su HTTP): una registrata, l'altra rifiutata
        final int sessions = 2000;
        try (ExecutorService executor = Executors.newFixedThreadPool(2)) {
            for (int i = 0; i < sessions; i++) {
                final String sessionId = "SESSION-P" + i;
                final StompSubscription subscription = StompSubscription.of(sessionId, "sub-0");
                service.connect(sessionId);
                final CyclicBarrier start = new CyclicBarrier(2);
                final Callable<Void> subscribe = () -> {
                    start.await();
                    service.subscribe(subscription, USERS, null, null);
                    return null;
                };
                final Future<Void> first = executor.submit(subscribe);
                final Future<Void> second = executor.submit(subscribe);
                first.get(5, TimeUnit.SECONDS);
                second.get(5, TimeUnit.SECONDS);
                Assertions.assertEquals(1, registry.countClients(USERS), sessionId);
                //Nessun client resta dopo la chiusura
                service.disconnect(sessionId);
                Assertions.assertEquals(0, registry.countClients(USERS), sessionId);
            }
        }
        //Per ogni sessione un CONNECTED e un ERROR
        Assertions.assertEquals(sessions, channel.messages().stream().filter(m -> "CONNECTED".equals(eventType(m))).count());
        Assertions.assertEquals(sessions, channel.messages().stream()
                .filter(m -> "ERROR".equals(eventType(m)) && "Subscription id 'sub-0' already in use".equals(message(m)))
                .count());
    }

    @Test
    void invalidUpdateKeepsPreviousFilters() {
        service.subscribe(SUB_0, USERS, NAME_ONLY, null);
        final FilteringOptions<Object> previous = registry.selectClient(SUB_0).filteringOptions();
        for (String filters : List.of(INVALID_FIELD, "{")) {
            channel.clear();
            service.updateFilters(SUB_0, filters, "r-1");
            final Message<?> error = channel.singleMessage();
            Assertions.assertEquals("ERROR", eventType(error));
            Assertions.assertEquals("sub-0", SimpMessageHeaderAccessor.getSubscriptionId(error.getHeaders()));
            Assertions.assertEquals("/push/USERS", SimpMessageHeaderAccessor.getDestination(error.getHeaders()));
            Assertions.assertTrue(channel.receiptIds().isEmpty());
            Assertions.assertSame(previous, registry.selectClient(SUB_0).filteringOptions());
        }
        //Stesso testo della risposta 400 di SSE
        channel.clear();
        service.updateFilters(SUB_0, INVALID_FIELD, null);
        Assertions.assertEquals(sseRejection(INVALID_FIELD), message(channel.singleMessage()));
    }

    @Test
    void updateReplacesAllFilters() {
        service.subscribe(SUB_0, USERS, "{\"requiredEventTypes\":[\"CREATED\"],\"requiredFieldsPath\":[\"name\"]}", null);
        service.updateFilters(SUB_0, "{\"contentFilters\":[{\"fieldPath\":\"age\",\"check\":\"GREATER_THEN\",\"value\":18}]}", "r-1");
        Assertions.assertEquals(List.of("r-1"), channel.receiptIds());
        //Come PUT /sse/filter: i campi assenti nella richiesta vengono rimossi
        final FilteringOptions<Object> options = registry.selectClient(SUB_0).filteringOptions();
        Assertions.assertNull(options.allowedEventTypes());
        Assertions.assertNull(options.fieldSelector());
        Assertions.assertNotNull(options.condition());
    }

    @Test
    void emptyUpdateRemovesFilters() {
        service.subscribe(SUB_0, USERS, NAME_ONLY, null);
        service.updateFilters(SUB_0, "{}", "r-1");
        Assertions.assertNull(registry.selectClient(SUB_0).filteringOptions());
        //Body vuoto senza filtri: nulla da cambiare, confermato comunque
        service.updateFilters(SUB_0, "", "r-2");
        Assertions.assertNull(registry.selectClient(SUB_0).filteringOptions());
        service.updateFilters(SUB_0, NAME_ONLY, "r-3");
        Assertions.assertNotNull(registry.selectClient(SUB_0).filteringOptions());
        service.updateFilters(SUB_0, "", "r-4");
        Assertions.assertNull(registry.selectClient(SUB_0).filteringOptions());
        Assertions.assertEquals(List.of("r-1", "r-2", "r-3", "r-4"), channel.receiptIds());
        Assertions.assertEquals(List.of("CONNECTED"), channel.messages().stream().map(CapturingChannel::eventType).toList());
    }

    @Test
    void updateOnUnknownSubscriptionSendsErrorToFilterDestination() {
        service.reject(SUB_1, "/push/NOPE", "No push channel for destination '/push/NOPE'");
        for (StompSubscription unknown : List.of(StompSubscription.of("SESSION-1", "sub-9"), SUB_1)) { //Le orfane non hanno client
            channel.clear();
            service.updateFilters(unknown, NAME_ONLY, "r-1");
            final Message<?> error = channel.singleMessage();
            final String filterDestination = "/push/filter/" + unknown.subscriptionId();
            Assertions.assertEquals("ERROR", eventType(error));
            Assertions.assertEquals(unknown.subscriptionId(), SimpMessageHeaderAccessor.getSubscriptionId(error.getHeaders()));
            Assertions.assertEquals(filterDestination, SimpMessageHeaderAccessor.getDestination(error.getHeaders()));
            Assertions.assertEquals(json("{\"subscriptionId\":\"" + unknown.subscriptionId() + "\",\"destination\":\"" + filterDestination
                                         + "\",\"message\":\"No client with subscription '" + unknown.subscriptionId()
                                         + "' in session 'SESSION-1'\"}"), body(error));
            Assertions.assertTrue(channel.receiptIds().isEmpty());
        }
    }

    @Test
    void foreignUnsubscribeIsNotOwned() {
        Assertions.assertFalse(service.unsubscribe(StompSubscription.of("SESSION-1", "app-sub"), "r-1"));
        Assertions.assertFalse(service.unsubscribe(StompSubscription.of("SESSION-9", "sub-0"), "r-1"));
        Assertions.assertTrue(channel.isEmpty()); //Il RECEIPT spetta all'applicazione o al broker
    }

    @Test
    void subscribeWithoutConnectIsIgnored() {
        final StompSubscription notConnected = StompSubscription.of("SESSION-2", "sub-0");
        service.subscribe(notConnected, USERS, null, "r-sub-0");
        Assertions.assertTrue(channel.isEmpty());
        Assertions.assertEquals(0, registry.countClients(USERS));
        Assertions.assertFalse(registry.isKnownSubscription(notConnected));
    }

    @Test
    void disconnectIsIdempotent() {
        service.subscribe(SUB_0, USERS, null, null);
        service.subscribe(SUB_1, USERS, NAME_ONLY, null);
        service.disconnect("SESSION-1");
        Assertions.assertEquals(0, registry.countClients(USERS));
        service.disconnect("SESSION-1");
        service.disconnect("SESSION-9"); //Mai connessa
        //Dopo la chiusura le sottoscrizioni vengono ignorate
        channel.clear();
        service.subscribe(SUB_0, USERS, null, "r-sub-0");
        Assertions.assertTrue(channel.isEmpty());
        Assertions.assertEquals(0, registry.countClients(USERS));
    }

    @Test
    void unexpectedFailureRejectsTheSubscription() {
        //CONNECTED non serializzabile dal mapper dell'applicazione: il client è già registrato quando l'invio fallisce
        final JsonMapper failingMapper = JsonMapper.builder()
                .addModule(new SimpleModule().addSerializer(StompSubscriptionResponse.class, new FailingSerializer()))
                .build();
        final StompRegistry failingRegistry = new StompRegistry(failingMapper, () -> channel);
        final StompService failingService = service(failingRegistry);
        failingService.connect("SESSION-1");

        failingService.subscribe(SUB_0, USERS, null, "r-sub-0");
        final Message<?> error = channel.singleMessage();
        Assertions.assertEquals("ERROR", eventType(error));
        Assertions.assertEquals("Subscription failed", message(error));
        Assertions.assertTrue(channel.receiptIds().isEmpty());
        //Rifiutata: nessun client registrato, l'id resta della libreria fino alla UNSUBSCRIBE
        Assertions.assertEquals(0, failingRegistry.countClients(USERS));
        Assertions.assertTrue(failingService.unsubscribe(SUB_0, null));
        Assertions.assertFalse(failingService.unsubscribe(SUB_0, null));
        Assertions.assertEquals(0, failingRegistry.broadcast(StompPushEvent.of(user1(), EventType.CREATED, USERS)));
    }

    private static StompService service(StompRegistry registry) {
        final StompService service = new StompService(MAPPER, VALIDATOR, StompDestinations.of("/push"));
        service.setProperties(new PushProperties(0, false, 0, 2000));
        service.setRegistry(registry);
        return service;
    }

    private static String message(Message<?> error) {
        return body(error).get("message").asString();
    }

    //Messaggio della violazione di un campo di StreamingFilterRequest senza 'check', nella lingua del validator
    private static String violation(String fieldPath, String field) {
        return VALIDATOR.validateProperty(new StreamingFilterRequest(fieldPath, null, null, null), field).iterator().next().getMessage();
    }

    //Messaggio con cui SSE rifiuta gli stessi filtri (corpo della risposta 400)
    private static String sseRejection(String filters) {
        try {
            PushMapper.mapToFilteringOptions(MAPPER.readValue(filters, FilteringRequest.class), UserDto.class, MAPPER);
            return Assertions.fail("Filters " + filters + " should be rejected");
        } catch (InvalidFilterException e) {
            return e.getMessage();
        }
    }

    private static final class FailingSerializer extends ValueSerializer<StompSubscriptionResponse> {

        @Override
        public void serialize(StompSubscriptionResponse value, JsonGenerator generator, SerializationContext context) {
            throw new IllegalStateException("Serialization failure");
        }
    }
}
