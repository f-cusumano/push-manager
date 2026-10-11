package com.fcusumano.push.util;

import com.fcusumano.push.event.EventType;
import com.fcusumano.push.event.SsePushEvent;
import com.fcusumano.push.exception.InvalidFilterException;
import com.fcusumano.push.filter.CheckType;
import com.fcusumano.push.filter.FieldSelector;
import com.fcusumano.push.filter.FilteringOptions;
import com.fcusumano.push.filter.FilteringRequest;
import com.fcusumano.push.filter.StreamingFilterRequest;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import org.springframework.util.ClassUtils;
import org.springframework.util.CollectionUtils;
import org.springframework.util.ReflectionUtils;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter.SseEventBuilder;
import tools.jackson.databind.json.JsonMapper;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.function.Predicate;

import static com.fcusumano.push.filter.FilteringRequest.nullEquivalent;
import static com.fcusumano.push.filter.TargetDefinition.SINGLE_OBJECT;
import static com.fcusumano.push.filter.TargetDefinition.TWO_OBJ_SAME_TYPE;
import static com.fcusumano.push.util.Constant.NESTED_PROPERTY_SEPARATOR;
import static org.springframework.util.StringUtils.hasText;


/**
 * Classe statica che raccoglie metodi di supporto per i mapping non lineari tra tipi.
 */
public final class PushMapper {

    private PushMapper() {
    }

    /**
     * Mappa un {@link SsePushEvent} in un nuovo {@link SseEventBuilder} con il dato indicato,
     * che può essere il payload dell'evento o una sua selezione di campi.
     * Il tempo di riconnessione viene inviato solo se valorizzato nell'evento,
     * così da non sovrascrivere quello comunicato al client con l'evento {@code CONNECTED}.
     *
     * @param event <i>(obbligatorio)</i> un evento SSE
     * @param data <i>(opzionale)</i> il dato da trasmettere
     * @return l'evento SSE da inviare
     */
    public static SseEventBuilder toEventBuilder(@NotNull SsePushEvent<?> event, Object data) {
        final SseEventBuilder builder = SseEmitter.event()
                .name(event.getEventType().name())
                .id(event.getMessageId());
        if (event.getRetryMs() != null) builder.reconnectTime(event.getRetryMs());
        return builder.data(data == null ? "" : data);
    }

    /**
     * Mappa un {@link FilteringRequest} in un nuovo {@link FilteringOptions} specificando il tipo
     * dell'oggetto interessato dal criterio di filtraggio; il mapping si avvale di metodi specifici
     * opportunamente valorizzati per elaborare il mapping dei singoli attributi.
     *
     * @param inputFilters <i>(opzionale)</i> la richiesta di filtri da applicare allo streaming di dati
     * @param type         <i>(obbligatorio)</i> il tipo dell'oggetto interessato
     * @param jsonMapper   <i>(obbligatorio)</i> il mapper usato per convertire i valori dei filtri nel tipo dei campi
     * @param <T>          la specifica del tipo dell'oggetto interessato
     * @return {@code null} se l'input è {@code null}, l'oggetto mappato altrimenti
     * @throws InvalidFilterException se un filtro o un campo selezionato non è applicabile al tipo
     */
    public static <T> FilteringOptions<T> mapToFilteringOptions(FilteringRequest inputFilters, @NotNull Class<T> type,
                                                                @NotNull JsonMapper jsonMapper) {
        return nullEquivalent(inputFilters) ? null : FilteringOptions.of(
                filterMuteable(inputFilters.requiredEventTypes()),
                mapToPredicates(inputFilters.contentFilters(), type, jsonMapper),
                FieldSelector.of(type, inputFilters.requiredFieldsPath())
        );
    }

    /**
     * Filtra la lista di {@link EventType} richiesti mantenendo solo quelli muteable.
     * Se la lista risultante è vuota, questo equivale a consentire solo gli eventi non muteable.
     *
     * @param original <i>(opzionale)</i> la lista di {@link EventType} richiesti
     * @return {@code null} se l'input è {@code null} o vuoto, la lista filtrata di {@link EventType} altrimenti
     */
    public static List<EventType> filterMuteable(List<EventType> original) {
        return CollectionUtils.isEmpty(original) ? null : original.stream().filter(EventType::isMuteable).toList();
    }

    /**
     * Mappa una {@link StreamingFilterRequest} in un nuovo {@link Predicate} tentando di costruire la regola
     * richiesta in input sul tipo di oggetto specificato. Il predicato risultante:
     * <ul>
     *     <li>accetterà in input un oggetto del tipo specificato</li>
     *     <li>ne ricaverà il/i valore/i del/i campo/i specificato/i</li>
     *     <li>costituirà un test di idoneità per l'input</li>
     * </ul>
     * La coerenza tra check, tipi dei campi e valore viene verificata qui, una volta sola:
     * il valore in input viene convertito nel tipo atteso dal check, così il predicato non fallisce
     * durante lo streaming.
     *
     * @param inputFilter <i>(obbligatorio)</i> la singola regola di filtraggio da applicare allo streaming di dati
     * @param type <i>(obbligatorio)</i> il tipo dell'oggetto interessato
     * @param jsonMapper <i>(obbligatorio)</i> il mapper usato per convertire il valore nel tipo del campo
     * @param <T> la specifica del tipo dell'oggetto interessato
     * @return il predicato per testare se un dato payload è di interesse per un dato client
     * @throws InvalidFilterException se il filtro non è applicabile al tipo
     */
    public static <T> Predicate<T> mapToPredicate(@NotNull StreamingFilterRequest inputFilter, @NotNull Class<T> type,
                                                  @NotNull JsonMapper jsonMapper) {
        final CheckType checkType = inputFilter.check();
        final FieldAccessor<T> arg1 = mapFieldAccessor(type, inputFilter.fieldPath());
        if (!checkType.supports(arg1.type())) {
            throw new InvalidFilterException("CheckType '%s' is not supported by field '%s' of type %s",
                    checkType, inputFilter.fieldPath(), arg1.type().getSimpleName());
        }
        if (checkType.getTargetDefinition() == SINGLE_OBJECT) {
            //Dato un oggetto 'o' si estrae il valore del campo richiesto e si testa
            return o -> checkType.testPredicate(arg1.getter().apply(o));
        }
        final List<Class<?>> expectedTypes2 = checkType.getTargetDefinition() == TWO_OBJ_SAME_TYPE
                ? List.of(arg1.type())
                : checkType.getPossibleTypes2(arg1.type());

        //Il secondo argomento può essere un altro campo target..
        if (hasText(inputFilter.fieldPathValue())) {
            final FieldAccessor<T> arg2 = mapFieldAccessor(type, inputFilter.fieldPathValue());
            if (expectedTypes2.stream().noneMatch(t -> compatible(t, arg2.type()))) {
                throw new InvalidFilterException("Field '%s' of type %s cannot be compared with field '%s' of type %s by CheckType '%s'",
                        inputFilter.fieldPathValue(), arg2.type().getSimpleName(),
                        inputFilter.fieldPath(), arg1.type().getSimpleName(), checkType);
            }
            return o -> checkType.testPredicate(arg1.getter().apply(o), arg2.getter().apply(o));
        }
        //..oppure un valore inserito manualmente, convertito una volta sola nel tipo atteso
        final Object arg2Value = convertValue(inputFilter, expectedTypes2, jsonMapper);
        return o -> checkType.testPredicate(arg1.getter().apply(o), arg2Value);
    }

    /**
     * Mappa una lista di {@link StreamingFilterRequest} in una nuova lista di {@link Predicate}.
     *
     * @param inputFilters <i>(opzionale)</i> le regole di filtraggio da applicare allo streaming di dati
     * @param type <i>(obbligatorio)</i> il tipo dell'oggetto interessato
     * @param jsonMapper <i>(obbligatorio)</i> il mapper usato per convertire i valori nel tipo dei campi
     * @param <T> la specifica del tipo dell'oggetto interessato
     * @return {@code null} se l'input è {@code null} o vuoto, la lista di {@link Predicate} altrimenti
     */
    public static <T> List<Predicate<T>> mapToPredicates(List<StreamingFilterRequest> inputFilters, @NotNull Class<T> type,
                                                         @NotNull JsonMapper jsonMapper) {
        if (CollectionUtils.isEmpty(inputFilters)) return null;
        final List<Predicate<T>> outputFilters = new ArrayList<>();
        for (StreamingFilterRequest f : inputFilters) {
            outputFilters.add(mapToPredicate(f, type, jsonMapper));
        }
        return outputFilters;
    }

    //Converte il valore nel primo tipo atteso compatibile (es. 5 -> Long, "2030-01-01T00:00:00Z" -> OffsetDateTime)
    private static Object convertValue(@NotNull StreamingFilterRequest inputFilter, @NotNull List<Class<?>> expectedTypes,
                                       @NotNull JsonMapper jsonMapper) {
        for (Class<?> expectedType : expectedTypes) {
            try {
                return jsonMapper.convertValue(inputFilter.value(), expectedType);
            } catch (RuntimeException _) {
            }
        }
        throw new InvalidFilterException("Value '%s' is not compatible with field '%s' for CheckType '%s'",
                inputFilter.value(), inputFilter.fieldPath(), inputFilter.check());
    }

    //Due tipi sono compatibili se uno dei due può contenere l'altro (es. Number e Integer)
    private static boolean compatible(@NotNull Class<?> type1, @NotNull Class<?> type2) {
        return type1.isAssignableFrom(type2) || type2.isAssignableFrom(type1);
    }

    /**
     * Traduce il parametro path in una funzione che dato un oggetto di tipo T
     * fornisce il valore del campo specificato nel path se valorizzato, null altrimenti,
     * insieme al tipo (wrapper per i primitivi) del campo.
     * Se il path non è coerente con la struttura del tipo T viene sollevata una eccezione.
     *
     * @param type <i>(obbligatorio)</i> il tipo dell'oggetto interessato
     * @param <T> la specifica del tipo dell'oggetto interessato
     * @param fieldPath <i>(obbligatorio)</i> il path del campo da estrarre
     * @return la funzione che dato in input un oggetto del tipo specificato restituisce il valore
     * del campo descritto dal path, e il tipo del campo
     * @throws InvalidFilterException se il path non è valido per il tipo
     */
    private static <T> FieldAccessor<T> mapFieldAccessor(@NotNull Class<T> type, @NotBlank String fieldPath) {
        //Estrazione del percorso per il campo richiesto
        //NOTA: la dimensione dell'array equivale alla profondità del campo in oggetti annidati
        final String[] pathCrumbles = fieldPath.split("\\" + NESTED_PROPERTY_SEPARATOR);
        if (pathCrumbles.length == 0) {
            throw new InvalidFilterException("'%s' is not a valid path for %s", fieldPath, type.getSimpleName());
        }
        Function<T, Object> chain = o -> o;
        Class<?> currentType = type;

        for (String pathCrumble: pathCrumbles) { //Per ogni passo del percorso..
            final Field field = ReflectionUtils.findField(currentType, pathCrumble); //..si estrae il campo dall'oggetto corrente..
            if (field == null) {
                throw new InvalidFilterException("'%s' is not a valid field for %s", pathCrumble, currentType.getSimpleName());
            }
            ReflectionUtils.makeAccessible(field);

            final MethodHandle getter;
            try {
                getter = MethodHandles.lookup().unreflectGetter(field); //..e se ne cerca il getter..
            } catch (IllegalAccessException _) {
                throw new InvalidFilterException("%s has not a valid getter for '%s'", currentType.getSimpleName(), pathCrumble);
            }

            Function<Object, ?> step = obj -> { //..da cui si ricava un function..
                //Questo controllo rende sicura la catena di get nel caso di step null
                if (obj == null) return null;
                try {
                    return getter.invoke(obj);
                } catch (Throwable t) {
                    throw new RuntimeException(t); //In teoria non si verificano mai le condizioni per entrare qui.
                }
            };
            chain = chain.andThen(step); //..che si mette in cascata a quella calcolata fino ad ora.
            currentType = field.getType();
        }
        return new FieldAccessor<>(chain, ClassUtils.resolvePrimitiveIfNecessary(currentType));
    }

    private record FieldAccessor<T>(Function<T, ?> getter, Class<?> type) {}
}
