package com.fcusumano.push.util;

import com.fcusumano.push.event.EventType;
import com.fcusumano.push.event.SsePushEvent;
import com.fcusumano.push.filter.CheckType;
import com.fcusumano.push.filter.FilteringOptions;
import com.fcusumano.push.filter.FilteringRequest;
import com.fcusumano.push.filter.StreamingFilterRequest;
import com.fcusumano.push.filter.TargetDefinition;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import org.springframework.util.CollectionUtils;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter.SseEventBuilder;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.function.Predicate;

import static com.fcusumano.push.filter.FilteringRequest.nullEquivalent;
import static com.fcusumano.push.filter.TargetDefinition.SINGLE_OBJECT;
import static com.fcusumano.push.util.Constant.NESTED_PROPERTY_SEPARATOR;
import static org.springframework.util.StringUtils.hasText;


/**
 * Classe statica che raccoglie metodi di supporto per i mapping non lineari tra tipi.
 */
public final class ObjectMapper {

    private ObjectMapper() {
    }

    /**
     * Mappa un {@link SsePushEvent} in un nuovo {@link SseEventBuilder}.
     *
     * @param event <i>(opzionale)</i> un evento SSE
     * @return {@code null} se l'input è {@code null}, l'oggetto mappato altrimenti
     */
    public static SseEventBuilder toEventBuilder(SsePushEvent<?> event) {
        return event == null ? null : SseEmitter.event()
                .name(event.getEventType().name())
                .id(event.getMessageId())
                .reconnectTime(event.getRetryMs() == null ? 0L : event.getRetryMs())
                .data(event.getData() == null ? "" : event.getData());
    }

    /**
     * Mappa un {@link FilteringRequest} in un nuovo {@link FilteringOptions} specificando il tipo
     * dell'oggetto interessato dal criterio di filtraggio; il mapping si avvale di metodi specifici
     * opportunamente valorizzati per elaborare il mapping dei singoli attributi.
     *
     * @param inputFilters <i>(opzionale)</i> la richiesta di filtri da applicare allo streaming di dati
     * @param type         <i>(obbligatorio)</i> il tipo dell'oggetto interessato
     * @param <T>          la specifica del tipo dell'oggetto interessato
     * @return {@code null} se l'input è {@code null}, l'oggetto mappato altrimenti
     */
    public static <T> FilteringOptions<T> mapToFilteringOptions(FilteringRequest inputFilters, @NotNull Class<T> type) {
        return nullEquivalent(inputFilters) ? null : FilteringOptions.of(
                filterMuteable(inputFilters.requiredEventTypes()),
                mapToPredicates(inputFilters.contentFilters(), type)
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
     *
     * @param inputFilter <i>(obbligatorio)</i> la singola regola di filtraggio da applicare allo streaming di dati
     * @param type <i>(obbligatorio)</i> il tipo dell'oggetto interessato
     * @param <T> la specifica del tipo dell'oggetto interessato
     * @return il predicato per testare se un dato payload è di interesse per un dato client
     */
    public static <T> Predicate<T> mapToPredicate(@NotNull StreamingFilterRequest inputFilter, @NotNull Class<T> type) {
        final CheckType checkType = inputFilter.check();
        final TargetDefinition targetDefinition = checkType.getTargetDefinition();
        final Function<T, ?> arg1mapper = mapFieldGetter(type, inputFilter.fieldPath());
        if (targetDefinition == SINGLE_OBJECT) {
            //Dato un oggetto 'o' si estrae il valore del campo richiesto e si testa
            return o -> checkType.testPredicate(arg1mapper.apply(o));
        }
        //Il secondo argomento può essere un altro campo target oppure un valore inserito manualmente;
        //Nel primo caso viene inizializzata la function per ricavare il valore del campo dall'oggetto;
        final Function<T, ?> arg2mapper = !hasText(inputFilter.fieldPathValue()) ? null
                : mapFieldGetter(type, inputFilter.fieldPathValue());
        //Nel secondo caso si tenta di castare il valore in input sul tipo corretto in base al valore del primo argomento
        // e usando la targetDefinition
        final Function<Object, ?> arg2valueByDefinition = arg1 -> arg1 == null //Se il primo argomento è nullo..
                ? inputFilter.value() //..non serve tipizzare il secondo (vedi testPredicate(T obj1, U obj2))
                : switch (targetDefinition) { //altrimenti..
            //Stesso tipo di arg1, quindi si usa il tipo di arg1 per castare il valore in input
            case TWO_OBJ_SAME_TYPE -> arg1.getClass().cast(inputFilter.value());
            //Si tenta di dedurre il tipo dal primo attraverso la definizione del check
            case TWO_OBJ_DIFFERENT_TYPE -> findRightCast(inputFilter.value(), checkType, arg1.getClass());
            //SINGLE_OBJECT già trattato in precedenza nel metodo
            default -> throw new RuntimeException("Impossible case in 'arg2valueByDefinition'");
        };

        return o -> { //Dato un oggetto 'o'
            final Object arg1Value = arg1mapper.apply(o); //si estrae il valore del primo argomento dal path
            final Object arg2Value = arg2mapper != null //si estrae il valore del secondo argomento..
                    ? arg2mapper.apply(o) //..dal path
                    : arg2valueByDefinition.apply(arg1Value); //..o dal valore inserito manualmente
            return checkType.testPredicate(arg1Value, arg2Value); //e si testano
        };
    }

    /**
     * Mappa una lista di {@link StreamingFilterRequest} in una nuova lista di {@link Predicate}.
     *
     * @param inputFilters <i>(opzionale)</i> le regole di filtraggio da applicare allo streaming di dati
     * @param type <i>(obbligatorio)</i> il tipo dell'oggetto interessato
     * @param <T> la specifica del tipo dell'oggetto interessato
     * @return {@code null} se l'input è {@code null} o vuoto, la lista di {@link Predicate} altrimenti
     */
    public static <T> List<Predicate<T>> mapToPredicates(List<StreamingFilterRequest> inputFilters, @NotNull Class<T> type) {
        if (CollectionUtils.isEmpty(inputFilters)) return null;
        final List<Predicate<T>> outputFilters = new ArrayList<>();
        for (StreamingFilterRequest f : inputFilters) {
            outputFilters.add(mapToPredicate(f, type));
        }
        return outputFilters;
    }

    //Studiato per TWO_OBJ_DIFFERENT_TYPE
    private static Object findRightCast(Object value, @NotNull CheckType check, @NotNull Class<?> arg1type) {
        for (Class<?> arg2type : check.getPossibleTypes2(arg1type)) {
            try {
                return arg2type.cast(value);
            } catch (ClassCastException _) {
            }
        }
        throw new RuntimeException("Types mismatch for " + check.name());
    }

    /**
     * Traduce il parametro path in una funzione che dato un oggetto di tipo T
     * fornisce il valore del campo specificato nel path se valorizzato, null altrimenti.
     * Se il path non è coerente con la struttura del tipo T viene sollevata una eccezione.
     *
     * @param type <i>(obbligatorio)</i> il tipo dell'oggetto interessato
     * @param <T> la specifica del tipo dell'oggetto interessato
     * @param fieldPath <i>(obbligatorio)</i> il path del campo da estrarre
     * @return una funzione che dato in input un oggetto del tipo specificato restituisce il valore
     * del campo descritto dal path
     */
    @SuppressWarnings("unchecked")
    private static <T> Function<T, ?> mapFieldGetter(@NotNull Class<T> type, @NotBlank String fieldPath) {
        //Estrazione del percorso per il campo richiesto
        //NOTA: la dimensione dell'array equivale alla profondità del campo in oggetti annidati
        final String[] pathCrumbles = fieldPath.split("\\" + NESTED_PROPERTY_SEPARATOR);
        if (pathCrumbles.length == 0) {
            throw new IllegalArgumentException("'" + fieldPath + "' is not a valid path for " + type.getSimpleName());
        }
        Function<T, Object> chain = o -> o;
        Class<?> currentType = type;

        for (String pathCrumble: pathCrumbles) { //Per ogni passo del percorso..
            final Field field;
            try {
                field = getFieldByName(currentType, pathCrumble); //..si estrae il campo dall'oggetto corrente..
            } catch (NoSuchFieldException _) {
                throw new IllegalArgumentException("'" + pathCrumble + "' is not a valid field for " + currentType.getSimpleName());
            }
            field.setAccessible(true);

            final MethodHandle getter;
            try {
                getter = MethodHandles.lookup().unreflectGetter(field); //..e se ne cerca il getter..
            } catch (IllegalAccessException _) {
                throw new IllegalArgumentException(currentType.getSimpleName() + " has not a valid getter for '" + pathCrumble + "'");
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
        return chain;
    }

    /**
     * Data una classe, cerca al suo interno il campo con nome indicato;
     * se non presente nella definizione base, la ricerca viene estesa alla definizione
     * di un'eventuale superclasse.
     * Se il campo non viene trovato, solleva un'eccezione.
     *
     * @param type la definizione della classe da cui estrarre il campo
     * @param property il nome del campo (case-sensitive) da cercare nella definizione della classe
     * @return il campo della classe col nome indicato in input
     * @throws NoSuchFieldException dovuto all'utilizzo di reflection
     */
    private static Field getFieldByName(Class<?> type, String property) throws NoSuchFieldException {
        try {
            return type.getDeclaredField(property);
        } catch (NoSuchFieldException e) {
            if (type.getSuperclass() != null) {
                return getFieldByName(type.getSuperclass(), property);
            }
            throw e;
        }
    }
}
