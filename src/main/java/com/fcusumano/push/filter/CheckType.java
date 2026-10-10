package com.fcusumano.push.filter;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.BiPredicate;
import java.util.function.Predicate;

import static com.fcusumano.push.filter.TargetDefinition.SINGLE_OBJECT;
import static com.fcusumano.push.filter.TargetDefinition.TWO_OBJ_DIFFERENT_TYPE;
import static com.fcusumano.push.filter.TargetDefinition.TWO_OBJ_SAME_TYPE;

/**
 * Definisce l'insieme delle operazioni di controllo applicabili durante
 * la valutazione delle regole di filtraggio degli eventi in streaming.
 *
 * <p>
 * {@code CheckType} rappresenta il cuore logico del sistema di filtering: ogni
 * valore dell'enum descrive <b>che tipo di confronto</b> deve essere applicato
 * e <b>su quali tipi di oggetti</b> tale confronto è consentito.
 * </p>
 *
 * <p>
 * Ogni check è associato a:
 * </p>
 * <ul>
 *     <li>una {@link TargetDefinition}, che descrive la struttura del confronto
 *         (un solo oggetto, due oggetti dello stesso tipo o due oggetti di tipo differente);</li>
 *     <li>una o più {@link CheckDefinition}, che mappano le coppie di tipi supportate
 *         dalla funzione di confronto concreta.</li>
 * </ul>
 *
 * <p>
 * L'enum è annotato con {@link io.swagger.v3.oas.annotations.media.Schema}
 * per consentire a Swagger/OpenAPI di documentare i valori come riferimento
 * enumerativo all'interno delle API.
 * </p>
 *
 * <h2>Risoluzione dinamica dei predicati</h2>
 * <p>
 * {@code CheckType} supporta la selezione dinamica della funzione di confronto
 * corretta in base ai tipi runtime degli oggetti confrontati:
 * </p>
 * <ul>
 *     <li>{@link #selectPredicate(Object)} per check su singolo oggetto;</li>
 *     <li>{@link #selectPredicate(Object, Object)} per check binari;</li>
 *     <li>{@link #testPredicate(Object)} e {@link #testPredicate(Object, Object)}
 *         come metodi di utilità per l'esecuzione diretta del check.</li>
 * </ul>
 *
 * <p>
 * Se nessuna {@link CheckDefinition} compatibile viene trovata, viene sollevata
 * una {@link RuntimeException}. La validazione preventiva sulla correttezza
 * della combinazione di tipi è demandata al livello applicativo e ai validator.
 * </p>
 *
 * <h2>Design interno: CheckDefinition</h2>
 * <p>
 * L’enum utilizza internamente il record {@code CheckDefinition} per incapsulare:
 * </p>
 * <ul>
 *     <li>il tipo del primo operando;</li>
 *     <li>il tipo del secondo operando (eventualmente nullo);</li>
 *     <li>la funzione di confronto espressa come {@link java.util.function.Predicate}
 *         o {@link java.util.function.BiPredicate}.</li>
 * </ul>
 *
 * <p>
 * Questo approccio consente di dichiarare in modo compatto, type-safe ed estensibile
 * tutte le varianti di confronto supportate dal sistema.
 * </p>
 *
 * <h2>Note</h2>
 * <ul>
 *     <li>L’enum descrive la semantica dei check, non il parsing delle richieste.</li>
 *     <li>Nuovi check possono essere aggiunti introducendo nuovi valori dell’enum
 *         senza modificare la logica di esecuzione.</li>
 *     <li>Il sistema è progettato per essere facilmente estendibile a nuovi tipi
 *         (numerici, temporali, stringa, ecc.).</li>
 * </ul>
 *
 * @author Francesco Cusumano
 */
@Schema(enumAsRef = true)
public enum CheckType {

    /**
     * Controlla se un dato campo è valorizzato, es:
     * <pre>{@code
     * FIELD_PRESENT.testPredicate("Hello World"); // true
     * FIELD_PRESENT.testPredicate(null); // false
     * }</pre>
     * Nota: possibilità per il futuro di estendere alle stringe e alle collezioni con 'notBlank' e 'notEmpty'
     */
    FIELD_PRESENT(SINGLE_OBJECT, List.of(CheckDefinition.of(Object.class, Objects::nonNull))),

    /**
     * Controlla se un dato campo è non valorizzato, es:
     * <pre>{@code
     * FIELD_ABSENT.testPredicate("Hello World"); // false
     * FIELD_ABSENT.testPredicate(null); // true
     * }</pre>
     * Nota: possibilità per il futuro di estendere alle stringe e alle collezioni con 'blank' e 'empty'
     */
    FIELD_ABSENT(SINGLE_OBJECT, List.of(CheckDefinition.of(Object.class, Objects::isNull))),

    /**
     * Controlla se due oggetti sono concettualmente identici, es:
     * SAME_VALUE.testPredicate("Hello World", "Hello World"); // true
     * SAME_VALUE.testPredicate("Hello World", "HelloWorld"); // false
     */
    SAME_VALUE(TWO_OBJ_SAME_TYPE, List.of(CheckDefinition.of(Object.class, Objects::equals))),

    /**
     * Controlla se il primo valore è inferiore del secondo, es:
     * <pre>{@code
     * LESS_THEN.testPredicate(5, 10); // true
     * LESS_THEN.testPredicate(5, 5); // false
     * LESS_THEN.testPredicate(10, 5); // false
     * LESS_THEN.testPredicate("2026-04-02 10:00:00", "2026-04-02 15:00:00"); // true
     * LESS_THEN.testPredicate("2026-04-02 10:00:00", "2026-04-02 10:00:00"); // false
     * LESS_THEN.testPredicate("2026-04-02 15:00:00", "2026-04-02 10:00:00"); // false
     * }</pre>
     */
    LESS_THEN(TWO_OBJ_SAME_TYPE, List.of(
            CheckDefinition.of(Integer.class, (a, b) -> a < b),
            CheckDefinition.of(Long.class, (a, b) -> a < b),
            CheckDefinition.of(Double.class, (a, b) -> a < b),
            CheckDefinition.of(OffsetDateTime.class, OffsetDateTime::isBefore),
            CheckDefinition.of(LocalDateTime.class, LocalDateTime::isBefore),
            CheckDefinition.of(Instant.class, Instant::isBefore)
    )),

    /**
     * Controlla se il primo valore è inferiore o uguale al secondo, es:
     * <pre>{@code
     * LESS_THEN_OR_EQUAL_TO.testPredicate(5, 10); // true
     * LESS_THEN_OR_EQUAL_TO.testPredicate(5, 5); // true
     * LESS_THEN_OR_EQUAL_TO.testPredicate(10, 5); // false
     * LESS_THEN_OR_EQUAL_TO.testPredicate("2026-04-02 10:00:00", "2026-04-02 15:00:00"); // true
     * LESS_THEN_OR_EQUAL_TO.testPredicate("2026-04-02 10:00:00", "2026-04-02 10:00:00"); // true
     * LESS_THEN_OR_EQUAL_TO.testPredicate("2026-04-02 15:00:00", "2026-04-02 10:00:00"); // false
     * }</pre>
     */
    LESS_THEN_OR_EQUAL_TO(TWO_OBJ_SAME_TYPE, List.of(
            CheckDefinition.of(Integer.class, (a, b) -> a <= b),
            CheckDefinition.of(Long.class, (a, b) -> a <= b),
            CheckDefinition.of(Double.class, (a, b) -> a <= b),
            CheckDefinition.of(OffsetDateTime.class, (a, b) -> !a.isAfter(b)),
            CheckDefinition.of(LocalDateTime.class, (a, b) -> !a.isAfter(b)),
            CheckDefinition.of(Instant.class, (a, b) -> !a.isAfter(b))
    )),

    /**
     * Controlla se il primo valore è maggiore del secondo, es:
     * <pre>{@code
     * GREATER_THEN.testPredicate(5, 10); // false
     * GREATER_THEN.testPredicate(5, 5); // false
     * GREATER_THEN.testPredicate(10, 5); // true
     * GREATER_THEN.testPredicate("2026-04-02 10:00:00", "2026-04-02 15:00:00"); // false
     * GREATER_THEN.testPredicate("2026-04-02 10:00:00", "2026-04-02 10:00:00"); // false
     * GREATER_THEN.testPredicate("2026-04-02 15:00:00", "2026-04-02 10:00:00"); // true
     * }</pre>
     */
    GREATER_THEN(TWO_OBJ_SAME_TYPE, List.of(
            CheckDefinition.of(Integer.class, (a, b) -> a > b),
            CheckDefinition.of(Long.class, (a, b) -> a > b),
            CheckDefinition.of(Double.class, (a, b) -> a > b),
            CheckDefinition.of(OffsetDateTime.class, OffsetDateTime::isAfter),
            CheckDefinition.of(LocalDateTime.class, LocalDateTime::isAfter),
            CheckDefinition.of(Instant.class, Instant::isAfter)
    )),

    /**
     * Controlla se il primo valore è maggiore o uguale al secondo, es:
     * <pre>{@code
     * GREATER_THEN_OR_EQUAL_TO.testPredicate(5, 10); // false
     * GREATER_THEN_OR_EQUAL_TO.testPredicate(5, 5); // true
     * GREATER_THEN_OR_EQUAL_TO.testPredicate(10, 5); // true
     * GREATER_THEN_OR_EQUAL_TO.testPredicate("2026-04-02 10:00:00", "2026-04-02 15:00:00"); // false
     * GREATER_THEN_OR_EQUAL_TO.testPredicate("2026-04-02 10:00:00", "2026-04-02 10:00:00"); // true
     * GREATER_THEN_OR_EQUAL_TO.testPredicate("2026-04-02 15:00:00", "2026-04-02 10:00:00"); // true
     * }</pre>
     */
    GREATER_THEN_OR_EQUAL_TO(TWO_OBJ_SAME_TYPE, List.of(
            CheckDefinition.of(Integer.class, (a, b) -> a >= b),
            CheckDefinition.of(Long.class, (a, b) -> a >= b),
            CheckDefinition.of(Double.class, (a, b) -> a >= b),
            CheckDefinition.of(OffsetDateTime.class, (a, b) -> !a.isBefore(b)),
            CheckDefinition.of(LocalDateTime.class, (a, b) -> !a.isBefore(b)),
            CheckDefinition.of(Instant.class, (a, b) -> !a.isBefore(b))
    )),

    /**
     * Controlla se una data è nel passato, es:
     * <pre>{@code
     * //Presupponendo che il controllo venga fatto il "2026-04-02 10:00:00"...
     * IS_IN_THE_PAST.testPredicate("2026-04-02 05:00:00"); // true
     * IS_IN_THE_PAST.testPredicate("2026-04-02 10:00:00"); // false
     * IS_IN_THE_PAST.testPredicate("2026-04-02 15:00:00"); // false
     * }</pre>
     */
    IS_IN_THE_PAST(SINGLE_OBJECT, List.of(
            CheckDefinition.of(OffsetDateTime.class, a -> a.isBefore(OffsetDateTime.now())),
            CheckDefinition.of(LocalDateTime.class, a -> a.isBefore(LocalDateTime.now())),
            CheckDefinition.of(Instant.class, a -> a.isBefore(Instant.now()))
    )),

    /**
     * Controlla se una data è nel futuro, es:
     * <pre>{@code
     * //Presupponendo che il controllo venga fatto il "2026-04-02 10:00:00"...
     * IS_IN_THE_FUTURE.testPredicate("2026-04-02 05:00:00"); // false
     * IS_IN_THE_FUTURE.testPredicate("2026-04-02 10:00:00"); // false
     * IS_IN_THE_FUTURE.testPredicate("2026-04-02 15:00:00"); // true
     * }</pre>
     */
    IS_IN_THE_FUTURE(SINGLE_OBJECT, List.of(
            CheckDefinition.of(OffsetDateTime.class, a -> a.isAfter(OffsetDateTime.now())),
            CheckDefinition.of(LocalDateTime.class, a -> a.isAfter(LocalDateTime.now())),
            CheckDefinition.of(Instant.class, a -> a.isAfter(Instant.now()))
    )),

    /**
     * Controlla se una stringa inizia con una certa combinazione di caratteri, es:
     * <pre>{@code
     * STARTS_WITH.testPredicate("Hello World", "Hello"); // true
     * STARTS_WITH.testPredicate("Hello World", "World"); // false
     * STARTS_WITH.testPredicate("Hello World", "hello"); // false
     * }</pre>
     */
    STARTS_WITH(TWO_OBJ_SAME_TYPE, List.of(CheckDefinition.of(String.class, String::startsWith))),

    /**
     * Controlla se una stringa finisce con una certa combinazione di caratteri, es:
     * <pre>{@code
     * ENDS_WITH.testPredicate("Hello World", "Hello"); // false
     * ENDS_WITH.testPredicate("Hello World", "World"); // true
     * ENDS_WITH.testPredicate("Hello World", "world"); // false
     * }</pre>
     */
    ENDS_WITH(TWO_OBJ_SAME_TYPE, List.of(CheckDefinition.of(String.class, String::endsWith))),

    /**
     * Controlla se una stringa contiene con una certa combinazione di caratteri, es:
     * <pre>{@code
     * CONTAINS.testPredicate("Hello World", "Hello"); // true
     * CONTAINS.testPredicate("Hello World", "World"); // true
     * CONTAINS.testPredicate("Hello World", " "); // true
     * CONTAINS.testPredicate("Hello World", "My name is"); // false
     * CONTAINS.testPredicate("Hello World", "hello"); // false
     * CONTAINS.testPredicate("Hello World", "world"); // false
     * }</pre>
     */
    CONTAINS(TWO_OBJ_SAME_TYPE, List.of(CheckDefinition.of(String.class, String::contains))),

    /**
     * Controlla se una stringa inizia con una certa combinazione di caratteri (ignora maiuscole/minuscole), es:
     * <pre>{@code
     * STARTS_WITH_IGNORE_CASE.testPredicate("Hello World", "Hello"); // true
     * STARTS_WITH_IGNORE_CASE.testPredicate("Hello World", "World"); // false
     * STARTS_WITH_IGNORE_CASE.testPredicate("Hello World", "hello"); // true
     * }</pre>
     */
    STARTS_WITH_IGNORE_CASE(TWO_OBJ_SAME_TYPE, List.of(
            CheckDefinition.of(String.class, (a, b) -> a.toUpperCase().startsWith(b.toUpperCase()))
    )),

    /**
     * Controlla se una stringa finisce con una certa combinazione di caratteri (ignora maiuscole/minuscole), es:
     * <pre>{@code
     * ENDS_WITH_IGNORE_CASE.testPredicate("Hello World", "Hello"); // false
     * ENDS_WITH_IGNORE_CASE.testPredicate("Hello World", "World"); // true
     * ENDS_WITH_IGNORE_CASE.testPredicate("Hello World", "world"); // true
     * }</pre>
     */
    ENDS_WITH_IGNORE_CASE(TWO_OBJ_SAME_TYPE, List.of(
            CheckDefinition.of(String.class, (a, b) -> a.toUpperCase().endsWith(b.toUpperCase()))
    )),

    /**
     * Controlla se una stringa contiene con una certa combinazione di caratteri (ignora maiuscole/minuscole), es:
     * <pre>{@code
     * CONTAINS_IGNORE_CASE.testPredicate("Hello World", "Hello"); // true
     * CONTAINS_IGNORE_CASE.testPredicate("Hello World", "World"); // true
     * CONTAINS_IGNORE_CASE.testPredicate("Hello World", " "); // true
     * CONTAINS_IGNORE_CASE.testPredicate("Hello World", "My name is"); // false
     * CONTAINS_IGNORE_CASE.testPredicate("Hello World", "hello"); // true
     * CONTAINS_IGNORE_CASE.testPredicate("Hello World", "world"); // true
     * }</pre>
     */
    CONTAINS_IGNORE_CASE(TWO_OBJ_SAME_TYPE, List.of(
            CheckDefinition.of(String.class, (a, b) -> a.toUpperCase().contains(b.toUpperCase()))
    )),

    /**
     * Controlla se una stringa ha esattamente la lunghezza specificata, es:
     * <pre>{@code
     * EXACT_LENGTH.testPredicate("Hello World", 11); // true
     * EXACT_LENGTH.testPredicate("Hello World", 10); // false
     * }</pre>
     */
    EXACT_LENGTH(TWO_OBJ_DIFFERENT_TYPE, List.of(
            CheckDefinition.of(String.class, Integer.class, (s, l) -> s.length() == l)
    )),

    /**
     * Controlla se la lunghezza di una stringa è inferiore alla lunghezza specificata, es:
     * <pre>{@code
     * LENGTH_LESS_THAN.testPredicate("Hello World", 11); // false
     * LENGTH_LESS_THAN.testPredicate("Hello World", 10); // true
     * LENGTH_LESS_THAN.testPredicate("Hello World", 12); // false
     * }</pre>
     */
    LENGTH_LESS_THAN(TWO_OBJ_DIFFERENT_TYPE, List.of(
            CheckDefinition.of(String.class, Integer.class, (s, l) -> s.length() < l)
    )),

    /**
     * Controlla se la lunghezza di una stringa è superiore alla lunghezza specificata, es:
     * <pre>{@code
     * LENGTH_LESS_THAN.testPredicate("Hello World", 11); // false
     * LENGTH_LESS_THAN.testPredicate("Hello World", 10); // false
     * LENGTH_LESS_THAN.testPredicate("Hello World", 12); // true
     * }</pre>
     */
    LENGTH_MORE_THAN(TWO_OBJ_DIFFERENT_TYPE, List.of(
            CheckDefinition.of(String.class, Integer.class, (s, l) -> s.length() > l)
    ));

    private final @NotNull TargetDefinition targetDefinition;
    private final @NotEmpty List<CheckDefinition<?, ?>> type2definition;

    CheckType(@NotNull TargetDefinition targetDefinition,
              @NotEmpty List<CheckDefinition<?, ?>> type2definition) {
        this.targetDefinition = targetDefinition;
        this.type2definition = type2definition;
    }

    public TargetDefinition getTargetDefinition() {
        return targetDefinition;
    }

    /**
     * Permette di interrogare il sistema per conoscere
     * i possibili tipi del secondo operando, dato il tipo del primo.
     * Questa funzionalità è utile per:
     * <ul>
     *     <li>validazioni semantiche delle richieste di filtro;</li>
     *     <li>costruzione dinamica di interfacce di configurazione;</li>
     *     <li>prevenzione di confronti non supportati.</li>
     * </ul>
     */
    public List<Class<?>> getPossibleTypes2(@NotNull Class<?> type1) {
        final List<Class<?>> result = new ArrayList<>();
        for (CheckDefinition<?, ?> d : type2definition) {
            if (Objects.equals(d.type1, type1) && d.type2 != null) {
                result.add(d.type2);
            }
        }
        return List.copyOf(result);
    }

    private <T> Predicate<T> selectPredicate(@NotNull T obj) {
        return o -> ((BiPredicate<T, ?>) type2definition.stream()
                .filter(d -> d.type1.isInstance(obj))
                .findFirst().map(CheckDefinition::checkFunction)
                .orElseThrow(RuntimeException::new)).test(o, null);
    }

    private <T, U> BiPredicate<T, U> selectPredicate(@NotNull T obj1, @NotNull U obj2) {
        return (BiPredicate<T, U>) type2definition.stream()
                .filter(d -> d.type1.isInstance(obj1) && d.type2.isInstance(obj2))
                .findFirst().map(CheckDefinition::checkFunction).orElseThrow(RuntimeException::new);
    }

    public <T> boolean testPredicate(T obj) {
        //Per testare un predicato l'argomenti deve essere valorizzato,
        //unica eccezione per il caso FIELD_ABSENT;
        //In tutti gli altri casi se è nullo il controllo restituisce false.
        return obj == null
                ? this == FIELD_ABSENT //Semplificazione null-safe -> rendere dinamico
                : selectPredicate(obj).test(obj);
    }

    public <T, U> boolean testPredicate(T obj1, U obj2) {
        //Per testare un predicato entrambi gli argomenti devono essere valorizzati,
        //unica eccezione per il caso SAME_VALUE ed entrambi gli argomenti nulli;
        //In tutti gli altri casi se uno dei due è nullo il controllo restituisce false.
        return obj1 == null
                ? this == SAME_VALUE && obj2 == null //Semplificazione null-safe -> rendere dinamico
                : obj2 != null && selectPredicate(obj1, obj2).test(obj1, obj2);
    }


    /**
     * Record interno che rappresenta una singola definizione di controllo.
     *
     * <p>{@code CheckDefinition} incapsula l’associazione tra:</p>
     * <ul>
     *     <li>il tipo del primo operando;</li>
     *     <li>il tipo del secondo operando (opzionale);</li>
     *     <li>la funzione di confronto concreta da applicare.</li>
     * </ul>
     *
     * <p>
     * Questo record consente di descrivere in modo compatto, type‑safe ed estensibile
     * le combinazioni di tipi supportate da ciascun {@link CheckType}, delegando
     * la logica di valutazione a {@link java.util.function.Predicate} o
     * {@link java.util.function.BiPredicate}.
     * </p>
     *
     * <h2>Parametri di tipo</h2>
     * <ul>
     *     <li><b>T</b> – tipo del primo operando;</li>
     *     <li><b>U</b> – tipo del secondo operando, se previsto dal check
     *         (può essere {@code null} per check su singolo oggetto).</li>
     * </ul>
     *
     * <h2>Componenti del record</h2>
     * <ul>
     *     <li><b>type1</b> – classe del primo operando supportato;</li>
     *     <li><b>type2</b> – classe del secondo operando supportato, oppure {@code null}
     *         se il check opera su un solo oggetto;</li>
     *     <li><b>checkFunction</b> – funzione di confronto da applicare,
     *         espressa come {@link BiPredicate} e mappato come {@link Predicate}
     *         se il check opera su un solo oggetto.</li>
     * </ul>
     *
     * <h2>Note</h2>
     * <ul>
     *     <li>Il record è dichiarato {@code private} perché è un dettaglio
     *         implementativo di {@link CheckType}.</li>
     *     <li>L’assenza di {@code type2} (null) indica un check unario.</li>
     *     <li>La responsabilità della validazione preventiva dei tipi è demandata
     *         ai validator applicativi.</li>
     *     <li>Il design consente di estendere facilmente il sistema introducendo
     *         nuove combinazioni di tipi e nuove funzioni di confronto.</li>
     * </ul>
     *
     * @author Francesco Cusumano
     */
    private record CheckDefinition<T, U>(
            @NotNull Class<T> type1,
            Class<U> type2,
            @NotNull BiPredicate<T, U> checkFunction
    ) {

        private static <A> CheckDefinition<A, ?> of(@NotNull Class<A> type, @NotNull Predicate<A> checkFunction) {
            return new CheckDefinition<>(type, null, (a, b) -> checkFunction.test(a));
        }

        private static <A> CheckDefinition<A, A> of(@NotNull Class<A> type, @NotNull BiPredicate<A, A> checkFunction) {
            return new CheckDefinition<>(type, type, checkFunction);
        }

        private static <A, B> CheckDefinition<A, B> of(@NotNull Class<A> type1, @NotNull Class<B> type2,
                                                       @NotNull BiPredicate<A, B> checkFunction) {
            return new CheckDefinition<>(type1, type2, checkFunction);
        }
    }
}
