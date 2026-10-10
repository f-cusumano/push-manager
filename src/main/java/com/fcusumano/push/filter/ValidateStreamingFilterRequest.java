package com.fcusumano.push.filter;

import jakarta.validation.Constraint;
import jakarta.validation.Payload;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;


/**
 * Annotation di validazione utilizzata per eseguire controlli semantici avanzati
 * su un oggetto {@link StreamingFilterRequest}.
 *
 * <p>
 * {@code @ValidateStreamingFilterRequest} è una constraint di Bean Validation
 * che delega la logica di validazione alla classe {@link StreamingFilterRequestValidator}.
 * L’annotation è applicabile a livello di <b>tipo</b> ({@link ElementType#TYPE})
 * poiché la validazione coinvolge la relazione tra più campi dell’oggetto
 * e non singole proprietà.
 * </p>
 *
 * <h2>Scopo</h2>
 * <p>
 * L’annotation garantisce che una richiesta di filtraggio per lo streaming
 * SSE sia:
 * </p>
 * <ul>
 *     <li>strutturalmente coerente;</li>
 *     <li>semanticamente valida rispetto al {@link CheckType} scelto;</li>
 *     <li>non ambigua nella combinazione dei parametri {@code value} e
 *         {@code fieldPathValue}.</li>
 * </ul>
 *
 * <h2>Regole di validazione</h2>
 * <p>
 * Le regole applicate sono implementate in
 * {@link StreamingFilterRequestValidator} e includono:
 * </p>
 * <ul>
 *     <li>validazione condizionata dal {@link TargetDefinition} del {@link CheckType};</li>
 *     <li>verifica della presenza esclusiva o assente di parametri opzionali;</li>
 *     <li>prevenzione di configurazioni di filtro non supportate.</li>
 * </ul>
 *
 * @author Francesco Cusumano
 */
@Documented
@Target({ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
@Constraint(validatedBy = StreamingFilterRequestValidator.class)
public @interface ValidateStreamingFilterRequest {

    String message() default "Validation failed.";

    Class<?>[] groups() default {};

    Class<? extends Payload>[] payload() default {};
}
