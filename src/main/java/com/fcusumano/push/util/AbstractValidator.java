package com.fcusumano.push.util;

import jakarta.validation.ConstraintValidator;
import jakarta.validation.ConstraintValidatorContext;

import java.lang.annotation.Annotation;

/**
 * Base per i {@link ConstraintValidator} che restituiscono messaggi di errore personalizzati.
 * <p>
 * I metodi {@code validationError} sostituiscono la violazione di default con il messaggio indicato
 * e restituiscono sempre {@code false}, per poter scrivere:
 * </p>
 * <pre>{@code
 * return condizione || validationError(context, "Messaggio per '%s'.", arg);
 * }</pre>
 *
 * @param <A> annotazione di validazione gestita
 * @param <T> tipo dell'oggetto validato
 *
 * @author Francesco Cusumano
 */
public abstract class AbstractValidator<A extends Annotation, T> implements ConstraintValidator<A, T> {

    /**
     * Registra una violazione con il messaggio indicato al posto di quello di default.
     *
     * @param context contesto della validazione
     * @param message messaggio di errore
     * @return sempre {@code false}
     */
    protected boolean validationError(ConstraintValidatorContext context, String message) {
        context.disableDefaultConstraintViolation();
        context.buildConstraintViolationWithTemplate(message).addConstraintViolation();
        return false;
    }

    /**
     * Come {@link #validationError(ConstraintValidatorContext, String)}, con messaggio formattato
     * tramite {@link String#format(String, Object...)}.
     *
     * @param context contesto della validazione
     * @param pattern pattern del messaggio di errore
     * @param args argomenti del pattern
     * @return sempre {@code false}
     */
    protected boolean validationError(ConstraintValidatorContext context, String pattern, Object... args) {
        return validationError(context, String.format(pattern, args));
    }
}
