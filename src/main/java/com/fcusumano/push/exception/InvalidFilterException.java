package com.fcusumano.push.exception;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ResponseStatus;

/**
 * Filtro o selezione dei campi non applicabile al payload del canale
 * (campo inesistente, check non supportato dal tipo del campo, valore non convertibile):
 * gli endpoint rispondono {@code 400}.
 *
 * @author Francesco Cusumano
 */
@ResponseStatus(HttpStatus.BAD_REQUEST)
public class InvalidFilterException extends RuntimeException {

    public InvalidFilterException(String message) {
        super(message);
    }

    public InvalidFilterException(String pattern, Object... args) {
        this(String.format(pattern, args));
    }
}
