package com.fcusumano.push.exception;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ResponseStatus;

/**
 * Nessun client registrato con il session-id richiesto: gli endpoint rispondono {@code 404}.
 *
 * @author Francesco Cusumano
 */
@ResponseStatus(HttpStatus.NOT_FOUND)
public class NoSuchClientException extends RuntimeException {

    public NoSuchClientException(String message) {
        super(message);
    }

    public NoSuchClientException(String pattern, Object... args) {
        this(String.format(pattern, args));
    }
}
