package com.fcusumano.push.util;

import jakarta.validation.ConstraintValidator;
import jakarta.validation.ConstraintValidatorContext;

import java.lang.annotation.Annotation;

public abstract class AbstractValidator<A extends Annotation, T> implements ConstraintValidator<A, T> {

    protected boolean validationError(ConstraintValidatorContext context, String message) {
        context.disableDefaultConstraintViolation();
        context.buildConstraintViolationWithTemplate(message).addConstraintViolation();
        return false;
    }

    protected boolean validationError(ConstraintValidatorContext context, String pattern, Object... args) {
        return validationError(context, String.format(pattern, args));
    }
}
