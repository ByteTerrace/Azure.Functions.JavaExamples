package com.byteterrace.azure.functions;

import java.nio.charset.CharacterCodingException;

public final class UncheckedCharacterCodingException extends RuntimeException {
    public UncheckedCharacterCodingException(CharacterCodingException exception) {
        super(exception);
    }
}
