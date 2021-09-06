package com.byteterrace.azure.functions;

public final class UncheckedInterruptedException extends RuntimeException {
    public UncheckedInterruptedException(InterruptedException exception) {
        super(exception);
    }
}
