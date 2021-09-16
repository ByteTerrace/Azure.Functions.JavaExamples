package com.byteterrace.azure.functions;

import java.util.List;

public final class SomePojo {
    public String A;
    public String B;

    public SomePojo(final List<String> fields) {
        A = fields.get(0);
        B = fields.get(1);
    }

    public String toString() {
        return String.format("%s,%s", A, B);
    }
}
