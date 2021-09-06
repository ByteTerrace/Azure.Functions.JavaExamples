package com.byteterrace.azure.functions;

import java.nio.ByteBuffer;

public final class LineReaderState {
    private final ByteBuffer m_buffer;
    private final StringBuilder m_stringBuilder;

    private boolean m_isPreviousCharacterCarriageReturn;

    public ByteBuffer getBuffer() {
        return m_buffer;
    }
    public boolean getIsPreviousCharacterCarriageReturn() {
        return m_isPreviousCharacterCarriageReturn;
    }
    public void setIsPreviousCharacterCarriageReturn(boolean value) {
        m_isPreviousCharacterCarriageReturn = value;
    }
    public StringBuilder getStringBuilder() {
        return m_stringBuilder;
    }

    public LineReaderState(ByteBuffer buffer) {
        m_buffer = buffer;
        m_stringBuilder = new StringBuilder(137);

        setIsPreviousCharacterCarriageReturn(false);
    }
}
