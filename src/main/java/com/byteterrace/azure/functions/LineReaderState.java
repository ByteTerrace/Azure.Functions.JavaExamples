package com.byteterrace.azure.functions;

import java.nio.ByteBuffer;

public final class LineReaderState {
    private final ByteBuffer m_buffer;
    private final StringBuilder m_stringBuilder;

    private boolean m_isNewLineCharacterHandlingEnabled;

    public ByteBuffer getBuffer() {
        return m_buffer;
    }
    public boolean getIsNewLineCharacterHandlingEnabled() {
        return m_isNewLineCharacterHandlingEnabled;
    }
    public void setIsNewLineCharacterHandlingEnabled(boolean value) {
        m_isNewLineCharacterHandlingEnabled = value;
    }
    public StringBuilder getStringBuilder() {
        return m_stringBuilder;
    }

    public LineReaderState(ByteBuffer buffer) {
        m_buffer = buffer;
        m_stringBuilder = new StringBuilder(137);

        setIsNewLineCharacterHandlingEnabled(false);
    }
}
