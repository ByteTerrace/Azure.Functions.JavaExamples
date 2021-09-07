package com.byteterrace.azure.functions;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CodingErrorAction;
import java.util.ArrayList;

public final class LineReaderState {
    private final CharsetDecoder m_charsetDecoder;
    private final CharBuffer m_decodedBlock;
    private final ByteBuffer m_encodedBlock;
    private final ArrayList<String> m_lines;
    private final StringBuilder m_stringBuilder;

    private boolean m_isNewLineCharacterHandlingEnabled;

    public CharBuffer getDecodedBlock() {
        return m_decodedBlock;
    }
    public ByteBuffer getEncodedBlock() {
        return m_encodedBlock;
    }
    public CharsetDecoder getCharsetDecoder() {
        return m_charsetDecoder;
    }
    public boolean getIsNewLineCharacterHandlingEnabled() {
        return m_isNewLineCharacterHandlingEnabled;
    }
    public void setIsNewLineCharacterHandlingEnabled(boolean value) {
        m_isNewLineCharacterHandlingEnabled = value;
    }
    public ArrayList<String> getLines() {
        return m_lines;
    }
    public StringBuilder getStringBuilder() {
        return m_stringBuilder;
    }

    public LineReaderState(int bufferSize, Charset charset) {
        final CharsetDecoder charsetDecoder = charset
            .newDecoder()
            .onMalformedInput(CodingErrorAction.REPORT)
            .onUnmappableCharacter(CodingErrorAction.REPORT);

        m_charsetDecoder = charsetDecoder;
        m_decodedBlock = CharBuffer.allocate((int)(((double)bufferSize) * charsetDecoder.maxCharsPerByte()));;
        m_encodedBlock = ByteBuffer.allocate(bufferSize);
        m_lines = new ArrayList<>();
        m_stringBuilder = new StringBuilder(137);

        setIsNewLineCharacterHandlingEnabled(false);
    }
}
