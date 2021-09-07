package com.byteterrace.azure.functions;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CoderResult;
import java.nio.charset.CodingErrorAction;
import java.util.function.BiFunction;
import java.util.function.Consumer;

public final class LineReaderState {
    public static BiFunction<LineReaderState, ByteBuffer, LineReaderState> createReducer() {
        return (state, input) -> {
            final CharsetDecoder charsetDecoder = state.getCharsetDecoder();
            final CharBuffer decodedBlock = state.getDecodedBlock();
            final ByteBuffer encodedBlock = state.getEncodedBlock();
            final Consumer<String> lineConsumer = state.getLineConsumer();
            final StringBuilder stringBuilder = state.getStringBuilder();

            boolean isAdditionalDecodingRequired = false;
            boolean isNewLineCharacterHandlingEnabled = state.getIsNewLineCharacterHandlingEnabled();

            encodedBlock.put(input);
            encodedBlock.flip();

            do {
                try {
                    final CoderResult decodeResult = charsetDecoder.decode(encodedBlock, decodedBlock, false);

                    isAdditionalDecodingRequired = CoderResult.OVERFLOW.equals(decodeResult);

                    if (CoderResult.UNDERFLOW.equals(decodeResult) || isAdditionalDecodingRequired) {
                        decodedBlock.flip();

                        final char[] a = decodedBlock.array();
                        final int l = decodedBlock.limit();

                        int o = 0;
                        int p = decodedBlock.position();

                        if (o < l) {
                            do {
                                final int c = a[o++];

                                if ('\r' != c) {
                                    if (('\n' == c) && isNewLineCharacterHandlingEnabled) {
                                        stringBuilder.append(a, p, ((o - p) - 1));
                                        lineConsumer.accept(stringBuilder.toString());
                                        stringBuilder.setLength(0);
                                        p = o;
                                    }

                                    isNewLineCharacterHandlingEnabled = true;
                                }
                                else {
                                    stringBuilder.append(a, p, ((o - p) - 1));
                                    lineConsumer.accept(stringBuilder.toString());
                                    stringBuilder.setLength(0);
                                    p = o;
                                    isNewLineCharacterHandlingEnabled = false;
                                }
                            } while (o < l);
                        }

                        stringBuilder.append(a, p, (l - p));
                        decodedBlock.clear();
                    }
                    else {
                        decodeResult.throwException();
                    }
                }
                catch (final CharacterCodingException e) {
                    throw new UncheckedCharacterCodingException(e);
                }
            } while (isAdditionalDecodingRequired);

            encodedBlock.compact();
            state.setIsNewLineCharacterHandlingEnabled(isNewLineCharacterHandlingEnabled);

            return state;
        };
    }

    private final CharsetDecoder m_charsetDecoder;
    private final CharBuffer m_decodedBlock;
    private final ByteBuffer m_encodedBlock;
    private final Consumer<String> m_lineConsumer;
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
    public Consumer<String> getLineConsumer() {
        return m_lineConsumer;
    }
    public StringBuilder getStringBuilder() {
        return m_stringBuilder;
    }

    public LineReaderState(int bufferSize, Charset charset, Consumer<String> lineConsumer) {
        final CharsetDecoder charsetDecoder = charset
            .newDecoder()
            .onMalformedInput(CodingErrorAction.REPORT)
            .onUnmappableCharacter(CodingErrorAction.REPORT);

        m_charsetDecoder = charsetDecoder;
        m_decodedBlock = CharBuffer.allocate((int)(((double)bufferSize) * charsetDecoder.maxCharsPerByte()));;
        m_encodedBlock = ByteBuffer.allocate(bufferSize);
        m_lineConsumer = lineConsumer;
        m_stringBuilder = new StringBuilder(137);

        setIsNewLineCharacterHandlingEnabled(false);
    }
}
