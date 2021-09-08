package com.byteterrace.azure.functions;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CoderResult;
import java.nio.charset.CodingErrorAction;
import java.util.ArrayList;

public final class LineReaderState {
    private final CharsetDecoder m_charsetDecoder;
    private final CharBuffer m_decodedBlock;
    private final ByteBuffer m_encodedBlock;
    private final ArrayList<String> m_lines;
    private final StringBuilder m_stringBuilder;

    private boolean m_isNewLineCharacterHandlingEnabled;

    public LineReaderState(int bufferSize, Charset charset) {
        final CharsetDecoder charsetDecoder = charset
            .newDecoder()
            .onMalformedInput(CodingErrorAction.REPORT)
            .onUnmappableCharacter(CodingErrorAction.REPORT);

        m_charsetDecoder = charsetDecoder;
        m_decodedBlock = CharBuffer.allocate((int)(((double)bufferSize) * charsetDecoder.maxCharsPerByte()));;
        m_encodedBlock = ByteBuffer.allocate(bufferSize);
        m_isNewLineCharacterHandlingEnabled = true;
        m_lines = new ArrayList<>();
        m_stringBuilder = new StringBuilder(137);
    }

    public ArrayList<String> processBufferCore(ByteBuffer buffer) {
        final CharsetDecoder charsetDecoder = m_charsetDecoder;
        final CharBuffer decodedBlock = m_decodedBlock;
        final ByteBuffer encodedBlock = m_encodedBlock;
        final ArrayList<String> lines = m_lines;
        final StringBuilder stringBuilder = m_stringBuilder;

        boolean isAdditionalDecodingRequired = true;

        encodedBlock.put(buffer);
        encodedBlock.flip();
        lines.clear();

        if (isAdditionalDecodingRequired) {
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
                                final char c = a[o++];

                                if (('\n' == c) || ('\r' == c)) {
                                    if (m_isNewLineCharacterHandlingEnabled) {
                                        stringBuilder.append(decodedBlock.array(), p, ((o - p) - 1));
                                        lines.add(stringBuilder.toString());
                                        stringBuilder.setLength(0);
                                    }

                                    p = o;
                                }

                                m_isNewLineCharacterHandlingEnabled = ('\r' != c);
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
        }

        encodedBlock.compact();

        return m_lines;
    }
    public ArrayList<String> processBufferFinal() {
        final CharsetDecoder charsetDecoder = m_charsetDecoder;
        final CharBuffer finalDecodedBlock = m_decodedBlock;
        final ByteBuffer finalEncodedBlock = m_encodedBlock;
        final ArrayList<String> lines = m_lines;
        final StringBuilder stringBuilder = m_stringBuilder;

        finalEncodedBlock.flip();
        charsetDecoder.decode(finalEncodedBlock, finalDecodedBlock, true); // TODO: Handle any potential errors.
        charsetDecoder.flush(finalDecodedBlock); // TODO: Handle any potential errors.
        finalDecodedBlock.flip();

        final int l = finalDecodedBlock.limit();
        final int p = finalDecodedBlock.position();

        stringBuilder.append(finalDecodedBlock.array(), p, (l - p));
        lines.add(stringBuilder.toString());
        stringBuilder.setLength(0);
        finalDecodedBlock.position(l);

        return lines;
    }
}
