package com.byteterrace.azure.functions;

import com.azure.core.credential.TokenCredential;
import com.azure.identity.DefaultAzureCredentialBuilder;
import com.azure.storage.blob.BlobAsyncClient;
import com.azure.storage.blob.BlobContainerAsyncClient;
import com.azure.storage.blob.BlobServiceAsyncClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.specialized.BlobLeaseAsyncClient;
import com.azure.storage.blob.specialized.BlobLeaseClientBuilder;
import com.microsoft.azure.functions.annotation.FunctionName;
import com.microsoft.azure.functions.annotation.EventGridTrigger;
import com.microsoft.azure.functions.ExecutionContext;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CoderResult;
import java.nio.charset.StandardCharsets;
import java.util.function.Function;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.logging.Logger;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/*
    http://localhost:7071/runtime/webhooks/EventGrid?functionName=EventGridMonitor

    FUNCTIONS_WORKER_JAVA_LOAD_APP_LIBS = 1
 */
public class EventGridMonitor {
    private static final Charset DefaultCharset = StandardCharsets.UTF_8;
    private static final Locale DefaultLocale = Locale.ROOT;
    private static final TokenCredential TokenCredential = new DefaultAzureCredentialBuilder().build();

    private static Function<Flux<ByteBuffer>, Flux<String>> createLineReader(int bufferSize, Charset charset) {
        final Mono<LineReaderState> lineReaderState = Mono.just(new LineReaderState(bufferSize, charset));

        return (buffer) -> {
            return buffer
                .flatMap(input -> lineReaderState.map(state -> {
                    final CharsetDecoder charsetDecoder = state.getCharsetDecoder();
                    final CharBuffer decodedBlock = state.getDecodedBlock();
                    final ByteBuffer encodedBlock = state.getEncodedBlock();
                    final ArrayList<String> lines = state.getLines();
                    final StringBuilder stringBuilder = state.getStringBuilder();

                    boolean isAdditionalDecodingRequired = false;
                    boolean isNewLineCharacterHandlingEnabled = state.getIsNewLineCharacterHandlingEnabled();

                    encodedBlock.put(input);
                    encodedBlock.flip();
                    lines.clear();

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

                                        if (('\n' == c) || ('\r' == c)) {
                                            if (isNewLineCharacterHandlingEnabled) {
                                                stringBuilder.append(a, p, ((o - p) - 1));
                                                lines.add(stringBuilder.toString());
                                                stringBuilder.setLength(0);
                                            }

                                            p = o;
                                        }

                                        isNewLineCharacterHandlingEnabled = ('\r' != c);
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
                }))
                .flatMapIterable(state -> state.getLines())
                .then(lineReaderState.map(state -> {
                    final CharsetDecoder charsetDecoder = state.getCharsetDecoder();
                    final CharBuffer finalDecodedBlock = state.getDecodedBlock();
                    final ByteBuffer finalEncodedBlock = state.getEncodedBlock();
                    final List<String> lines = state.getLines();
                    final StringBuilder stringBuilder = state.getStringBuilder();

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

                    return state;
                }))
                .flatMapIterable(state -> state.getLines());
        };
    }
    private static int nthIndexOf(final String value, final char charToFind, final int n) {
        int count = 0;
        int index = -1;

        do {
            index = value.indexOf(charToFind, ++index);

            if (-1 == index) {
                break;
            }
        } while (++count < n);

        return index;
    }

    @FunctionName("EventGridMonitor")
    public void run(
        final ExecutionContext context,
        @EventGridTrigger(
            name = "event"
        )
        final EventSchema event
    ) {
        final Logger logger = context.getLogger();
        final String eventSubject = event.subject.substring(nthIndexOf(event.subject, '/', 4) + 1);
        final String containerName = eventSubject.split("/")[0];
        final String storageAccountName = event.topic.substring(nthIndexOf(event.topic, '/', 8) + 1);
        final String blobPath = eventSubject.substring(nthIndexOf(eventSubject, '/', 2) + 1);
        final BlobServiceAsyncClient blobServiceClient = new BlobServiceClientBuilder()
            .credential(TokenCredential)
            .endpoint(String.format(DefaultLocale, "https://%s.blob.core.windows.net", storageAccountName))
            .buildAsyncClient();
        final BlobContainerAsyncClient containerClient = blobServiceClient.getBlobContainerAsyncClient(containerName);
        final BlobAsyncClient blobClient = containerClient.getBlobAsyncClient(blobPath);
        final BlobLeaseAsyncClient blobLeaseClient = new BlobLeaseClientBuilder()
            .blobAsyncClient(blobClient)
            .buildAsyncClient();
        final String blobLeaseId = blobLeaseClient
            .acquireLease(-1)
            .block();

        logger.info(String.format(DefaultLocale, "Blob lease acquired (id: %s).", blobLeaseId));

        try {
            final Disposable processBlobOperation = blobClient
                .downloadStream()
                .transform(createLineReader(16384, DefaultCharset))
                .subscribe();

            do {
                Thread.sleep(743);
            }
            while (!processBlobOperation.isDisposed());
        }
        catch (final InterruptedException e) {
            throw new UncheckedInterruptedException(e);
        }
        finally {
            blobLeaseClient
                .releaseLease()
                .block();
            logger.info(String.format(DefaultLocale, "Blob lease released (id: %s).", blobLeaseId));
        }
    }
}
