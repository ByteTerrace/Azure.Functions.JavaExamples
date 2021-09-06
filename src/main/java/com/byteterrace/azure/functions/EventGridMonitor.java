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
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.Locale;
import java.util.logging.Logger;
import reactor.core.Disposable;

/*
    http://localhost:7071/runtime/webhooks/EventGrid?functionName=EventGridMonitor
 */
public class EventGridMonitor {
    private static final int BufferSize = 16384;
    private static final String StorageAccountName = "byteterrace";
    private static final String StorageAccountEndpoint = String.format(Locale.ROOT, "https://%s.blob.core.windows.net", StorageAccountName);
    private static final Charset Utf8Charset = StandardCharsets.UTF_8;
    private static final TokenCredential TokenCredential = new DefaultAzureCredentialBuilder().build();

    private static int nthIndexOf(final String s, final char c, final int n) {
        int count = 0;
        int index = -1;

        do {
            index = s.indexOf(c, ++index);

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
        final String blobPath = eventSubject.substring(nthIndexOf(eventSubject, '/', 2) + 1);
        final BlobServiceAsyncClient blobServiceClient = new BlobServiceClientBuilder()
            .credential(TokenCredential)
            .endpoint(StorageAccountEndpoint)
            .buildAsyncClient();
        final BlobContainerAsyncClient containerClient = blobServiceClient.getBlobContainerAsyncClient(containerName);
        final BlobAsyncClient blobClient = containerClient.getBlobAsyncClient(blobPath);
        final BlobLeaseAsyncClient blobLeaseClient = new BlobLeaseClientBuilder()
            .blobAsyncClient(blobClient)
            .buildAsyncClient();
        final String blobLeaseId = blobLeaseClient
            .acquireLease(-1)
            .block();

        logger.info("Blob lease id: " + blobLeaseId);

        try {
            final CharsetDecoder utf8Decoder = Utf8Charset
                .newDecoder()
                .onMalformedInput(CodingErrorAction.REPORT)
                .onUnmappableCharacter(CodingErrorAction.REPORT);
            final CharBuffer decodedBlock = CharBuffer.allocate((int)(((double)BufferSize) * utf8Decoder.maxCharsPerByte()));
            final Disposable processBlobOperation = blobClient
                .downloadStream()
                .doFinally(signalType -> {
                    logger.info("Processed blob: " + event.subject);
                })
                .reduce(new LineReaderState(ByteBuffer.allocate(BufferSize)), (state, input) -> {
                    final ByteBuffer encodedBlock = state.getBuffer();
                    final StringBuilder stringBuilder = state.getStringBuilder();

                    boolean isAdditionalDecodingRequired = false;
                    boolean isNewLineCharacterHandlingEnabled = state.getIsPreviousCharacterCarriageReturn();

                    do {
                        encodedBlock.put(input);
                        encodedBlock.flip();

                        try {
                            final CoderResult decodeResult = utf8Decoder.decode(encodedBlock, decodedBlock, false);

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
                                                p = o;
                                                stringBuilder.toString(); // TODO: Add logic that processes each intermediate line.
                                                stringBuilder.setLength(0);
                                            }

                                            isNewLineCharacterHandlingEnabled = true;
                                        }
                                        else {
                                            stringBuilder.append(a, p, ((o - p) - 1));
                                            p = o;
                                            stringBuilder.toString(); // TODO: Add logic that processes each intermediate line.
                                            stringBuilder.setLength(0);
                                            isNewLineCharacterHandlingEnabled = false;
                                        }
                                    } while (o < l);
                                }

                                stringBuilder.append(a, p, (l - p));
                                decodedBlock.position(l);
                                encodedBlock.compact();
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

                    state.setIsPreviousCharacterCarriageReturn(isNewLineCharacterHandlingEnabled);

                    return state;
                })
                .subscribe(state -> {
                    final ByteBuffer finalEncodedBlock = state.getBuffer();
                    final StringBuilder stringBuilder = state.getStringBuilder();

                    finalEncodedBlock.flip();
                    utf8Decoder.decode(finalEncodedBlock, decodedBlock, true);
                    utf8Decoder.flush(decodedBlock);
                    decodedBlock.flip();

                    final int l = decodedBlock.limit();
                    final int p = decodedBlock.position();

                    stringBuilder.append(decodedBlock.array(), p, (l - p));
                    decodedBlock.position(l);
                    stringBuilder.toString(); // TODO: Add logic that processes the final line.
                    stringBuilder.setLength(0);
                });

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
        }
    }
}