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
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.StandardCharsets;
import java.util.function.Consumer;
import java.util.function.Function;
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
    private static final String StorageAccountName = "byteterrace";
    private static final String StorageAccountEndpoint = String.format(Locale.ROOT, "https://%s.blob.core.windows.net", StorageAccountName);
    private static final Charset Utf8Charset = StandardCharsets.UTF_8;
    private static final TokenCredential TokenCredential = new DefaultAzureCredentialBuilder().build();

    private static Function<Flux<ByteBuffer>, Mono<LineReaderState>> createLineReader(int bufferSize, Charset charset, Consumer<String> lineConsumer) {
        return (buffer) -> buffer
            .reduce(new LineReaderState(bufferSize, charset, lineConsumer), LineReaderState.createReducer())
            .map(state -> {
                final CharsetDecoder charsetDecoder = state.getCharsetDecoder();
                final CharBuffer finalDecodedBlock = state.getDecodedBlock();
                final ByteBuffer finalEncodedBlock = state.getEncodedBlock();
                final StringBuilder stringBuilder = state.getStringBuilder();

                finalEncodedBlock.flip();
                charsetDecoder.decode(finalEncodedBlock, finalDecodedBlock, true); // TODO: Handle any potential errors.
                charsetDecoder.flush(finalDecodedBlock); // TODO: Handle any potential errors.
                finalDecodedBlock.flip();

                final int l = finalDecodedBlock.limit();
                final int p = finalDecodedBlock.position();

                stringBuilder.append(finalDecodedBlock.array(), p, (l - p));
                finalDecodedBlock.position(l);
                state.emitLine();
                stringBuilder.setLength(0);

                return state;
            });
    }
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
            final Disposable processBlobOperation = blobClient
                .downloadStream()
                .transform(createLineReader(16384, Utf8Charset, logger::info))
                .doFinally(signalType -> {
                    logger.info("Processed blob: " + event.subject);
                })
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
        }
    }
}
