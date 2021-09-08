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
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.function.Function;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicBoolean;
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
        final LineReaderState lineReaderState = new LineReaderState(bufferSize, charset);

        return (buffer) -> {
            return buffer
                .flatMapIterable(lineReaderState::processBufferCore)
                .then(Mono.just(lineReaderState))
                .flatMapIterable(state -> state.processBufferFinal());
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
            final AtomicBoolean isWindowClosed = new AtomicBoolean(true); // TODO: Research whether this is a terrible idea or not...

            final Disposable processBlobOperation = blobClient
                .downloadStream()
                .transform(createLineReader(16384, DefaultCharset))
                .windowUntil(line -> {
                    if ((line.codePoints().filter(codePoint -> (codePoint == '"')).count() & 1) == 1) {
                        if (!isWindowClosed.get()) {
                            isWindowClosed.set(true);

                            return true;
                        }
                        else {
                            isWindowClosed.set(false);

                            return false;
                        }
                    }

                    return isWindowClosed.get();
                })
                .flatMap(chunks -> chunks.reduce("", (x, y) -> (x + y)))
                .doOnNext(logger::info)
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
