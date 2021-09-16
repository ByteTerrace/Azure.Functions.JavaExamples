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
import com.microsoft.sqlserver.jdbc.ISQLServerBulkData;
import com.microsoft.sqlserver.jdbc.SQLServerBulkCopy;
import com.microsoft.sqlserver.jdbc.SQLServerBulkCopyOptions;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.function.Function;
import java.util.List;
import java.util.Locale;
import java.util.logging.Logger;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.util.function.Tuples;

/*
    http://localhost:7071/runtime/webhooks/EventGrid?functionName=EventGridMonitor

    FUNCTIONS_WORKER_JAVA_LOAD_APP_LIBS = 1
 */
public class EventGridMonitor {
    private static final Charset DefaultCharset = StandardCharsets.UTF_8;
    private static final Locale DefaultLocale = Locale.ROOT;
    private static final TokenCredential TokenCredential = new DefaultAzureCredentialBuilder().build();

    private static String appendFieldChunk(final String fieldValue, final String chunkValue, final int chunkStartIndex, final int chunkLength) {
        if (0 < chunkLength) {
            return (fieldValue + chunkValue.substring(chunkStartIndex, (chunkStartIndex + chunkLength)));
        }
        else {
            return fieldValue;
        }
    }
    private static void appendFieldValue(final List<String> fieldValues, final String fieldValue, final char escapeChar) {
        fieldValues.add(((1 < fieldValue.length()) || ((0 < fieldValue.length()) && (fieldValue.charAt(0) != escapeChar)) ? fieldValue : null));
    }
    public static void bulkInsertMsSql(final String connectionString, final String userName, final String userPassword, final String tableName, final SQLServerBulkCopyOptions bulkCopyOptions, final ISQLServerBulkData records) throws ClassNotFoundException, SQLException {
        final String driverClassName = "com.microsoft.sqlserver.jdbc.SQLServerDriver";

        try (
            final Connection dbConnection = openConnection(driverClassName, connectionString, userName, userPassword);
            final SQLServerBulkCopy dbBulkCopy = new SQLServerBulkCopy(dbConnection);
        ) {
            dbBulkCopy.setBulkCopyOptions(bulkCopyOptions);
            dbBulkCopy.setDestinationTableName(tableName);
            dbBulkCopy.writeToServer(records);
        }
    }
    private static Function<Flux<String>, Flux<List<String>>> createCsvReader() {
        final Sinks.Many<Boolean> isWindowClosedSink = Sinks
            .many()
            .unicast()
            .onBackpressureBuffer();

        isWindowClosedSink.tryEmitNext(true);

        return (lines) -> {
            return lines
                .withLatestFrom(isWindowClosedSink.asFlux(), Tuples::of)
                .windowUntil(t -> {
                    Boolean isWindowClosed = t.getT2();

                    if ((t.getT1().codePoints().filter(codePoint -> (codePoint == '"')).count() & 1) == 1) {
                        isWindowClosed = !isWindowClosed;

                        isWindowClosedSink.tryEmitNext(isWindowClosed);
                    }

                    return isWindowClosed;
                })
                .flatMap(chunks -> chunks.reduce("", (x, y) -> (x + y.getT1())))
                .map(line -> split(',', '"', line))
                .doFinally(signalType -> isWindowClosedSink.tryEmitComplete());
        };
    }
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
    public static Connection openConnection(final String driverClassName, final String connectionString, final String userName, final String password) throws ClassNotFoundException, SQLException {
        Class.forName(driverClassName);
        return DriverManager.getConnection(connectionString, userName, password);
    }
    private static List<String> split(final char delimitChar, final char quoteChar, final String lineValue) {
        if (null != lineValue) {
            final int lineLength = lineValue.length();
            final List<String> recordValue = new ArrayList<>();

            int fieldHead = 0;
            int fieldTail = 0;
            String fieldValue = "";
            boolean isQuoted = false;

            while (fieldTail < lineLength) {
                char currentChar = lineValue.charAt(fieldTail++);

                if (currentChar == quoteChar) {
                    fieldValue = appendFieldChunk(fieldValue, lineValue, fieldHead, (fieldTail - fieldHead - 1));

                    if ((fieldTail < lineLength) && (lineValue.charAt(fieldTail) == quoteChar)) {
                        fieldValue += quoteChar;
                        fieldTail++;
                    }
                    else {
                        isQuoted = !isQuoted;
                    }

                    fieldHead = fieldTail;
                }

                if (!isQuoted && (currentChar == delimitChar)) {
                    fieldValue = appendFieldChunk(fieldValue, lineValue, fieldHead, (fieldTail - fieldHead - 1));
                    appendFieldValue(recordValue, fieldValue, quoteChar);

                    fieldHead = fieldTail;
                    fieldValue = "";
                }
            }

            fieldValue = appendFieldChunk(fieldValue, lineValue, fieldHead, (fieldTail - fieldHead));
            appendFieldValue(recordValue, fieldValue, quoteChar);

            return recordValue;
        }
        else {
            return null;
        }
    }

    private static Function<Flux<List<String>>, Flux<SomePojo>> mapPojo() {
        return (rows) -> {
            return rows.map(SomePojo::new);
        };
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
            final Flux<List<String>> csvRows = blobClient
                .downloadStream()
                .transform(createLineReader(16384, DefaultCharset))
                .transform(createCsvReader());

            final SqlServerBulkDataT sqlServerBulkDataT = new SqlServerBulkDataT(csvRows.toIterable());

            bulkInsertMsSql(null, null, null, null, sqlServerBulkDataT);

            //do {
            //    Thread.sleep(743);
            //}
            //while (!processBlobOperation.isDisposed());
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
