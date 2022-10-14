/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *  http://aws.amazon.com/apache2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package software.amazon.awssdk.services.s3.internal.crt;

import static software.amazon.awssdk.services.s3.internal.crt.CrtChecksumUtils.crtChecksumAlgorithm;
import static software.amazon.awssdk.services.s3.internal.crt.CrtChecksumUtils.validateResponseChecksum;
import static software.amazon.awssdk.services.s3.internal.crt.S3InternalSdkHttpExecutionAttribute.CRT_PAUSE_RESUME_TOKEN;
import static software.amazon.awssdk.services.s3.internal.crt.S3InternalSdkHttpExecutionAttribute.HTTP_CHECKSUM;
import static software.amazon.awssdk.services.s3.internal.crt.S3InternalSdkHttpExecutionAttribute.METAREQUEST_PAUSE_OBSERVABLE;
import static software.amazon.awssdk.services.s3.internal.crt.S3InternalSdkHttpExecutionAttribute.OPERATION_NAME;
import static software.amazon.awssdk.utils.FunctionalUtils.invokeSafely;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import software.amazon.awssdk.annotations.SdkInternalApi;
import software.amazon.awssdk.annotations.SdkTestInternalApi;
import software.amazon.awssdk.core.interceptor.trait.HttpChecksum;
import software.amazon.awssdk.crt.http.HttpHeader;
import software.amazon.awssdk.crt.http.HttpRequest;
import software.amazon.awssdk.crt.s3.ChecksumAlgorithm;
import software.amazon.awssdk.crt.s3.S3Client;
import software.amazon.awssdk.crt.s3.S3ClientOptions;
import software.amazon.awssdk.crt.s3.S3MetaRequest;
import software.amazon.awssdk.crt.s3.S3MetaRequestOptions;
import software.amazon.awssdk.http.Header;
import software.amazon.awssdk.http.SdkHttpRequest;
import software.amazon.awssdk.http.async.AsyncExecuteRequest;
import software.amazon.awssdk.http.async.SdkAsyncHttpClient;
import software.amazon.awssdk.utils.AttributeMap;
import software.amazon.awssdk.utils.Logger;

/**
 * An implementation of {@link SdkAsyncHttpClient} that uses an CRT S3 HTTP client {@link S3Client} to communicate with S3.
 * Note that it does not work with other services
 */
@SdkInternalApi
public final class S3CrtAsyncHttpClient implements SdkAsyncHttpClient {
    private static final long MAX_WINDOW_SIZE = 100 * 1024 * 1024; // 100 MB //TODO: Run some perf tests
    private static final Logger log = Logger.loggerFor(S3CrtAsyncHttpClient.class);

    private final S3Client crtS3Client;

    private final S3NativeClientConfiguration s3NativeClientConfiguration;

    private S3CrtAsyncHttpClient(Builder builder) {
        s3NativeClientConfiguration = builder.clientConfiguration;
        Long initialWindowSize = s3NativeClientConfiguration.readBufferSizeInBytes();

        log.info(() -> "initial window size " + initialWindowSize);
        S3ClientOptions s3ClientOptions =
            new S3ClientOptions().withRegion(s3NativeClientConfiguration.signingRegion())
                                 .withEndpoint(s3NativeClientConfiguration.endpointOverride() == null ? null :
                                               s3NativeClientConfiguration.endpointOverride().toString())
                                 .withCredentialsProvider(s3NativeClientConfiguration.credentialsProvider())
                                 .withClientBootstrap(s3NativeClientConfiguration.clientBootstrap())
                                 .withPartSize(s3NativeClientConfiguration.partSizeBytes())
                                 .withComputeContentMd5(false)
                                 .withThroughputTargetGbps(s3NativeClientConfiguration.targetThroughputInGbps());

        if (initialWindowSize != null) {
            s3ClientOptions.withInitialReadWindowSize(initialWindowSize);
            s3ClientOptions.withReadBackpressureEnabled(true);
        }
        this.crtS3Client = new S3Client(s3ClientOptions);
    }

    @SdkTestInternalApi
    S3CrtAsyncHttpClient(S3Client crtS3Client,
                         S3NativeClientConfiguration nativeClientConfiguration) {
        this.crtS3Client = crtS3Client;
        this.s3NativeClientConfiguration = nativeClientConfiguration;
    }

    @Override
    public CompletableFuture<Void> execute(AsyncExecuteRequest asyncRequest) {
        CompletableFuture<Void> executeFuture = new CompletableFuture<>();
        URI uri = asyncRequest.request().getUri();
        HttpRequest httpRequest = toCrtRequest(uri, asyncRequest);
        S3CrtResponseHandlerAdapter responseHandler =
            new S3CrtResponseHandlerAdapter(executeFuture, asyncRequest.responseHandler());

        S3MetaRequestOptions.MetaRequestType requestType = requestType(asyncRequest);

        HttpChecksum httpChecksum = asyncRequest.httpExecutionAttributes().getAttribute(HTTP_CHECKSUM);
        ChecksumAlgorithm checksumAlgorithm =
            crtChecksumAlgorithm(httpChecksum, requestType, s3NativeClientConfiguration.checksumValidationEnabled());

        boolean validateChecksum =
            validateResponseChecksum(httpChecksum, requestType,  s3NativeClientConfiguration.checksumValidationEnabled());
        String resumeToken = asyncRequest.httpExecutionAttributes().getAttribute(CRT_PAUSE_RESUME_TOKEN);

        S3MetaRequestOptions requestOptions = new S3MetaRequestOptions()
            .withHttpRequest(httpRequest)
            .withMetaRequestType(requestType)
            .withChecksumAlgorithm(checksumAlgorithm)
            .withValidateChecksum(validateChecksum)
            .withEndpoint(getEndpoint(uri))
            .withResponseHandler(responseHandler)
            .withResumeToken(resumeToken);

        S3MetaRequest s3MetaRequest = crtS3Client.makeMetaRequest(requestOptions);
        S3MetaRequestPauseObservable observable =
            asyncRequest.httpExecutionAttributes().getAttribute(METAREQUEST_PAUSE_OBSERVABLE);

        responseHandler.metaRequest(s3MetaRequest);

        if (observable != null) {
            observable.subscribe(s3MetaRequest);
        }
        addCancelCallback(executeFuture, s3MetaRequest, responseHandler);

        return executeFuture;
    }

    private static URI getEndpoint(URI uri) {
        return invokeSafely(() -> new URI(uri.getScheme(), null, uri.getHost(), uri.getPort(), null, null, null));
    }

    @Override
    public String clientName() {
        return "s3crt";
    }

    private static S3MetaRequestOptions.MetaRequestType requestType(AsyncExecuteRequest asyncRequest) {
        String operationName = asyncRequest.httpExecutionAttributes().getAttribute(OPERATION_NAME);
        if (operationName != null) {
            switch (operationName) {
                case "GetObject":
                    return S3MetaRequestOptions.MetaRequestType.GET_OBJECT;
                case "PutObject":
                    return S3MetaRequestOptions.MetaRequestType.PUT_OBJECT;
                default:
                    return S3MetaRequestOptions.MetaRequestType.DEFAULT;
            }
        }
        return S3MetaRequestOptions.MetaRequestType.DEFAULT;
    }

    private static void addCancelCallback(CompletableFuture<Void> executeFuture,
                                          S3MetaRequest s3MetaRequest,
                                          S3CrtResponseHandlerAdapter responseHandler) {
        executeFuture.whenComplete((r, t) -> {
            if (executeFuture.isCancelled()) {
                log.debug(() -> "The request is cancelled, cancelling meta request");
                responseHandler.cancelRequest();
                s3MetaRequest.cancel();
            }
        });
    }

    private static HttpRequest toCrtRequest(URI uri, AsyncExecuteRequest asyncRequest) {
        SdkHttpRequest sdkRequest = asyncRequest.request();

        String method = sdkRequest.method().name();
        String encodedPath = sdkRequest.encodedPath();
        if (encodedPath == null || encodedPath.isEmpty()) {
            encodedPath = "/";
        }

        String encodedQueryString = sdkRequest.encodedQueryParameters()
                                              .map(value -> "?" + value)
                                              .orElse("");

        HttpHeader[] crtHeaderArray = createHttpHeaderList(uri, asyncRequest).toArray(new HttpHeader[0]);

        S3CrtRequestBodyStreamAdapter sdkToCrtRequestPublisher =
            new S3CrtRequestBodyStreamAdapter(asyncRequest.requestContentPublisher());

        return new HttpRequest(method, encodedPath + encodedQueryString, crtHeaderArray, sdkToCrtRequestPublisher);
    }

    @Override
    public void close() {
        s3NativeClientConfiguration.close();
        crtS3Client.close();
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder implements SdkAsyncHttpClient.Builder<S3CrtAsyncHttpClient.Builder> {
        private S3NativeClientConfiguration clientConfiguration;

        public Builder s3ClientConfiguration(S3NativeClientConfiguration clientConfiguration) {
            this.clientConfiguration = clientConfiguration;
            return this;
        }

        @Override
        public SdkAsyncHttpClient build() {
            return new S3CrtAsyncHttpClient(this);
        }

        @Override
        public SdkAsyncHttpClient buildWithDefaults(AttributeMap serviceDefaults) {
            // Intentionally ignore serviceDefaults
            return build();
        }
    }

    private static List<HttpHeader> createHttpHeaderList(URI uri, AsyncExecuteRequest asyncRequest) {
        SdkHttpRequest sdkRequest = asyncRequest.request();
        List<HttpHeader> crtHeaderList = new ArrayList<>();

        // Set Host Header if needed
        if (!sdkRequest.firstMatchingHeader(Header.HOST).isPresent()) {
            crtHeaderList.add(new HttpHeader(Header.HOST, uri.getHost()));
        }

        // Set Content-Length if needed
        Optional<Long> contentLength = asyncRequest.requestContentPublisher().contentLength();
        if (!sdkRequest.firstMatchingHeader(Header.CONTENT_LENGTH).isPresent() && contentLength.isPresent()) {
            crtHeaderList.add(new HttpHeader(Header.CONTENT_LENGTH, Long.toString(contentLength.get())));
        }

        // Add the rest of the Headers
        sdkRequest.forEachHeader((key, value) -> value.stream().map(val -> new HttpHeader(key, val))
                                                      .forEach(crtHeaderList::add));

        return crtHeaderList;
    }
}
