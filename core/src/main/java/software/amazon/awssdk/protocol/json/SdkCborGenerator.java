/*
 * Copyright 2010-2017 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package software.amazon.awssdk.protocol.json;

import com.fasterxml.jackson.core.JsonFactory;
import software.amazon.awssdk.annotation.SdkInternalApi;

/**
 * Thin wrapper around Jackson's JSON generator for CBOR.
 */
@SdkInternalApi
class SdkCborGenerator extends SdkJsonGenerator {

    private static final int CBOR_TAG_TIMESTAP = 1;

    public SdkCborGenerator(JsonFactory factory, String contentType) {
        super(factory, contentType);
    }
}
