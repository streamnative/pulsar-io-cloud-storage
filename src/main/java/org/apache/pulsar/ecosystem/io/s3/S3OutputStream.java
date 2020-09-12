/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar.ecosystem.io.s3;

import java.io.IOException;

import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.parquet.io.PositionOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Output stream enabling multi-part uploads of Kafka records.
 *
 * <p>The implementation has borrowed the general structure of Hadoop's implementation.
 */
public class S3OutputStream extends PositionOutputStream {
    private static final Logger log = LoggerFactory.getLogger(S3OutputStream.class);

    private long position;

    private ByteArrayOutputStream byteArrayOutputStream;


    public S3OutputStream() {
        this.byteArrayOutputStream = new ByteArrayOutputStream();
        this.position = 0L;
    }

    @Override
    public void write(int b) throws IOException {
        byteArrayOutputStream.write(b);
        position += 1;
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        byteArrayOutputStream.write(b, off, len);
        position += len;
    }

    @Override
    public void close() throws IOException {
        position = 0L;
        byteArrayOutputStream.close();
    }

    @Override
    public long getPos() {
        return position;
    }

    public ByteArrayOutputStream getByteArrayOutputStream() {
        return byteArrayOutputStream;
    }
}
