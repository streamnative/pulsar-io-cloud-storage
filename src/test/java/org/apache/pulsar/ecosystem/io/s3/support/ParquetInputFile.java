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
package org.apache.pulsar.ecosystem.io.s3.support;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.apache.parquet.io.DelegatingSeekableInputStream;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.SeekableInputStream;

/**
 * Parquet input file.
 */
public class ParquetInputFile implements InputFile {
    private final String streamId;
    private final byte[] data;

    /**
     *  seekable input stream for byte array.
     */
    public static class SeekableByteArrayInputStream extends ByteArrayInputStream {
        public SeekableByteArrayInputStream(byte[] buf) {
            super(buf);
        }

        public void setPos(int pos) {
            this.pos = pos;
        }

        public int getPos() {
            return this.pos;
        }
    }

    public ParquetInputFile(String streamId, ByteArrayOutputStream stream) {
        this.streamId = streamId;
        this.data = stream.toByteArray();
    }

    @Override
    public long getLength() throws IOException {
        return this.data.length;
    }

    @Override
    public SeekableInputStream newStream() throws IOException {
        return new DelegatingSeekableInputStream(new SeekableByteArrayInputStream(this.data)) {

            @Override
            public void seek(long newPos) throws IOException {
                ((SeekableByteArrayInputStream) this.getStream()).setPos(new Long(newPos).intValue());
            }

            @Override
            public long getPos() throws IOException {
                return new Integer(((SeekableByteArrayInputStream) this.getStream()).getPos()).longValue();
            }
        };
    }

    @Override
    public String toString() {
        return new StringBuilder("ParquetStream[").append(streamId).append("]").toString();

    }
}