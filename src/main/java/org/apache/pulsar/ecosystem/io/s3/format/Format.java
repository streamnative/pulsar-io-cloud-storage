package org.apache.pulsar.ecosystem.io.s3.format;

import com.google.common.io.ByteSource;

public interface Format<C, T> {
    /**
     * get format extension
     *
     * @return format extension
     */
    String getExtension();

    ByteSource recordWriter(C config, T record);
}
