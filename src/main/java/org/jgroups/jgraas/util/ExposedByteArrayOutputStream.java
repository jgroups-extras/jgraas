package org.jgroups.jgraas.util;

import org.jgroups.util.ByteArray;

import java.io.ByteArrayOutputStream;

/**
 * Exposes buffer of {@link java.io.ByteArrayInputStream}, so we don't need to copy
 * @author Bela Ban
 * @since  1.0.0
 */
public class ExposedByteArrayOutputStream extends ByteArrayOutputStream {

    public ExposedByteArrayOutputStream() {
        super();
    }

    public ExposedByteArrayOutputStream(int size) {
        super(size);
    }

    public synchronized ByteArray getBuffer() {
        return new ByteArray(buf, 0, count);
    }
}
