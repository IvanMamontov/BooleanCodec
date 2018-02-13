package org.apache.lucene.codecs.bool;/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Writer for {@link BooleanDocValuesFormat}
 */

class BooleanDocValuesConsumer extends DocValuesConsumer {
    IndexOutput data, meta;
    final int maxDoc;

    BooleanDocValuesConsumer(SegmentWriteState state, String dataCodec, String dataExtension, String metaCodec, String metaExtension) throws IOException {
        maxDoc = state.segmentInfo.maxDoc();
        boolean success = false;
        try {
            String dataName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, dataExtension);
            data = state.directory.createOutput(dataName, state.context);
            CodecUtil.writeIndexHeader(data, dataCodec, BooleanDocValuesProducer.VERSION_CURRENT, state.segmentInfo.getId(), state.segmentSuffix);
            String metaName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, metaExtension);
            meta = state.directory.createOutput(metaName, state.context);
            CodecUtil.writeIndexHeader(meta, metaCodec, BooleanDocValuesProducer.VERSION_CURRENT, state.segmentInfo.getId(), state.segmentSuffix);
            success = true;
        } finally {
            if (!success) {
                IOUtils.closeWhileHandlingException(this);
            }
        }
    }

    @Override
    public void addNumericField(FieldInfo field, Iterable<Number> values) throws IOException {
        meta.writeVInt(field.number);
        meta.writeByte(BooleanDocValuesProducer.NUMBER);
        meta.writeLong(data.getFilePointer());

        long bits = 0;
        int count = 0;
        List<Long> longValues = new ArrayList<>();
        for (Number n : values) {
            if (count == Long.SIZE) {
                longValues.add(bits);
                count = 0;
                bits = 0;
            }
            if (n != null && n.byteValue() != 0) {
                bits |= 1L << count;
            }
            count++;
        }
        if (count > 0) {
            longValues.add(bits);
        }

        meta.writeInt(longValues.size()); // count

        for (long v : longValues) {
            data.writeLong(v);
        }
    }

    @Override
    public void close() throws IOException {
        boolean success = false;
        try {
            if (meta != null) {
                meta.writeVInt(-1); // write EOF marker
                CodecUtil.writeFooter(meta); // write checksum
            }
            if (data != null) {
                CodecUtil.writeFooter(data);
            }
            success = true;
        } finally {
            if (success) {
                IOUtils.close(data, meta);
            } else {
                IOUtils.closeWhileHandlingException(data, meta);
            }
            data = meta = null;
        }
    }

    @Override
    public void addBinaryField(FieldInfo field, Iterable<BytesRef> values) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void addSortedField(FieldInfo field, Iterable<BytesRef> values, Iterable<Number> docToOrd) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void addSortedNumericField(FieldInfo field, Iterable<Number> docToValueCount, Iterable<Number> values) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void addSortedSetField(FieldInfo field, Iterable<BytesRef> values, Iterable<Number> docToOrdCount, Iterable<Number> ords) throws IOException {
        throw new UnsupportedOperationException();
    }
}
