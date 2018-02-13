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
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.index.*;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.*;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.IntFunction;

/**
 * Reader for {@link BooleanDocValuesFormat}
 */

class BooleanDocValuesProducer extends DocValuesProducer {
  // metadata maps (just file pointers and minimal stuff)
  private final Map<String,BooleanEntry> booleans = new HashMap<String,BooleanEntry>();
  private final IndexInput data;

  // ram instances we have already loaded
  private final Map<String,BooleanRawValues> booleanInstances = new HashMap<String,BooleanRawValues>();

  private final int numEntries;

  private final int maxDoc;
  private final AtomicLong ramBytesUsed;
  private final int version;

  private final boolean merging;

  static final byte NUMBER = 0;

  static final int VERSION_START = 3;
  static final int VERSION_CURRENT = VERSION_START;

  // clone for merge: when merging we don't do any instances.put()s
  BooleanDocValuesProducer(BooleanDocValuesProducer original) throws IOException {
    assert Thread.holdsLock(original);
    booleans.putAll(original.booleans);
    data = original.data.clone();

    booleanInstances.putAll(original.booleanInstances);

    numEntries = original.numEntries;
    maxDoc = original.maxDoc;
    ramBytesUsed = new AtomicLong(original.ramBytesUsed.get());
    version = original.version;
    merging = true;
  }

  BooleanDocValuesProducer(SegmentReadState state, String dataCodec, String dataExtension, String metaCodec,
                           String metaExtension) throws IOException {
    maxDoc = state.segmentInfo.maxDoc();
    merging = false;
    String metaName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, metaExtension);
    // read in the entries from the metadata file.
    ChecksumIndexInput in = state.directory.openChecksumInput(metaName, state.context);
    ramBytesUsed = new AtomicLong(RamUsageEstimator.shallowSizeOfInstance(getClass()));
    boolean success = false;
    try {
      version = CodecUtil.checkIndexHeader(in, metaCodec, VERSION_START, VERSION_CURRENT,
          state.segmentInfo.getId(), state.segmentSuffix);
      numEntries = readFields(in, state.fieldInfos);

      CodecUtil.checkFooter(in);
      success = true;
    } finally {
      if (success) {
        IOUtils.close(in);
      } else {
        IOUtils.closeWhileHandlingException(in);
      }
    }

    String dataName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, dataExtension);
    this.data = state.directory.openInput(dataName, state.context);
    success = false;
    try {
      final int version2 = CodecUtil.checkIndexHeader(data, dataCodec, VERSION_START, VERSION_CURRENT,
          state.segmentInfo.getId(), state.segmentSuffix);
      if (version != version2) {
        throw new CorruptIndexException("Format versions mismatch: meta=" + version + ", data=" + version2, data);
      }

      // NOTE: data file is too costly to verify checksum against all the bytes on open,
      // but for now we at least verify proper structure of the checksum footer: which looks
      // for FOOTER_MAGIC + algorithmID. This is cheap and can detect some forms of corruption
      // such as file truncation.
      CodecUtil.retrieveChecksum(data);

      success = true;
    } finally {
      if (!success) {
        IOUtils.closeWhileHandlingException(this.data);
      }
    }
  }

  private BooleanEntry readBooleanEntry(IndexInput meta) throws IOException {
    BooleanEntry entry = new BooleanEntry();
    entry.offset = meta.readLong();
    entry.count = meta.readInt();
    return entry;
  }

  private int readFields(IndexInput meta, FieldInfos infos) throws IOException {
    int numEntries = 0;
    int fieldNumber = meta.readVInt();
    while (fieldNumber != -1) {
      numEntries++;
      FieldInfo info = infos.fieldInfo(fieldNumber);
      int fieldType = meta.readByte();
      if (fieldType == NUMBER) {
        booleans.put(info.name, readBooleanEntry(meta));
      } else {
        throw new CorruptIndexException("invalid entry type: " + fieldType + ", field= " + info.name, meta);
      }
      fieldNumber = meta.readVInt();
    }
    return numEntries;
  }

  @Override
  public long ramBytesUsed() {
    return ramBytesUsed.get();
  }

  @Override
  public synchronized Collection<Accountable> getChildResources() {
    List<Accountable> resources = new ArrayList<Accountable>();
    resources.addAll(Accountables.namedAccountables("numeric field", booleanInstances));
    return Collections.unmodifiableList(resources);
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + "(entries=" + numEntries + ")";
  }

  @Override
  public void checkIntegrity() throws IOException {
    CodecUtil.checksumEntireFile(data.clone());
  }

  @Override
  public synchronized NumericDocValues getNumeric(FieldInfo field) throws IOException {
    BooleanRawValues instance = booleanInstances.get(field.name);
    BooleanEntry ne = booleans.get(field.name);
    if (instance == null) {
      // Lazy load
      instance = loadBoolean(ne);
      if (!merging) {
        booleanInstances.put(field.name, instance);
        ramBytesUsed.addAndGet(instance.ramBytesUsed());
      }
    }
    return new BooleanDocValuesWrapper(instance.valuesFunc);
  }

  private BooleanRawValues loadBoolean(BooleanEntry entry) throws IOException {
    BooleanRawValues ret = new BooleanRawValues();
    IndexInput data = this.data.clone();
    data.seek(entry.offset);

    final long[] bits = new long[entry.count];
    for (int i = 0; i < entry.count; i++) {
      bits[i] = data.readLong();
    }
    ret.bytesUsed = RamUsageEstimator.sizeOf(bits);
    ret.valuesFunc = idx -> {
      int i = idx >> 6;
      if (i < bits.length) {
        return (bits[i] & (1L << idx)) != 0 ? 1 : 0;
      } else {
        return 0;
      }
    };
    return ret;
  }

  @Override
  public synchronized BinaryDocValues getBinary(FieldInfo field) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public SortedDocValues getSorted(FieldInfo field) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public synchronized SortedNumericDocValues getSortedNumeric(FieldInfo field) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public synchronized SortedSetDocValues getSortedSet(FieldInfo field) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public Bits getDocsWithField(FieldInfo fieldInfo) throws IOException {
    return new Bits.MatchAllBits(maxDoc);
  }

  @Override
  public synchronized DocValuesProducer getMergeInstance() throws IOException {
    return new BooleanDocValuesProducer(this);
  }

  @Override
  public void close() throws IOException {
    data.close();
  }

  static class BooleanEntry {
    long offset;
    int count;
  }

  static class BooleanRawValues implements Accountable {
    IntFunction<Integer> valuesFunc;
    long bytesUsed;

    @Override
    public long ramBytesUsed() {
      return bytesUsed;
    }

    @Override
    public String toString() {
      return getClass().getSimpleName();
    }
  }

  static class BooleanDocValuesWrapper extends NumericDocValues {
    private final IntFunction<Integer> valuesFunc;

    public BooleanDocValuesWrapper(IntFunction<Integer> valuesFunc) {
      this.valuesFunc = valuesFunc;
    }

    @Override
    public String toString() {
      return getClass().getSimpleName();
    }

    @Override
    public long get(int i) {
      return valuesFunc.apply(i);
    }
  }
}
