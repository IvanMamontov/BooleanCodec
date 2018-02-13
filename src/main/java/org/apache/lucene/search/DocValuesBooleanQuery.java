/*
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
package org.apache.lucene.search;

import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;

import java.io.IOException;
import java.util.Objects;

/**
 * Like {@code DocValuesNumbersQuery}, but this query only
 * runs on a long {@link NumericDocValuesField}, matching
 * all documents whose value is {@code 1}. All documents with
 * {@code 0} and {@code null} values are considered as absent.
 * <p>
 *
 * @lucene.experimental
 */
public class DocValuesBooleanQuery extends Query {

    private final String field;

    public DocValuesBooleanQuery(String field) {
        this.field = Objects.requireNonNull(field);
    }

    @Override
    public int hashCode() {
        return Objects.hash(field);
    }

    @Override
    public String toString(String defaultField) {
        return "DocValuesBooleanQuery:" + field;
    }

    @Override
    public boolean equals(Object obj) {
        return false;
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, boolean needsScores) throws IOException {
        return new ConstantScoreWeight(this) {

            @Override
            public Scorer scorer(LeafReaderContext context) throws IOException {
                final NumericDocValues values = DocValues.getNumeric(context.reader(), field);
                final int maxDoc = context.reader().numDocs();
                final DocIdSetIterator iterator = new DocIdSetIterator() {

                    int docId = -1;

                    @Override
                    public int docID() {
                        return docId;
                    }

                    @Override
                    public int nextDoc() throws IOException {
                        return doNext(docId + 1);
                    }

                    @Override
                    public int advance(int target) throws IOException {
                        return doNext(target);
                    }

                    private int doNext(int doc) throws IOException {
                        for (int i = doc; i < maxDoc; i++) {
                            if (values.get(i) == 1) {
                                docId = i;
                                return i;
                            }
                        }
                        return DocIdSetIterator.NO_MORE_DOCS;
                    }

                    @Override
                    public long cost() {
                        return 5;
                    }
                };
                return new Scorer(this) {
                    @Override
                    public int docID() {
                        return iterator.docID();
                    }

                    @Override
                    public float score() throws IOException {
                        return 1;
                    }

                    @Override
                    public int freq() throws IOException {
                        return 1;
                    }

                    @Override
                    public DocIdSetIterator iterator() {
                        return iterator;
                    }
                };
            }
        };

    }
}
