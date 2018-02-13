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

package org.apache.solr.search;

import org.apache.lucene.search.DocValuesBooleanQuery;
import org.apache.lucene.search.Query;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.request.SolrQueryRequest;

/**
 * syntax fq={!bool field=store_1}
 */
public class BooleanDocValuesQParserPlugin extends QParserPlugin {

    public void init(NamedList params) {
    }

    public QParser createParser(String query, SolrParams localParams, SolrParams params, SolrQueryRequest request) {
        return new BooleanDocValuesQParser(query, localParams, params, request);
    }

    private class BooleanDocValuesQParser extends QParser {

        public BooleanDocValuesQParser(String query, SolrParams localParams, SolrParams params, SolrQueryRequest request) {
            super(query, localParams, params, request);
        }

        public Query parse() {
            String field = localParams.get("field");
            return new DocValuesBooleanQuery(field);
        }
    }

}
