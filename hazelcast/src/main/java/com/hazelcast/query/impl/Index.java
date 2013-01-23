/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.query.impl;

import com.hazelcast.nio.serialization.Data;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class Index {
    // recordKey -- indexValue
    private final ConcurrentMap<Data, Comparable> recordValues = new ConcurrentHashMap<Data, Comparable>(1000);
    // indexValue -- Map<recordKey, IndexEntry>
    private final IndexStore indexStore;
    private final String attribute;

    public static final NullObject NULL = new NullObject();
    private volatile TypeConverters.TypeConverter attributeTypeConverter;

    public Index(String attribute, boolean ordered) {
        this.attribute = attribute;
        indexStore = (ordered) ? new SortedIndexStore() : new UnsortedIndexStore();
    }

    public void removeIndex(QueryableEntry e) {
        Data key = e.getKeyData();
        Comparable oldValue = recordValues.remove(key);
        indexStore.removeIndex(oldValue, key);
    }

    public void saveIndex(QueryableEntry e) throws QueryException {
        Data key = e.getKeyData();
        Comparable oldValue = recordValues.remove(key);
        Comparable newValue = e.getAttribute(attribute);
        if (newValue == null) {
            newValue = NULL;
        }
        if (newValue.equals(oldValue)) return;
        recordValues.put(key, newValue);
        if (oldValue == null) {
            // new
            indexStore.newIndex(newValue, e);
        } else {
            // update
            indexStore.removeIndex(oldValue, key);
            indexStore.newIndex(newValue, e);
        }
        if (attributeTypeConverter == null) {
            attributeTypeConverter = e.getAttributeTypeConverter(attribute);
        }
    }

    public Set<QueryableEntry> getRecords(Comparable[] values) {
        if (values.length == 1) {
            return indexStore.getRecords(convert(values[0]));
        } else {
            Set<Comparable> convertedValues = new HashSet(values.length);
            for (Comparable value : values) {
                convertedValues.add(convert(value));
            }
            MultiResultSet results = new MultiResultSet();
            indexStore.getRecords(results, convertedValues);
            return results;
        }
    }

    public Set<QueryableEntry> getRecords(Comparable value) {
        return indexStore.getRecords(convert(value));
    }

    public Set<QueryableEntry> getSubRecordsBetween(Comparable from, Comparable to) {
        MultiResultSet results = new MultiResultSet();
        indexStore.getSubRecordsBetween(results, convert(from), convert(to));
        return results;
    }

    public Set<QueryableEntry> getSubRecords(ComparisonType comparisonType, Comparable searchedValue) {
        MultiResultSet results = new MultiResultSet();
        indexStore.getSubRecords(results, comparisonType, searchedValue);
        return results;
    }

    public Comparable convert(Comparable value) {
        if (attributeTypeConverter == null) return value;
        return attributeTypeConverter.convert(value);
    }

    final static class NullObject implements Comparable {

        public int compareTo(Object o) {
            if (o == this || o instanceof NullObject) return 0;
            return -1;
        }
    }
}