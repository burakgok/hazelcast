/*
 * Copyright 2023 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.sql.impl.inject;

import com.hazelcast.internal.serialization.impl.compact.DeserializedSchemaBoundGenericRecordBuilder;
import com.hazelcast.internal.serialization.impl.compact.Schema;
import com.hazelcast.nio.serialization.FieldKind;
import com.hazelcast.nio.serialization.genericrecord.GenericRecord;
import com.hazelcast.nio.serialization.genericrecord.GenericRecordBuilder;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.expression.RowValue;
import com.hazelcast.sql.impl.type.QueryDataType;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;

import static com.hazelcast.jet.sql.impl.inject.UpsertInjector.FAILING_TOP_LEVEL_INJECTOR;
import static com.hazelcast.jet.sql.impl.inject.UpsertTargetUtils.convertRowToCompactType;

@NotThreadSafe
class CompactUpsertTarget implements UpsertTarget {
    private final Schema schema;

    private GenericRecordBuilder record;

    CompactUpsertTarget(Schema schema) {
        this.schema = schema;
    }

    @Override
    @SuppressWarnings("checkstyle:ReturnCount")
    public UpsertInjector createInjector(@Nullable String path, QueryDataType type) {
        if (path == null) {
            return FAILING_TOP_LEVEL_INJECTOR;
        }
        if (!schema.hasField(path)) {
            return value -> {
                throw QueryException.error("Field \"" + path + "\" doesn't exist in Compact schema");
            };
        }

        FieldKind kind = schema.getField(path).getKind();
        switch (kind) {
            case STRING:
                return value -> record.setString(path, (String) value);
            case NULLABLE_BOOLEAN:
                return value -> record.setNullableBoolean(path, (Boolean) value);
            case NULLABLE_INT8:
                return value -> record.setNullableInt8(path, (Byte) value);
            case NULLABLE_INT16:
                return value -> record.setNullableInt16(path, (Short) value);
            case NULLABLE_INT32:
                return value -> record.setNullableInt32(path, (Integer) value);
            case NULLABLE_INT64:
                return value -> record.setNullableInt64(path, (Long) value);
            case DECIMAL:
                return value -> record.setDecimal(path, (BigDecimal) value);
            case NULLABLE_FLOAT32:
                return value -> record.setNullableFloat32(path, (Float) value);
            case NULLABLE_FLOAT64:
                return value -> record.setNullableFloat64(path, (Double) value);
            case TIME:
                return value -> record.setTime(path, (LocalTime) value);
            case DATE:
                return value -> record.setDate(path, (LocalDate) value);
            case TIMESTAMP:
                return value -> record.setTimestamp(path, (LocalDateTime) value);
            case TIMESTAMP_WITH_TIMEZONE:
                return value -> record.setTimestampWithTimezone(path, (OffsetDateTime) value);
            case COMPACT:
                return value -> record.setGenericRecord(path, convertRowToCompactType((RowValue) value, type));
            default:
                throw QueryException.error(kind + " kind is not supported in SQL with Compact format!");
        }
    }

    @Override
    public void init() {
        record = new DeserializedSchemaBoundGenericRecordBuilder(schema);
    }

    @Override
    public Object conclude() {
        GenericRecord record = this.record.build();
        this.record = null;
        return record;
    }
}
