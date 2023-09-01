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

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.TreeNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.type.QueryDataType;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;

import static com.hazelcast.internal.util.ExceptionUtil.sneakyThrow;
import static com.hazelcast.jet.sql.impl.inject.UpsertInjector.FAILING_TOP_LEVEL_INJECTOR;
import static com.hazelcast.sql.impl.type.QueryDataType.BIGINT;
import static com.hazelcast.sql.impl.type.QueryDataType.BOOLEAN;
import static com.hazelcast.sql.impl.type.QueryDataType.DOUBLE;
import static com.hazelcast.sql.impl.type.QueryDataType.INT;
import static com.hazelcast.sql.impl.type.QueryDataType.REAL;
import static com.hazelcast.sql.impl.type.QueryDataType.SMALLINT;
import static com.hazelcast.sql.impl.type.QueryDataType.TINYINT;
import static com.hazelcast.sql.impl.type.QueryDataType.VARCHAR;

@NotThreadSafe
class JsonUpsertTarget extends UpsertTarget {
    private static final JsonFactory JSON_FACTORY = new ObjectMapper().getFactory();

    private final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    private JsonGenerator json;

    JsonUpsertTarget(InternalSerializationService serializationService) {
        super(serializationService);
    }

    @Override
    public UpsertInjector createInjector(@Nullable String path, QueryDataType type) {
        if (path == null) {
            return FAILING_TOP_LEVEL_INJECTOR;
        }
        Injector<JsonGenerator> injector = createInjector0(path, type);
        return value -> injector.set(json, value);
    }

    private Injector<JsonGenerator> createInjector0(String path, QueryDataType type) {
        InjectorEx<JsonGenerator> injector = createInjector1(path, type);
        return (json, value) -> {
            try {
                if (value == null) {
                    json.writeNullField(path);
                } else {
                    injector.set(json, value);
                }
            } catch (Exception e) {
                throw sneakyThrow(e);
            }
        };
    }

    @SuppressWarnings("ReturnCount")
    private InjectorEx<JsonGenerator> createInjector1(String path, QueryDataType type) {
        switch (type.getTypeFamily()) {
            case BOOLEAN:
                return (json, value) -> json.writeBooleanField(path, (boolean) BOOLEAN.convert(value));
            case TINYINT:
                return (json, value) -> json.writeNumberField(path, (byte) TINYINT.convert(value));
            case SMALLINT:
                return (json, value) -> json.writeNumberField(path, (short) SMALLINT.convert(value));
            case INTEGER:
                return (json, value) -> json.writeNumberField(path, (int) INT.convert(value));
            case BIGINT:
                return (json, value) -> json.writeNumberField(path, (long) BIGINT.convert(value));
            case REAL:
                return (json, value) -> json.writeNumberField(path, (float) REAL.convert(value));
            case DOUBLE:
                return (json, value) -> json.writeNumberField(path, (double) DOUBLE.convert(value));
            case DECIMAL:
            case TIME:
            case DATE:
            case TIMESTAMP:
            case TIMESTAMP_WITH_TIME_ZONE:
            case VARCHAR:
                return (json, value) -> json.writeStringField(path, (String) VARCHAR.convert(value));
            case OBJECT:
                return (json, value) -> {
                    json.writeFieldName(path);
                    if (value instanceof TreeNode) {
                        json.writeTree((TreeNode) value);
                    } else if (value instanceof Map) {
                        json.writeObject(value);
                    } else if (value instanceof Boolean) {
                        json.writeBoolean((boolean) value);
                    } else if (value instanceof Byte) {
                        json.writeNumber((byte) value);
                    } else if (value instanceof Short) {
                        json.writeNumber((short) value);
                    } else if (value instanceof Integer) {
                        json.writeNumber((int) value);
                    } else if (value instanceof Long) {
                        json.writeNumber((long) value);
                    } else if (value instanceof Float) {
                        json.writeNumber((float) value);
                    } else if (value instanceof Double) {
                        json.writeNumber((double) value);
                    } else {
                        json.writeString((String) VARCHAR.convert(value));
                    }
                };
            default:
                throw QueryException.error("Unsupported type: " + type);
        }
    }

    @Override
    public void init() {
        baos.reset();
        try {
            json = JSON_FACTORY.createGenerator(baos);
            json.writeStartObject();
        } catch (IOException e) {
            throw sneakyThrow(e);
        }
    }

    @Override
    public Object conclude() {
        try {
            json.writeEndObject();
            json.close();
        } catch (IOException e) {
            throw sneakyThrow(e);
        }
        return baos.toByteArray();
    }
}
