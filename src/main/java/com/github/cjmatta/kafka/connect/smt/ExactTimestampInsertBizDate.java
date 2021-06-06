/*
 * Copyright © 2019 Christopher Matta (chris.matta@gmail.com)
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

package com.github.cjmatta.kafka.connect.smt;

import org.apache.kafka.common.cache.Cache;
import org.apache.kafka.common.cache.LRUCache;
import org.apache.kafka.common.cache.SynchronizedCache;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SchemaUtil;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import static org.apache.kafka.connect.transforms.util.Requirements.requireMap;
import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;

/**
 * Is this a SMT or a function like a operator?
 *
 * @param <R>
 */
public abstract class ExactTimestampInsertBizDate<R extends ConnectRecord<R>> implements Transformation<R>
{

    public static final String OVERVIEW_DOC =
        "Extract a field from a connect record and transform and into a connect record";

    private interface ConfigName
    {
        String INPUT_FIELD_NAME = "input.field.name";
        String OUTPUT_FORMAT = "output.format";
        String OUTPUT_FIELD_NAME = "output.field.name"; // If this field name exists in the input record, the field value will be overrided
    }

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
        .define(ConfigName.INPUT_FIELD_NAME, ConfigDef.Type.STRING, "", ConfigDef.Importance.HIGH,
                "field name for extraction")
        .define(ConfigName.OUTPUT_FORMAT, ConfigDef.Type.STRING, "yyyyMMdd", ConfigDef.Importance.HIGH,
                "default output date format")
        .define(ConfigName.OUTPUT_FIELD_NAME, ConfigDef.Type.STRING, "bizDate", ConfigDef.Importance.HIGH,
                "output field name")
        ;

    private static final String PURPOSE = "generate bizDate from a timestamp of format bigint";

    private String inputFieldName;
    private String outputFormat;
    private String outputFieldName;

    private Cache<Schema, Schema> schemaUpdateCache;

    @Override
    public void configure(Map<String, ?> props)
    {
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, props);
        inputFieldName = config.getString(ConfigName.INPUT_FIELD_NAME);
        outputFormat = config.getString(ConfigName.OUTPUT_FORMAT);
        outputFieldName = config.getString(ConfigName.OUTPUT_FIELD_NAME);

        schemaUpdateCache = new SynchronizedCache<>(new LRUCache<Schema, Schema>(16));
    }


    @Override
    public R apply(R record)
    {
        if (operatingSchema(record) == null) {
            return applySchemaless(record);
        } else {
            return applyWithSchema(record);
        }
    }

    private R applySchemaless(R record)
    {
        final Map<String, Object> value = requireMap(operatingValue(record), PURPOSE);

        final Map<String, Object> updatedValue = new HashMap<>(value);
        Object fieldValue = updatedValue.get(inputFieldName);
        if (!(fieldValue instanceof Long)) {
            throw new DataException("the config field name value type should be Long");
        }
        String bizDate = new SimpleDateFormat(outputFormat).format(new Date((Long) fieldValue));

        updatedValue.put(outputFieldName, bizDate);

        return newRecord(record, null, updatedValue);
    }

    private R applyWithSchema(R record)
    {
        final Struct value = requireStruct(operatingValue(record), PURPOSE);

        Schema updatedSchema = schemaUpdateCache.get(value.schema());
        if (updatedSchema == null) {
            updatedSchema = makeUpdatedSchema(value.schema());
            schemaUpdateCache.put(value.schema(), updatedSchema);
        }

        final Struct updatedValue = new Struct(updatedSchema);

        for (Field field : value.schema().fields()) {
            updatedValue.put(field.name(), value.get(field));
        }

        // add new bizDate field
        Object fieldValue = updatedValue.get(inputFieldName);
        if (!(fieldValue instanceof Long)) {
            throw new DataException("the config field name value type should be Long");
        }
        String bizDate = new SimpleDateFormat(outputFormat).format(new Date((Long) fieldValue));

        updatedValue.put(outputFieldName, bizDate);

        return newRecord(record, updatedSchema, updatedValue);
    }

    @Override
    public ConfigDef config()
    {
        return CONFIG_DEF;
    }

    @Override
    public void close()
    {
        schemaUpdateCache = null;
    }

    private Schema makeUpdatedSchema(Schema schema)
    {
        final SchemaBuilder builder = SchemaUtil.copySchemaBasics(schema, SchemaBuilder.struct());

        for (Field field : schema.fields()) {
            builder.field(field.name(), field.schema());
        }

        builder.field(outputFieldName, Schema.STRING_SCHEMA);

        return builder.build();
    }

    protected abstract Schema operatingSchema(R record);

    protected abstract Object operatingValue(R record);

    protected abstract R newRecord(R record, Schema updatedSchema, Object updatedValue);

    public static class Key<R extends ConnectRecord<R>> extends ExactTimestampInsertBizDate<R>
    {

        @Override
        protected Schema operatingSchema(R record)
        {
            return record.keySchema();
        }

        @Override
        protected Object operatingValue(R record)
        {
            return record.key();
        }

        @Override
        protected R newRecord(R record, Schema updatedSchema, Object updatedValue)
        {
            return record.newRecord(
                record.topic(),
                record.kafkaPartition(),
                updatedSchema,
                updatedValue,
                record.valueSchema(),
                record.value(),
                record.timestamp()
            );
        }

    }

    public static class Value<R extends ConnectRecord<R>> extends ExactTimestampInsertBizDate<R>
    {

        @Override
        protected Schema operatingSchema(R record)
        {
            return record.valueSchema();
        }

        @Override
        protected Object operatingValue(R record)
        {
            return record.value();
        }

        @Override
        protected R newRecord(R record, Schema updatedSchema, Object updatedValue)
        {
            return record.newRecord(
                record.topic(),
                record.kafkaPartition(),
                record.keySchema(),
                record.key(),
                updatedSchema,
                updatedValue,
                record.timestamp()
            );
        }

    }
}


