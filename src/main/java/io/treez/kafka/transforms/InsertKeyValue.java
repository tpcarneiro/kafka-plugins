package io.treez.kafka.transforms;

import org.apache.kafka.common.cache.Cache;
import org.apache.kafka.common.cache.LRUCache;
import org.apache.kafka.common.cache.SynchronizedCache;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SchemaUtil;
import org.apache.kafka.connect.transforms.util.SimpleConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

import static org.apache.kafka.connect.transforms.util.Requirements.requireMap;
import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;

public abstract class InsertKeyValue<R extends ConnectRecord<R>> implements Transformation<R> {

    private static final Logger log = LoggerFactory.getLogger(InsertKeyValue.class);

    public static final String OVERVIEW_DOC = "Insert the record key into the record value.";

    public static final String KEY_FIELD_CONFIG = "key.field";
    public static final String KEY_FIELD_DEFAULT = "__key";
    public static final String KEY_FIELD_DOC = "Field name for the record key.";

    public static final String INCLUDE_KEY_SCHEMA_CONFIG = "include.key.schema";
    public static final String INCLUDE_KEY_SCHEMA_DEFAULT = "false";
    public static final String INCLUDE_KEY_SCHEMA_DOC = "Whether to include the key schema in the value.";

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
        .define(KEY_FIELD_CONFIG, 
                ConfigDef.Type.STRING, 
                KEY_FIELD_DEFAULT,
                ConfigDef.Importance.HIGH, 
                KEY_FIELD_DOC)
        .define(INCLUDE_KEY_SCHEMA_CONFIG, 
                ConfigDef.Type.BOOLEAN, 
                INCLUDE_KEY_SCHEMA_DEFAULT,
                ConfigDef.Importance.MEDIUM, 
                INCLUDE_KEY_SCHEMA_DOC);

    private static final String PURPOSE = "inserting record key into value";

    private String keyFieldName;
    private boolean includeKeySchema;
    private Cache<Schema, Schema> schemaUpdateCache;

    @Override
    public void configure(Map<String, ?> props) {
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, props);
        keyFieldName = config.getString(KEY_FIELD_CONFIG);
        includeKeySchema = config.getBoolean(INCLUDE_KEY_SCHEMA_CONFIG);
        schemaUpdateCache = new SynchronizedCache<>(new LRUCache<>(16));
    }

    @Override
    public R apply(R record) {
        if (record.value() == null) {
            log.debug("Skipping record with null value");
            return record;
        }

        if (record.key() == null) {
            log.debug("Skipping record with null key");
            return record;
        }

        return applyWithSchema(record);
    }

    private R applyWithSchema(R record) {
        final Schema keySchema = record.keySchema();
        final Schema valueSchema = record.valueSchema();
        
        if (valueSchema == null) {
            return applySchemaless(record);
        }

        final Struct value = requireStruct(record.value(), PURPOSE);
        Schema updatedSchema = schemaUpdateCache.get(valueSchema);
        
        if (updatedSchema == null) {
            updatedSchema = makeUpdatedSchema(valueSchema, keySchema);
            schemaUpdateCache.put(valueSchema, updatedSchema);
        }

        final Struct updatedValue = new Struct(updatedSchema);
        
        // Copy existing fields
        for (Field field : value.schema().fields()) {
            updatedValue.put(field.name(), value.get(field));
        }
        
        // Add the key field
        updatedValue.put(keyFieldName, record.key());

        return newRecord(record, updatedSchema, updatedValue);
    }

    @SuppressWarnings("unchecked")
    private R applySchemaless(R record) {
        final Map<String, Object> value = requireMap(record.value(), PURPOSE);
        final Map<String, Object> updatedValue = new HashMap<>(value);
        updatedValue.put(keyFieldName, record.key());
        return newRecord(record, null, updatedValue);
    }

    // private Schema makeUpdatedSchema(Schema valueSchema, Schema keySchema) {
    //     final SchemaBuilder builder = SchemaUtil.copySchemaBasics(valueSchema, SchemaBuilder.struct());
        
    //     // Copy existing fields
    //     for (Field field : valueSchema.fields()) {
    //         builder.field(field.name(), field.schema());
    //     }
        
    //     // Add key field
    //     Schema keyFieldSchema = keySchema;
    //     if (!includeKeySchema) {
    //         // If not including schema, make it optional
    //         keyFieldSchema = Schema.OPTIONAL_STRING_SCHEMA;
    //     }
        
    //     builder.field(keyFieldName, keyFieldSchema);
    //     return builder.build();
    // }

    private Schema makeUpdatedSchema(Schema valueSchema, Schema keySchema) {
        final SchemaBuilder builder = SchemaUtil.copySchemaBasics(valueSchema, SchemaBuilder.struct());
        
        // Copy existing fields
        for (Field field : valueSchema.fields()) {
            builder.field(field.name(), field.schema());
        }
        
        // Add key field with proper schema
        Schema keyFieldSchema;
        if (includeKeySchema && keySchema != null) {
            // Use the actual key schema
            keyFieldSchema = keySchema;
        } else {
            // For schemaless or when not including schema, make it optional
            if (keySchema != null) {
                // Create an optional version of the key schema
                keyFieldSchema = SchemaBuilder.type(keySchema.type())
                        .optional()
                        .build();
                
                // Handle struct schemas specially
                if (keySchema.type() == Schema.Type.STRUCT) {
                    SchemaBuilder structBuilder = SchemaBuilder.struct().optional();
                    for (Field field : keySchema.fields()) {
                        structBuilder.field(field.name(), field.schema());
                    }
                    keyFieldSchema = structBuilder.build();
                }
            } else {
                keyFieldSchema = Schema.OPTIONAL_STRING_SCHEMA;
            }
        }
        
        builder.field(keyFieldName, keyFieldSchema);
        return builder.build();
    }

    protected abstract R newRecord(R record, Schema updatedSchema, Object updatedValue);

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public void close() {
        schemaUpdateCache = null;
    }

    public static class Key<R extends ConnectRecord<R>> extends InsertKeyValue<R> {
        @Override
        protected R newRecord(R record, Schema updatedSchema, Object updatedValue) {
            return record.newRecord(record.topic(), record.kafkaPartition(), updatedSchema, 
                    updatedValue, record.valueSchema(), record.value(), record.timestamp());
        }
    }

    public static class Value<R extends ConnectRecord<R>> extends InsertKeyValue<R> {
        @Override
        protected R newRecord(R record, Schema updatedSchema, Object updatedValue) {
            return record.newRecord(record.topic(), record.kafkaPartition(), 
                    record.keySchema(), record.key(), updatedSchema, updatedValue, record.timestamp());
        }
    }
}
