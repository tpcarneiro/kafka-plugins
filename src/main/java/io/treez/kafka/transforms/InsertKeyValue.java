package io.treez.kafka.transforms;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.Requirements;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

import java.util.HashMap;
import java.util.Map;

public class InsertKeyValue<R extends ConnectRecord<R>> implements Transformation<R> {
    
    public static final String FIELD_NAME_CONFIG = "field.name";
    
    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(FIELD_NAME_CONFIG, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE,
                    ConfigDef.Importance.HIGH, "Name of the field to add");
    
    private String fieldName;
    
    @Override
    public void configure(Map<String, ?> configs) {
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, configs);
        fieldName = config.getString(FIELD_NAME_CONFIG);
    }

    @Override
    public R apply(R record) {
        if (record.value() == null || record.key() == null) {
            return record;
        }
        
        // Check if the record has a schema
        if (record.valueSchema() == null) {
            // Schemaless - value is a Map
            return applySchemaless(record);
        } else {
            // Schema-based - value is a Struct
            return applyWithSchema(record);
        }
    }

    private R applySchemaless(R record) {
        // Handle schemaless records (Map-based values)
        @SuppressWarnings("unchecked")
        Map<String, Object> valueMap = (Map<String, Object>) record.value();
        Map<String, Object> updatedValue = new HashMap<>(valueMap);
        
        // Convert and add key
        Object keyToInsert = convertKeyToMap(record.key());
        updatedValue.put(fieldName, keyToInsert);
        
        return record.newRecord(
            record.topic(),
            record.kafkaPartition(),
            record.keySchema(),
            record.key(),
            record.valueSchema(), // null for schemaless
            updatedValue,
            record.timestamp()
        );
    }

    private R applyWithSchema(R record) {
        // Handle schema-based records (Struct-based values)
        final Struct value = (Struct) record.value();
        final Schema valueSchema = record.valueSchema();
        final Schema keySchema = record.keySchema();
        
        // Build new schema with the key field
        final SchemaBuilder builder = SchemaBuilder.struct();
        if (valueSchema.name() != null) {
            builder.name(valueSchema.name());
        }
        if (valueSchema.version() != null) {
            builder.version(valueSchema.version());
        }
        if (valueSchema.doc() != null) {
            builder.doc(valueSchema.doc());
        }
        
        // Copy existing fields to new schema
        for (org.apache.kafka.connect.data.Field field : valueSchema.fields()) {
            builder.field(field.name(), field.schema());
        }
        
        builder.field(fieldName, keySchema);
        
        final Schema updatedSchema = builder.build();
        final Struct updatedValue = new Struct(updatedSchema);
        
        // Copy existing field values
        for (org.apache.kafka.connect.data.Field field : valueSchema.fields()) {
            updatedValue.put(field.name(), value.get(field));
        }
        
        updatedValue.put(fieldName, record.key());
        
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

    private Object convertKeyToMap(Object key) {
        if (key == null) {
            return null;
        }
        
        if (key instanceof Struct) {
            // Convert Struct to Map for easier handling
            Struct structKey = (Struct) key;
            Map<String, Object> keyMap = new HashMap<>();
            for (org.apache.kafka.connect.data.Field field : structKey.schema().fields()) {
                keyMap.put(field.name(), structKey.get(field));
            }
            return keyMap;
        } else if (key instanceof Map) {
            // Already a Map, return a copy
            return new HashMap<>((Map<?, ?>) key);
        } else {
            // Primitive type (String, Integer, etc.) - return as-is
            return key;
        }
    }
    
    private String convertKeyToString(Object key) {
        if (key == null) {
            return null;
        }
        
        if (key instanceof Struct) {
            Struct structKey = (Struct) key;
            return structKey.schema().fields().stream()
                .map(field -> field.name() + "=" + structKey.get(field))
                .collect(java.util.stream.Collectors.joining(",", "{", "}"));
        } else {
            return key.toString();
        }
    }
    
    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }
    
    @Override
    public void close() {
        // Nothing to clean up
    }
}
