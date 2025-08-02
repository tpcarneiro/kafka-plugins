package io.treez.kafka.transforms;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;
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

    private Object convertKeyToString(Object key) {
        if (key == null) {
            return key;
        }
        
        if (key instanceof Struct) {
            // Convert Struct to Map for schemaless context
            Struct structKey = (Struct) key;
            Map<String, Object> keyMap = new HashMap<>();
            for (org.apache.kafka.connect.data.Field field : structKey.schema().fields()) {
                keyMap.put(field.name(), structKey.get(field));
            }
            return keyMap;
        } else if (key instanceof Map) {
            // Already a Map, return as-is
            return new HashMap<>((Map<?, ?>) key);
        } else {
            // Primitive type (String, Integer, etc.) - return as-is
            return key;
        }
    }
    
    @Override
    public R apply(R record) {
        if (record.value() == null || record.key() == null) {
            return record;
        }
        
        Map<String, Object> value = (Map<String, Object>) record.value();
        Map<String, Object> updatedValue = new java.util.HashMap<>(value);
        Object keyValue = convertKeyToString(record.key());
        updatedValue.put(fieldName, keyValue);
            
        return record.newRecord(
            record.topic(),
            record.kafkaPartition(),
            record.keySchema(),
            record.key(),
            record.valueSchema(),
            updatedValue,
            record.timestamp()
        );
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
