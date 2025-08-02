package io.treez.kafka.transforms;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.Requirements;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.TreeMap;


public class InsertKeyHash<R extends ConnectRecord<R>> implements Transformation<R> {
    
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
        
        String keyHash = generateKeyHash(record.key());
        updatedValue.put(fieldName, keyHash);
        
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
        
        // Build new schema
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
        
        // Add key field
        builder.field(fieldName, Schema.OPTIONAL_STRING_SCHEMA);
        
        final Schema updatedSchema = builder.build();
        final Struct updatedValue = new Struct(updatedSchema);
        
        // Copy existing field values
        for (org.apache.kafka.connect.data.Field field : valueSchema.fields()) {
            updatedValue.put(field.name(), value.get(field));
        }
        
        String keyHash = generateKeyHash(record.key());
        updatedValue.put(fieldName, keyHash);
        
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

    private String generateKeyHash(Object key) {
        try {
            MessageDigest messageDigest = MessageDigest.getInstance("MD5");
            String input = normalizeKeyForHashing(key);
            byte[] hash = messageDigest.digest(input.getBytes(StandardCharsets.UTF_8));
            StringBuilder hexString = new StringBuilder();
            
            for (byte b : hash) {
                String hex = Integer.toHexString(0xff & b);
                if (hex.length() == 1) {
                    hexString.append('0');
                }
                hexString.append(hex);
            }
            
            return hexString.toString();

        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("MD5 algorithm not available", e);
        }
    }

    private String normalizeKeyForHashing(Object key) {
        if (key == null) {
            return "null";
        }
        
        if (key instanceof Struct) {
            // Create deterministic string representation
            Struct structKey = (Struct) key;
            TreeMap<String, Object> sortedFields = new TreeMap<>();
            for (org.apache.kafka.connect.data.Field field : structKey.schema().fields()) {
                sortedFields.put(field.name(), structKey.get(field));
            }
            StringBuilder sb = new StringBuilder();
            sortedFields.forEach((name, value) -> 
                sb.append(value.toString()).append("|")
            );
            
            return sb.toString();

        } else if (key instanceof Map) {
            TreeMap<String, Object> sortedMap = new TreeMap<>((Map<String, Object>) key);
            StringBuilder sb = new StringBuilder();
            sortedMap.forEach((name, value) -> 
                sb.append(name).append("=").append(value).append(";")
            );
            return sb.toString();

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
