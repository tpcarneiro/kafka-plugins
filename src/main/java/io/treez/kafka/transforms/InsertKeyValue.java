package io.treez.kafka.transforms;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

import java.util.Map;

public class InsertKeyValue<R extends ConnectRecord<R>> implements Transformation<R> {
    
    public static final String FIELD_NAME_CONFIG = "field.name";
    public static final String FIELD_VALUE_CONFIG = "field.value";
    
    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(FIELD_NAME_CONFIG, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE,
                    ConfigDef.Importance.HIGH, "Name of the field to add")
            .define(FIELD_VALUE_CONFIG, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE,
                    ConfigDef.Importance.HIGH, "Value of the field to add");
    
    private String fieldName;
    private String fieldValue;
    
    @Override
    public void configure(Map<String, ?> configs) {
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, configs);
        fieldName = config.getString(FIELD_NAME_CONFIG);
        fieldValue = config.getString(FIELD_VALUE_CONFIG);
    }
    
    @Override
    public R apply(R record) {
        if (record.value() == null) {
            return record;
        }
        
        if (record.valueSchema() == null) {
            return applySchemaless(record);
        } else {
            return applyWithSchema(record);
        }
    }
    
    private R applySchemaless(R record) {
        // Handle schemaless records (Map)
        if (record.value() instanceof Map) {
            @SuppressWarnings("unchecked")
            final Map<String, Object> value = (Map<String, Object>) record.value();
            final Map<String, Object> updatedValue = new java.util.HashMap<>(value);
            updatedValue.put(fieldName, fieldValue);
            
            return record.newRecord(
                    record.topic(),
                    record.kafkaPartition(),
                    record.keySchema(),
                    record.key(),
                    null,
                    updatedValue,
                    record.timestamp()
            );
        }
        return record;
    }
    
    private R applyWithSchema(R record) {
        final Struct value = (Struct) record.value();
        final Schema schema = record.valueSchema();
        
        // Build new schema
        final SchemaBuilder builder = SchemaBuilder.struct();
        if (schema.name() != null) {
            builder.name(schema.name());
        }
        if (schema.version() != null) {
            builder.version(schema.version());
        }
        if (schema.doc() != null) {
            builder.doc(schema.doc());
        }
        
        // Copy existing fields to new schema
        for (org.apache.kafka.connect.data.Field field : schema.fields()) {
            builder.field(field.name(), field.schema());
        }
        
        // Add new field to schema
        builder.field(fieldName, Schema.STRING_SCHEMA);
        
        final Schema updatedSchema = builder.build();
        
        // Create new struct with updated schema
        final Struct updatedValue = new Struct(updatedSchema);
        
        // Copy existing field values
        for (org.apache.kafka.connect.data.Field field : schema.fields()) {
            updatedValue.put(field.name(), value.get(field));
        }
        
        // Add new field value
        updatedValue.put(fieldName, fieldValue);
        
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
    
    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }
    
    @Override
    public void close() {
        // Nothing to clean up
    }
}
