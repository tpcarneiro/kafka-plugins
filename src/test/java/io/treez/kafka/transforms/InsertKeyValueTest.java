package io.treez.kafka.transforms;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class InsertKeyValueTest {

    private InsertKeyValue<SinkRecord> transform; // Remove .Value

    @BeforeEach
    void setUp() {
        transform = new InsertKeyValue<>(); // Remove .Value
    }

    @AfterEach
    void tearDown() {
        transform.close();
    }

    @Test
    void testSchemalessTransformation() {
        Map<String, Object> config = new HashMap<>();
        config.put("field.name", "custom_field");
        config.put("field.value", "custom_value");
        transform.configure(config);

        Map<String, Object> key = new HashMap<>();
        key.put("id", 123);
        
        Map<String, Object> value = new HashMap<>();
        value.put("name", "John Doe");
        value.put("email", "john@example.com");

        SinkRecord record = new SinkRecord("test-topic", 0, null, key, null, value, 0);
        SinkRecord transformed = transform.apply(record);

        @SuppressWarnings("unchecked")
        Map<String, Object> transformedValue = (Map<String, Object>) transformed.value();
        
        // Verify original fields are preserved
        assertEquals("John Doe", transformedValue.get("name"));
        assertEquals("john@example.com", transformedValue.get("email"));
        
        // Verify new field was added
        assertEquals("custom_value", transformedValue.get("custom_field"));
        
        // Verify original value map wasn't modified
        assertFalse(value.containsKey("custom_field"));
    }

    @Test
    void testSchemaTransformation() {
        Map<String, Object> config = new HashMap<>();
        config.put("field.name", "processing_info");
        config.put("field.value", "processed_by_smt");
        transform.configure(config);

        Schema keySchema = SchemaBuilder.struct()
                .field("id", Schema.INT32_SCHEMA)
                .build();
        
        Schema valueSchema = SchemaBuilder.struct()
                .name("test.record.Value")
                .version(1)
                .doc("Test record schema")
                .field("name", Schema.STRING_SCHEMA)
                .field("email", Schema.STRING_SCHEMA)
                .build();

        Struct key = new Struct(keySchema).put("id", 123);
        Struct value = new Struct(valueSchema)
                .put("name", "John Doe")
                .put("email", "john@example.com");

        SinkRecord record = new SinkRecord("test-topic", 0, keySchema, key, valueSchema, value, 0);
        SinkRecord transformed = transform.apply(record);

        Struct transformedValue = (Struct) transformed.value();
        Schema transformedSchema = transformed.valueSchema();
        
        // Verify original fields are preserved
        assertEquals("John Doe", transformedValue.get("name"));
        assertEquals("john@example.com", transformedValue.get("email"));
        
        // Verify new field was added
        assertEquals("processed_by_smt", transformedValue.get("processing_info"));
        
        // Verify schema metadata is preserved
        assertEquals("test.record.Value", transformedSchema.name());
        assertEquals(Integer.valueOf(1), transformedSchema.version());
        assertEquals("Test record schema", transformedSchema.doc());
        
        // Verify the schema includes the new field
        assertNotNull(transformedSchema.field("processing_info"));
        assertEquals(Schema.STRING_SCHEMA, transformedSchema.field("processing_info").schema());
        
        // Verify original schema fields are still present
        assertNotNull(transformedSchema.field("name"));
        assertNotNull(transformedSchema.field("email"));
    }

    @Test
    void testNullValueSkipped() {
        Map<String, Object> config = new HashMap<>();
        config.put("field.name", "test_field");
        config.put("field.value", "test_value");
        transform.configure(config);

        SinkRecord record = new SinkRecord("test-topic", 0, null, null, null, null, 0);
        SinkRecord transformed = transform.apply(record);

        // Should return the same record unchanged
        assertEquals(record, transformed);
        assertNull(transformed.value());
    }

    @Test
    void testConfigurationValidation() {
        // Test missing required configuration
        assertThrows(Exception.class, () -> {
            Map<String, Object> config = new HashMap<>();
            config.put("field.name", "test_field");
            // Missing field.value
            transform.configure(config);
        });

        assertThrows(Exception.class, () -> {
            Map<String, Object> config = new HashMap<>();
            config.put("field.value", "test_value");
            // Missing field.name
            transform.configure(config);
        });
    }

    @Test
    void testOverwriteExistingField() {
        Map<String, Object> config = new HashMap<>();
        config.put("field.name", "name"); // This field already exists
        config.put("field.value", "overwritten_value");
        transform.configure(config);

        Map<String, Object> value = new HashMap<>();
        value.put("name", "John Doe");
        value.put("email", "john@example.com");

        SinkRecord record = new SinkRecord("test-topic", 0, null, null, null, value, 0);
        SinkRecord transformed = transform.apply(record);

        @SuppressWarnings("unchecked")
        Map<String, Object> transformedValue = (Map<String, Object>) transformed.value();
        
        // The transform should overwrite the existing field
        assertEquals("overwritten_value", transformedValue.get("name"));
        assertEquals("john@example.com", transformedValue.get("email"));
    }

    @Test
    void testComplexDebeziumLikeRecord() {
        Map<String, Object> config = new HashMap<>();
        config.put("field.name", "__entity_id");
        config.put("field.value", "xyz123");
        transform.configure(config);

        // Simulate a Debezium-like record structure (after ExtractNewRecordState)
        Schema recordSchema = SchemaBuilder.struct()
                .name("inventory.customers.Envelope")
                .field("id", Schema.INT32_SCHEMA)
                .field("name", Schema.STRING_SCHEMA)
                .field("email", Schema.STRING_SCHEMA)
                .field("db", Schema.STRING_SCHEMA)
                .field("table", Schema.STRING_SCHEMA)
                .field("op", Schema.STRING_SCHEMA)
                .field("ts_ms", Schema.INT64_SCHEMA)
                .build();

        Struct value = new Struct(recordSchema)
                .put("id", 1001)
                .put("name", "John Doe")
                .put("email", "john@example.com")
                .put("db", "inventory")
                .put("table", "customers")
                .put("op", "c")
                .put("ts_ms", 1690934400000L);

        SinkRecord record = new SinkRecord("inventory.customers", 0, null, null, recordSchema, value, 0);
        SinkRecord transformed = transform.apply(record);

        Struct transformedValue = (Struct) transformed.value();
        
        // Verify all original fields are preserved
        assertEquals(1001, transformedValue.get("id"));
        assertEquals("John Doe", transformedValue.get("name"));
        assertEquals("john@example.com", transformedValue.get("email"));
        assertEquals("inventory", transformedValue.get("db"));
        assertEquals("customers", transformedValue.get("table"));
        assertEquals("c", transformedValue.get("op"));
        assertEquals(1690934400000L, transformedValue.get("ts_ms"));
        
        // Verify new field was added
        assertEquals("xyz123", transformedValue.get("__entity_id"));
        
        // Verify schema has the new field
        assertNotNull(transformed.valueSchema().field("__entity_id"));
    }
}
