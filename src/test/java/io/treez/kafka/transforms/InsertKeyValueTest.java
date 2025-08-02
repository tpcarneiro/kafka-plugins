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
        config.put("field.name", "test_field");
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
        assertEquals(key, transformedValue.get("test_field"));
    }

    @Test
    void testNullValueSkipped() {
        Map<String, Object> config = new HashMap<>();
        config.put("field.name", "test_field");
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
            config.put("field.wrong_name", "test_field");
            // Missing field.name
            transform.configure(config);
        });
    }

    @Test
    void testOverwriteExistingField() {
        Map<String, Object> config = new HashMap<>();
        config.put("field.name", "name"); // This field already exists
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
        
        // The transform should overwrite the existing field
        assertEquals(key, transformedValue.get("name"));
        assertEquals("john@example.com", transformedValue.get("email"));
    }
}
