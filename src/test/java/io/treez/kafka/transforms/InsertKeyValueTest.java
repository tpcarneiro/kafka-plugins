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

    private InsertKeyValue.Value<SinkRecord> transform;

    @BeforeEach
    void setUp() {
        transform = new InsertKeyValue.Value<>();
    }

    @AfterEach
    void tearDown() {
        transform.close();
    }

    @Test
    void testSchemalessTransformation() {
        Map<String, Object> config = new HashMap<>();
        config.put("key.field", "_messageKey");
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
        
        assertEquals("John Doe", transformedValue.get("name"));
        assertEquals("john@example.com", transformedValue.get("email"));
        assertEquals(key, transformedValue.get("_messageKey"));
    }

    @Test
    void testSchemaTransformation() {
        Map<String, Object> config = new HashMap<>();
        config.put("key.field", "_key");
        config.put("include.key.schema", "true");
        transform.configure(config);

        Schema keySchema = SchemaBuilder.struct()
                .field("id", Schema.INT32_SCHEMA)
                .build();
        
        Schema valueSchema = SchemaBuilder.struct()
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
        
        assertEquals("John Doe", transformedValue.get("name"));
        assertEquals("john@example.com", transformedValue.get("email"));
        assertEquals(key, transformedValue.get("_key"));
        // Verify the schema includes the key field
        assertNotNull(transformedValue.schema().field("_key"));
        assertEquals(keySchema, transformedValue.schema().field("_key").schema());
    }

    @Test
    void testNullKeySkipped() {
        Map<String, Object> config = new HashMap<>();
        transform.configure(config);

        Map<String, Object> value = new HashMap<>();
        value.put("name", "John Doe");

        SinkRecord record = new SinkRecord("test-topic", 0, null, null, null, value, 0);
        SinkRecord transformed = transform.apply(record);

        assertEquals(record, transformed);
    }
}
