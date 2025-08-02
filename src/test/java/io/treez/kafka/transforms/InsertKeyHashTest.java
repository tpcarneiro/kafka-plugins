package io.treez.kafka.transforms;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class InsertKeyHashTest {

    private InsertKeyHash<SinkRecord> transform;

    @BeforeEach
    void setUp() {
        transform = new InsertKeyHash<>();
    }

    @AfterEach
    void tearDown() {
        if (transform != null) {
            transform.close();
        }
    }

    @Test
    void testMd5Available() {
        // Test that MD5 is available in the environment
        assertDoesNotThrow(() -> {
            MessageDigest digest = MessageDigest.getInstance("MD5");
            assertNotNull(digest);
        });
    }

    @Test
    void testConfiguration() {
        Map<String, Object> config = new HashMap<>();
        config.put("field.name", "test_hash");
        
        // This should not throw an exception
        assertDoesNotThrow(() -> {
            transform.configure(config);
        });
    }

    @Test
    void testBasicSchemalessTransformation() {
        Map<String, Object> config = new HashMap<>();
        config.put("field.name", "key_hash");
        transform.configure(config);

        Map<String, Object> key = new HashMap<>();
        key.put("id", "test123");
        
        Map<String, Object> value = new HashMap<>();
        value.put("name", "Test User");

        SinkRecord record = new SinkRecord("test-topic", 0, null, key, null, value, 0);
        SinkRecord transformed = transform.apply(record);

        assertNotNull(transformed);
        assertNotNull(transformed.value());
        
        @SuppressWarnings("unchecked")
        Map<String, Object> transformedValue = (Map<String, Object>) transformed.value();
        
        // Verify original fields are preserved
        assertEquals("Test User", transformedValue.get("name"));
        
        // Verify hash field was added
        String hash = (String) transformedValue.get("key_hash");
        assertNotNull(hash);
        assertEquals(32, hash.length(), "MD5 should be 32 hex characters");
        assertTrue(hash.matches("[a-f0-9]+"), "Hash should be lowercase hex");
    }

    @Test
    void testBasicStructTransformation() {
        Map<String, Object> config = new HashMap<>();
        config.put("field.name", "record_hash");
        transform.configure(config);

        // Create simple key
        String simpleKey = "simple-key";

        // Create value schema and struct
        Schema valueSchema = SchemaBuilder.struct()
                .field("message", Schema.STRING_SCHEMA)
                .build();
        
        Struct valueStruct = new Struct(valueSchema)
                .put("message", "Hello World");

        SinkRecord record = new SinkRecord("test", 0, null, simpleKey, valueSchema, valueStruct, 0);
        SinkRecord transformed = transform.apply(record);

        assertNotNull(transformed);
        assertNotNull(transformed.value());
        assertTrue(transformed.value() instanceof Struct);
        
        Struct transformedValue = (Struct) transformed.value();
        
        // Verify original field is preserved
        assertEquals("Hello World", transformedValue.getString("message"));
        
        // Verify hash field was added
        String hash = transformedValue.getString("record_hash");
        assertNotNull(hash);
        assertEquals(32, hash.length(), "MD5 should be 32 hex characters");
        assertTrue(hash.matches("[a-f0-9]+"), "Hash should be lowercase hex");
    }

    @Test
    void testHashConsistency() {
        Map<String, Object> config = new HashMap<>();
        config.put("field.name", "consistent_hash");
        transform.configure(config);

        String key = "consistent-test-key";
        
        Map<String, Object> value1 = new HashMap<>();
        value1.put("data", "test1");
        
        Map<String, Object> value2 = new HashMap<>();
        value2.put("data", "test2");

        SinkRecord record1 = new SinkRecord("test-topic", 0, null, key, null, value1, 0);
        SinkRecord record2 = new SinkRecord("test-topic", 0, null, key, null, value2, 0);

        SinkRecord transformed1 = transform.apply(record1);
        SinkRecord transformed2 = transform.apply(record2);

        assertNotNull(transformed1);
        assertNotNull(transformed2);
        
        @SuppressWarnings("unchecked")
        Map<String, Object> resultValue1 = (Map<String, Object>) transformed1.value();
        @SuppressWarnings("unchecked")
        Map<String, Object> resultValue2 = (Map<String, Object>) transformed2.value();
        
        // Same key should produce same hash
        assertEquals(resultValue1.get("consistent_hash"), resultValue2.get("consistent_hash"));
    }

    @Test
    void testNullHandling() {
        Map<String, Object> config = new HashMap<>();
        config.put("field.name", "test_hash");
        transform.configure(config);

        // Test null value
        SinkRecord nullValueRecord = new SinkRecord("test-topic", 0, null, "key", null, null, 0);
        SinkRecord nullValueResult = transform.apply(nullValueRecord);
        assertEquals(nullValueRecord, nullValueResult);

        // Test null key
        Map<String, Object> value = new HashMap<>();
        value.put("data", "test");
        SinkRecord nullKeyRecord = new SinkRecord("test-topic", 0, null, null, null, value, 0);
        SinkRecord nullKeyResult = transform.apply(nullKeyRecord);
        assertEquals(nullKeyRecord, nullKeyResult);
    }

    @Test
    void testConfigurationValidation() {
        // Test missing required configuration
        assertThrows(Exception.class, () -> {
            Map<String, Object> config = new HashMap<>();
            // Missing field.name
            transform.configure(config);
        });
    }

    @Test
    void testDifferentKeys() {
        Map<String, Object> config = new HashMap<>();
        config.put("field.name", "unique_hash");
        transform.configure(config);

        String key1 = "key-one";
        String key2 = "key-two";
        
        Map<String, Object> value = new HashMap<>();
        value.put("data", "test");

        SinkRecord record1 = new SinkRecord("test-topic", 0, null, key1, null, value, 0);
        SinkRecord record2 = new SinkRecord("test-topic", 0, null, key2, null, value, 0);

        SinkRecord transformed1 = transform.apply(record1);
        SinkRecord transformed2 = transform.apply(record2);

        @SuppressWarnings("unchecked")
        Map<String, Object> value1 = (Map<String, Object>) transformed1.value();
        @SuppressWarnings("unchecked")
        Map<String, Object> value2 = (Map<String, Object>) transformed2.value();
        
        // Different keys should produce different hashes
        assertNotEquals(value1.get("unique_hash"), value2.get("unique_hash"));
    }

    @Test
    void testMapKeyNormalization() {
        Map<String, Object> config = new HashMap<>();
        config.put("field.name", "normalized_hash");
        transform.configure(config);

        // Create two maps with same content but different insertion order
        Map<String, Object> key1 = new HashMap<>();
        key1.put("b", "second");
        key1.put("a", "first");
        
        Map<String, Object> key2 = new HashMap<>();
        key2.put("a", "first");
        key2.put("b", "second");

        Map<String, Object> value1 = new HashMap<>();
        value1.put("data", "test");
        
        Map<String, Object> value2 = new HashMap<>();
        value2.put("data", "test");

        SinkRecord record1 = new SinkRecord("test", 0, null, key1, null, value1, 0);
        SinkRecord record2 = new SinkRecord("test", 0, null, key2, null, value2, 0);

        SinkRecord result1 = transform.apply(record1);
        SinkRecord result2 = transform.apply(record2);

        @SuppressWarnings("unchecked")
        Map<String, Object> resultValue1 = (Map<String, Object>) result1.value();
        @SuppressWarnings("unchecked")
        Map<String, Object> resultValue2 = (Map<String, Object>) result2.value();

        // Same content in different order should produce same hash
        assertEquals(resultValue1.get("normalized_hash"), resultValue2.get("normalized_hash"));
    }
}
