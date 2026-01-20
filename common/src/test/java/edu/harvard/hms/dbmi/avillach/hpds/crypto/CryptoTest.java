package edu.harvard.hms.dbmi.avillach.hpds.crypto;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test suite for Crypto class covering:
 * - Basic encryption/decryption functionality
 * - Null input validation
 * - Large payload handling
 * - Wire format validation
 */
class CryptoTest {

    private static final String TEST_KEY_PATH = "src/test/resources/test_encryption_key";
    private static final String TEST_MESSAGE = "This is a test message for encryption.";

    @BeforeAll
    static void setUp() {
        // Load test encryption key
        Crypto.loadKey(Crypto.DEFAULT_KEY_NAME, new File(TEST_KEY_PATH).getAbsolutePath());
    }

    // ==================== Basic Functionality Tests ====================

    @Test
    void testBasicEncryptDecrypt() {
        byte[] plaintext = TEST_MESSAGE.getBytes(StandardCharsets.UTF_8);
        byte[] encrypted = Crypto.encryptData(plaintext);

        assertNotNull(encrypted);
        assertFalse(Arrays.equals(plaintext, encrypted), "Encrypted data should differ from plaintext");

        byte[] decrypted = Crypto.decryptData(encrypted);
        assertArrayEquals(plaintext, decrypted, "Decrypted data should match original plaintext");
        assertEquals(TEST_MESSAGE, new String(decrypted, StandardCharsets.UTF_8));
    }

    @Test
    void testEncryptDecryptEmptyArray() {
        byte[] empty = new byte[0];
        byte[] encrypted = Crypto.encryptData(empty);
        byte[] decrypted = Crypto.decryptData(encrypted);

        assertArrayEquals(empty, decrypted, "Empty array should round-trip successfully");
    }

    @Test
    void testEncryptDecryptLargePayload() {
        // 10MB payload
        byte[] large = new byte[10_000_000];
        Arrays.fill(large, (byte) 0x42);

        byte[] encrypted = Crypto.encryptData(large);
        byte[] decrypted = Crypto.decryptData(encrypted);

        assertArrayEquals(large, decrypted, "Large payload should round-trip successfully");
    }

    // ==================== Null Input Validation Tests ====================

    @Test
    void testEncryptDataWithNullInput() {
        IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            () -> Crypto.encryptData(null),
            "encryptData should reject null input"
        );
        assertEquals("Plaintext data cannot be null", exception.getMessage());
    }

    @Test
    void testDecryptDataWithNullInput() {
        IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            () -> Crypto.decryptData(null),
            "decryptData should reject null input"
        );
        assertEquals("Encrypted data cannot be null", exception.getMessage());
    }

    @Test
    void testEncryptDataWithNullKeyName() {
        IllegalStateException exception = assertThrows(
            IllegalStateException.class,
            () -> Crypto.encryptData("NONEXISTENT_KEY", TEST_MESSAGE.getBytes()),
            "encryptData should reject missing key"
        );
        assertTrue(exception.getMessage().contains("not found"),
            "Error message should indicate key not found");
    }

    @Test
    void testDecryptDataWithNullKeyName() {
        byte[] encrypted = Crypto.encryptData(TEST_MESSAGE.getBytes());

        IllegalStateException exception = assertThrows(
            IllegalStateException.class,
            () -> Crypto.decryptData("NONEXISTENT_KEY", encrypted),
            "decryptData should reject missing key"
        );
        assertTrue(exception.getMessage().contains("not found"),
            "Error message should indicate key not found");
    }

    // ==================== Hard Limit Tests ====================

    @Test
    void testLargePayloadEncryption() {
        // Test 100MB payload - well under Java array limits
        int safeSize = 100_000_000;
        byte[] data = new byte[safeSize];
        Arrays.fill(data, (byte) 0xAA);

        assertDoesNotThrow(() -> {
            byte[] encrypted = Crypto.encryptData(data);
            byte[] decrypted = Crypto.decryptData(encrypted);
            assertEquals(safeSize, decrypted.length);
        }, "100MB payload should encrypt successfully");
    }

    // ==================== Wire Format Validation Tests ====================

    @Test
    void testWireFormatStructure() {
        byte[] plaintext = TEST_MESSAGE.getBytes(StandardCharsets.UTF_8);
        byte[] encrypted = Crypto.encryptData(plaintext);

        // Wire format: [ivLen:4 bytes][iv:12-32 bytes][ciphertext+tag]
        assertTrue(encrypted.length > 4, "Encrypted data should contain at least IV length field");

        // Extract IV length (first 4 bytes as int)
        int ivLength = java.nio.ByteBuffer.wrap(encrypted).getInt();
        assertTrue(ivLength >= 12 && ivLength <= 32, "IV length should be between 12-32 bytes");

        // Verify structure size
        int expectedMinSize = 4 + ivLength + plaintext.length + 16; // 4(ivLen) + iv + plaintext + 16(auth tag)
        assertTrue(encrypted.length >= expectedMinSize, "Encrypted size should include all components");
    }

    @Test
    void testEncryptionProducesDifferentOutputs() {
        // Same plaintext encrypted twice should produce different ciphertexts (due to random IV)
        byte[] plaintext = TEST_MESSAGE.getBytes(StandardCharsets.UTF_8);
        byte[] encrypted1 = Crypto.encryptData(plaintext);
        byte[] encrypted2 = Crypto.encryptData(plaintext);

        assertFalse(Arrays.equals(encrypted1, encrypted2),
            "Same plaintext should produce different ciphertexts due to random IV");

        // But both should decrypt to same plaintext
        assertArrayEquals(plaintext, Crypto.decryptData(encrypted1));
        assertArrayEquals(plaintext, Crypto.decryptData(encrypted2));
    }

}

