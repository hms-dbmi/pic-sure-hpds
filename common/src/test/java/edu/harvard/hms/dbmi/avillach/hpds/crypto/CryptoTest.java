package edu.harvard.hms.dbmi.avillach.hpds.crypto;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestPropertySource;

import java.lang.reflect.Method;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest(classes = Crypto.class)
@TestPropertySource(properties = {
        "encryption.enabled=false"
})

public class CryptoTest {

    private final String testData = "This is a test string.";

    @BeforeEach
    public void setUp() throws Exception {
        Method setKey = Crypto.class.getDeclaredMethod("setKey", String.class, byte[].class);
        setKey.setAccessible(true);
        setKey.invoke(null, Crypto.DEFAULT_KEY_NAME, "1234567890123456".getBytes());
        Crypto.ENCRYPTION_ENABLED = true;
    }

    @Test
    public void encryptsAndDecryptsDataCorrectly() {
        byte[] plaintext = testData.getBytes();
        byte[] encrypted = Crypto.encryptData(plaintext);

        assertNotNull(encrypted);
        assertNotEquals(testData, new String(encrypted));

        byte[] decrypted = Crypto.decryptData(encrypted);
        assertNotNull(decrypted);
        assertEquals(testData, new String(decrypted));
    }

    @Test
    public void returnsPlaintextWhenEncryptionDisabled() {
        Crypto.ENCRYPTION_ENABLED = false;

        byte[] plaintext = testData.getBytes();
        byte[] result = Crypto.encryptData(plaintext);

        assertArrayEquals(plaintext, result);
        assertArrayEquals(plaintext, Crypto.decryptData(result));
    }
}