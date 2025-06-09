package edu.harvard.hms.dbmi.avillach.hpds.etl.phenotype.csv;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.crypto.Cipher;
import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import javax.crypto.spec.IvParameterSpec;
import java.security.SecureRandom;

import static org.junit.jupiter.api.Assertions.*;

public class CryptoBenchmarkTest {
    private byte[] testData;
    private SecretKey secretKey;
    private byte[] iv;

    @BeforeEach
    void setUp() throws Exception {
        SecureRandom random = new SecureRandom();
        testData = new byte[100 * 1024 * 1024]; // 100MB of random data
        random.nextBytes(testData);

        // Generate AES key
        KeyGenerator keyGen = KeyGenerator.getInstance("AES");
        keyGen.init(128);
        secretKey = keyGen.generateKey();

        // Generate IV
        iv = new byte[16];
        random.nextBytes(iv);
    }

    private byte[] encryptData(byte[] data) throws Exception {
        Cipher cipher = Cipher.getInstance("AES/CBC/PKCS5Padding");
        cipher.init(Cipher.ENCRYPT_MODE, secretKey, new IvParameterSpec(iv));
        return cipher.doFinal(data);
    }

    private byte[] decryptData(byte[] data) throws Exception {
        Cipher cipher = Cipher.getInstance("AES/CBC/PKCS5Padding");
        cipher.init(Cipher.DECRYPT_MODE, secretKey, new IvParameterSpec(iv));
        return cipher.doFinal(data);
    }

    @Test
    void testEncryptionPerformance() throws Exception {
        long startTime = System.nanoTime();
        byte[] encryptedData = encryptData(testData);
        long endTime = System.nanoTime();
        double durationMs = (endTime - startTime) / 1_000_000.0;

        System.out.println("================ Encryption Performance ================");
        System.out.printf("Encryption Time: %.3f ms\n", durationMs);
        System.out.println("Description: Measures the time required to encrypt 100MB of data using AES-128 CBC.");
        System.out.println("========================================================\n");

        assertNotNull(encryptedData);
    }

    @Test
    void testDecryptionPerformance() throws Exception {
        byte[] encryptedData = encryptData(testData);
        long startTime = System.nanoTime();
        byte[] decryptedData = decryptData(encryptedData);
        long endTime = System.nanoTime();
        double durationMs = (endTime - startTime) / 1_000_000.0;

        System.out.println("================ Decryption Performance ================");
        System.out.printf("Decryption Time: %.3f ms\n", durationMs);
        System.out.println("Description: Measures the time required to decrypt the previously encrypted 100MB of data.");
        System.out.println("========================================================\n");

        assertArrayEquals(testData, decryptedData);
    }

    @Test
    void testBypassPerformance() {
        long startTime = System.nanoTime();
        byte[] bypassedData = testData; // Just returning the data as is
        long endTime = System.nanoTime();
        double durationMs = (endTime - startTime) / 1_000_000.0;

        System.out.println("================ Bypass Performance ==================");
        System.out.printf("Bypass Time: %.6f ms\n", durationMs);
        System.out.println("Description: Measures the time taken to return the original data without encryption.");
        System.out.println("======================================================\n");

        assertSame(testData, bypassedData);
    }
}
