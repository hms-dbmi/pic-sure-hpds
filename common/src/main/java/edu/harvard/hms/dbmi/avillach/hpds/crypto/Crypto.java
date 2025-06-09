package edu.harvard.hms.dbmi.avillach.hpds.crypto;

import jakarta.annotation.PostConstruct;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;

import javax.crypto.*;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.HashMap;

public class Crypto {

	public static final String DEFAULT_KEY_NAME = "DEFAULT";
	private static final String DEFAULT_ENCRYPTION_KEY_PATH = "/opt/local/hpds/encryption_key";

	private static final Logger LOGGER = LoggerFactory.getLogger(Crypto.class);
	private static final HashMap<String, byte[]> keys = new HashMap<>();

	@Value("${encryption.enabled:true}")
	private boolean encryptionEnabled;

	public static boolean ENCRYPTION_ENABLED = true;

	@PostConstruct
	public void init() {
		ENCRYPTION_ENABLED = encryptionEnabled;
		LOGGER.info("ENCRYPTION_ENABLED set to: {}", ENCRYPTION_ENABLED);
		loadDefaultKey();
	}

	/**
	 * Loads the default encryption key from the predefined file path.
	 * <p>
	 * This method checks if encryption is enabled before attempting to load the key.
	 * If encryption is disabled, no action is taken.
	 * <p>
	 * The key is loaded using {@link #loadKey(String, String)} with the default key name
	 * and default encryption key file path.
	 */
	public static void loadDefaultKey() {
		if (ENCRYPTION_ENABLED) {
			loadKey(DEFAULT_KEY_NAME, DEFAULT_ENCRYPTION_KEY_PATH);
		}
	}

	/**
	 * Loads an encryption key from the specified file path and stores it in memory.
	 * <p>
	 * The key is read as a string from the file, trimmed of any extra spaces, and
	 * converted into a byte array before being stored in the key map.
	 * <p>
	 * If the key file is not found or an error occurs while reading, an error is logged.
	 *
	 * @param keyName  The name under which the key will be stored.
	 * @param filePath The file path from which the encryption key is loaded.
	 */
	public static void loadKey(String keyName, String filePath) {
		try {
			setKey(keyName, IOUtils.toString(new FileInputStream(filePath), Charset.forName("UTF-8")).trim().getBytes());
			LOGGER.info("****LOADED CRYPTO KEY****");
		} catch (IOException e) {
			LOGGER.error("****CRYPTO KEY NOT FOUND****", e);
		}
	}

	/**
	 * Encrypts the given plaintext using the default encryption key.
	 * <p>
	 * If encryption is disabled, the plaintext is returned as-is.
	 * This method delegates encryption to {@link #encryptData(String, byte[])}
	 * using the default key.
	 *
	 * @param plaintext The byte array to be encrypted.
	 * @return The encrypted byte array, or the original plaintext if encryption is disabled.
	 */
	public static byte[] encryptData(byte[] plaintext) {
		return encryptData(DEFAULT_KEY_NAME, plaintext);
	}

	/**
	 * Encrypts the given plaintext using the specified encryption key.
	 * <p>
	 * This method uses AES/GCM/NoPadding encryption with a randomly generated IV.
	 * The IV is included in the output for decryption purposes.
	 * <p>
	 * The method returns a byte array structured as follows:
	 * - First 4 bytes: The length of the IV.
	 * - Next IV-length bytes: The IV itself.
	 * - Remaining bytes: The encrypted ciphertext.
	 * <p>
	 * If encryption is disabled, the plaintext is returned unmodified.
	 *
	 * @param keyName   The name of the encryption key to use.
	 * @param plaintext The byte array containing the data to encrypt.
	 * @return The encrypted byte array, or the original plaintext if encryption is disabled.
	 * @throws RuntimeException If an error occurs during encryption.
	 */
	public static byte[] encryptData(String keyName, byte[] plaintext) {
		if (!ENCRYPTION_ENABLED) {
			return plaintext;
		}

		byte[] key = keys.get(keyName);
		SecureRandom secureRandom = new SecureRandom();
		SecretKey secretKey = new SecretKeySpec(key, "AES");
		byte[] iv = new byte[12]; // NEVER REUSE THIS IV WITH SAME KEY
		secureRandom.nextBytes(iv);
		byte[] cipherText;
		Cipher cipher;
		try {
			cipher = Cipher.getInstance("AES/GCM/NoPadding");
			GCMParameterSpec parameterSpec = new GCMParameterSpec(128, iv); // 128-bit auth tag length
			cipher.init(Cipher.ENCRYPT_MODE, secretKey, parameterSpec);
			cipherText = new byte[cipher.getOutputSize(plaintext.length)];
			cipher.doFinal(plaintext, 0, plaintext.length, cipherText, 0);
			LOGGER.debug("Length of cipherText : " + cipherText.length);
			ByteBuffer byteBuffer = ByteBuffer.allocate(4 + iv.length + cipherText.length);
			byteBuffer.putInt(iv.length);
			byteBuffer.put(iv);
			byteBuffer.put(cipherText);
			byte[] cipherMessage = byteBuffer.array();
			return cipherMessage;
		} catch (NoSuchAlgorithmException | NoSuchPaddingException | InvalidKeyException | InvalidAlgorithmParameterException | ShortBufferException | IllegalBlockSizeException | BadPaddingException e) {
			throw new RuntimeException("Exception while trying to encrypt data : ", e);
		}
	}

	/**
	 * Decrypts the provided data using the default encryption key.
	 * <p>
	 * If encryption is disabled, the method returns the input data as-is.
	 *
	 * @param data The byte array to be decrypted.
	 * @return The decrypted byte array, or the original data if encryption is disabled.
	 */
	public static byte[] decryptData(byte[] data) {
		return decryptData(DEFAULT_KEY_NAME, data);
	}

	/**
	 * Decrypts the provided data using the specified encryption key.
	 * <p>
	 * If encryption is disabled, the method returns the input data as-is.
	 * <p>
	 * The method assumes the input data is structured as follows:
	 * - First 4 bytes: The length of the IV (Initialization Vector).
	 * - Next IV-length bytes: The actual IV.
	 * - Remaining bytes: The ciphertext.
	 *
	 * @param keyName The name of the encryption key to use for decryption.
	 * @param data The byte array containing the encrypted data.
	 * @return The decrypted byte array, or the original data if encryption is disabled.
	 * @throws RuntimeException If an error occurs during decryption.
	 */
	public static byte[] decryptData(String keyName, byte[] data) {
		if (!ENCRYPTION_ENABLED) {
			return data;
		}

		byte[] key = keys.get(keyName);
		ByteBuffer byteBuffer = ByteBuffer.wrap(data);
		int ivLength = byteBuffer.getInt();
		byte[] iv = new byte[ivLength];
		byteBuffer.get(iv);
		byte[] cipherText = new byte[byteBuffer.remaining()];
		byteBuffer.get(cipherText);
		Cipher cipher;
		try {
			cipher = Cipher.getInstance("AES/GCM/NoPadding");
			cipher.init(Cipher.DECRYPT_MODE, new SecretKeySpec(key, "AES"), new GCMParameterSpec(128, iv));
			return cipher.doFinal(cipherText);
		} catch (NoSuchAlgorithmException | NoSuchPaddingException | InvalidKeyException | InvalidAlgorithmParameterException |
				 IllegalBlockSizeException | BadPaddingException e) {
			throw new RuntimeException("Exception caught trying to decrypt data : " + e, e);
		}
	}

	private static void setKey(String keyName, byte[] key) {
		keys.put(keyName, key);
	}

	public static boolean hasKey(String keyName) {
		if (!ENCRYPTION_ENABLED) {
			return true;
		} else {
			return keys.containsKey(keyName);
		}
	}
}