package edu.harvard.hms.dbmi.avillach.hpds.crypto;


import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.security.*;
import java.util.HashMap;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKey;
import javax.crypto.ShortBufferException;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Crypto {

	public static final String DEFAULT_KEY_NAME = "DEFAULT";

	// This needs to be set in a static initializer block to be overridable in tests.
	private static final String DEFAULT_ENCRYPTION_KEY_PATH;
	static{
		DEFAULT_ENCRYPTION_KEY_PATH = "/opt/local/hpds/encryption_key";
	}

	private static final Logger LOGGER = LoggerFactory.getLogger(Crypto.class);

	private static final HashMap<String, byte[]> keys = new HashMap<String, byte[]>();

	public static void loadDefaultKey() {
		loadKey(DEFAULT_KEY_NAME, DEFAULT_ENCRYPTION_KEY_PATH);
	}

	public static void loadKey(String keyName, String filePath) {
		try {
			setKey(keyName, IOUtils.toString(new FileInputStream(filePath), Charset.forName("UTF-8")).trim().getBytes());
			LOGGER.info("****LOADED CRYPTO KEY****");	
		} catch (IOException e) {
			LOGGER.error("****CRYPTO KEY NOT FOUND****", e);
		}
	}

	public static byte[] encryptData(byte[] plaintextBytes) {
		return encryptData(DEFAULT_KEY_NAME, plaintextBytes);
	}

	public static byte[] encryptData(String keyName, byte[] plaintextBytes) {
		byte[] key = keys.get(keyName);
		if (key == null) {
			throw new IllegalStateException("No key loaded for: " + keyName);
		}

		SecretKey secretKey = new SecretKeySpec(key, "AES");

		byte[] iv = new byte[12]; // NEVER REUSE THIS IV WITH SAME KEY
		new SecureRandom().nextBytes(iv);

		try {
			Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
			GCMParameterSpec parameterSpec = new GCMParameterSpec(128, iv); // 128-bit auth tag length
			cipher.init(Cipher.ENCRYPT_MODE, secretKey, parameterSpec);

			// Allocate the final message once: [ivLen:int][iv][ciphertext+tag]
			int ctLen = cipher.getOutputSize(plaintextBytes.length);
			byte[] cipherMessage = new byte[4 + iv.length + ctLen];

			// header + iv
			ByteBuffer.wrap(cipherMessage, 0, 4).putInt(iv.length);
			System.arraycopy(iv, 0, cipherMessage, 4, iv.length);

			// write ciphertext directly into final array (avoids a second large allocation/copy)
			cipher.doFinal(plaintextBytes, 0, plaintextBytes.length, cipherMessage, 4 + iv.length);

			LOGGER.debug("Length of cipherText : {}", ctLen);
			return cipherMessage;
		} catch (NoSuchAlgorithmException | NoSuchPaddingException | InvalidKeyException
				 | InvalidAlgorithmParameterException | ShortBufferException
				 | IllegalBlockSizeException | BadPaddingException e) {
			throw new RuntimeException("Exception while trying to encrypt data : ", e);
		}
	}

	public static byte[] decryptData(byte[] encrypted) {
		return decryptData(DEFAULT_KEY_NAME, encrypted);
	}

	public static byte[] decryptData(String keyName, byte[] encrypted) {
		byte[] key = keys.get(keyName);

		if (encrypted.length < 4) {
			throw new IllegalArgumentException("Encrypted payload too small");
		}

		ByteBuffer bb = ByteBuffer.wrap(encrypted);
		int ivLength = bb.getInt();

		// GCM IV is typically 12 bytes; allow a small range if you need flexibility
		if (ivLength < 12 || ivLength > 32) {
			throw new IllegalArgumentException("Invalid ivLength: " + ivLength);
		}
		if (encrypted.length < 4 + ivLength + 16) { // 16 = GCM tag
			throw new IllegalArgumentException("Encrypted payload truncated");
		}

		byte[] iv = new byte[ivLength];
		bb.get(iv);

		int ctOffset = 4 + ivLength;
		int ctLen = encrypted.length - ctOffset;

		try {
			Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
			cipher.init(Cipher.DECRYPT_MODE, new SecretKeySpec(key, "AES"), new GCMParameterSpec(128, iv));
			return cipher.doFinal(encrypted, ctOffset, ctLen);
		} catch (GeneralSecurityException e) {
			throw new RuntimeException("Exception caught trying to decrypt data : " + e, e);
		}
	}
	private static void setKey(String keyName, byte[] key) {
		keys.put(keyName, key);
	}

	public static boolean hasKey(String keyName) {
		return keys.containsKey(keyName);
	}

}
