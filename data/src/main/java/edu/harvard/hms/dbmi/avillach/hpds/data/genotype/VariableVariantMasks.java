package edu.harvard.hms.dbmi.avillach.hpds.data.genotype;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.ser.std.ToStringSerializer;

import java.io.Serializable;
import java.math.BigInteger;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

public class VariableVariantMasks implements Serializable {
	private static final long serialVersionUID = 6225420483804601477L;
	private static final String oneone = "11";
	private static final char one = '1';
	private static final char zero = '0';
	private static final String hetero = "0/1";
	private static final String heteroDel = "1/0";
	private static final String heteroPhased = "0|1";
	private static final String heteroPhased2 = "1|0";
	private static final String homo = "1/1";
	private static final String homoPhased = "1|1";
	private static final String homoNoCall = "./.";
	private static final String heteroNoCall = "./1";

	private static Map<Integer, BigInteger> emptyBitmaskMap = new ConcurrentHashMap<>();

	private static int SPARSE_VARIANT_THRESHOLD = 5;

	/*
	 * These indices result from the char values of the 3 characters in a VCF sample
	 * record summed as integers % 7
	 *
	 * This allows us to not actually do string comparisons, instead we add 3 values,
	 * do one modulus operation, then use the result as the index into the result array.
	 */

	// ./0 = (46 + 47 + 48) % 7 = 1
	// 0|. = (48 + 124 + 46) % 7 = 1
	// .|0 = (46 + 124 + 48) % 7 = 1
	public static final int HETERO_NOCALL_REF_CHAR_INDEX = 1;

	// ./1 = (46 + 47 + 49) % 7 = 2
	// 1|. = (49 + 124 + 46) % 7 = 2
	// .|1 = (46 + 124 + 49) % 7 = 2
	// ./1 = (46 + 47 + 49) % 7 = 2
	// 1|. = (49 + 124 + 46) % 7 = 2
	// .|1 = (46 + 124 + 49) % 7 = 2
	public static final int HETERO_NOCALL_VARIANT_CHAR_INDEX = 2;

	// 0/0 = (48 + 47 + 48) % 7 = 3
	// 0|0 = (48 + 124 + 48) % 7 = 3
	public static final int ZERO_ZERO_CHAR_INDEX = 3;

	// 0/1 = (48 + 47 + 49) % 7 = 4
	// 1|0 = (49 + 124 + 48) % 7 = 4
	// 0|1 = (48 + 124 + 49) % 7 = 4
	public static final int ZERO_ONE_CHAR_INDEX = 4;

	// 1/1 = (49 + 47 + 49) % 7 = 5
	// 1|1 = (49 + 124 + 49) % 7 = 5
	public static final int ONE_ONE_CHAR_INDEX = 5;

	// ./. = (46 + 47 + 46) % 7 = 6
	// .|. = (46 + 124 + 46) % 7 = 6
	public static final int HOMO_NOCALL_CHAR_INDEX = 6;

	public VariableVariantMasks(String[] values) {
		char[] homozygousBits = new char[values.length];
		char[] heterozygousBits = new char[values.length];
		char[] homozygousNoCallBits = new char[values.length];
		char[] heterozygousNoCallBits = new char[values.length];
		length = values.length;
		boolean hasHetero = false;
		boolean hasHomo = false;
		boolean hasHeteroNoCall = false;
		boolean hasHomoNoCall = false;

		for(int x = 0;x<values.length;x++) {
			homozygousBits[x]=zero;
			heterozygousBits[x]=zero;
			homozygousNoCallBits[x]=zero;
			heterozygousNoCallBits[x]=zero;
			if(values[x]!=null) {
				switch(values[x]) {
				case hetero:{
					heterozygousBits[x] = one;
					hasHetero = true;
					break;
				}
				case heteroPhased:{
					heterozygousBits[x] = one;
					hasHetero = true;
					break;
				}
				case heteroPhased2:{
					heterozygousBits[x] = one;
					hasHetero = true;
					break;
				}
				case heteroDel:{
					heterozygousBits[x] = one;
					hasHetero = true;
					break;
				}
				case homo:{
					homozygousBits[x] = one;
					hasHomo = true;
					break;
				}
				case homoPhased:{
					homozygousBits[x] = one;
					hasHomo = true;
					break;
				}
				case heteroNoCall:{
					heterozygousNoCallBits[x] = one;
					hasHeteroNoCall = true;
					break;
				}
				case homoNoCall:{
					homozygousNoCallBits[x] = one;
					hasHomoNoCall = true;
					break;
				}
				}
			}

		}
		if(hasHetero) {
			StringBuilder heteroString = new StringBuilder(values.length + 4);
			heteroString.append(oneone);
			heteroString.append(heterozygousBits);
			heteroString.append(oneone);
			heterozygousMask = new VariantMaskBitmaskImpl(new BigInteger(heteroString.toString(), 2));
		}
		if(hasHomo) {
			StringBuilder homoString = new StringBuilder(values.length + 4);
			homoString.append(oneone);
			homoString.append(homozygousBits);
			homoString.append(oneone);
			homozygousMask = new VariantMaskBitmaskImpl(new BigInteger(homoString.toString(), 2));
		}
		if(hasHomoNoCall) {
			StringBuilder homoNoCallString = new StringBuilder(values.length + 4);
			homoNoCallString.append(oneone);
			homoNoCallString.append(homozygousNoCallBits);
			homoNoCallString.append(oneone);
			homozygousNoCallMask = new VariantMaskBitmaskImpl(new BigInteger(homoNoCallString.toString(), 2));
		}
		if(hasHeteroNoCall) {
			StringBuilder heteroNoCallString = new StringBuilder(values.length + 4);
			heteroNoCallString.append(oneone);
			heteroNoCallString.append(heterozygousNoCallBits);
			heteroNoCallString.append(oneone);
			heterozygousNoCallMask = new VariantMaskBitmaskImpl(new BigInteger(heteroNoCallString.toString(), 2));
		}
	}

	public VariableVariantMasks(char[][] maskValues) {
		String heteroMaskStringRaw = new String(maskValues[ZERO_ONE_CHAR_INDEX]);
		String homoMaskStringRaw = new String(maskValues[ONE_ONE_CHAR_INDEX]);
		String heteroNoCallMaskStringRaw = new String(maskValues[HETERO_NOCALL_VARIANT_CHAR_INDEX]);
		String homoNoCallMaskStringRaw = new String(maskValues[HOMO_NOCALL_CHAR_INDEX]);
		if(heteroMaskStringRaw.contains("1")) {
			heterozygousMask = new VariantMaskBitmaskImpl(new BigInteger(oneone + heteroMaskStringRaw + oneone, 2));
		}
		if(homoMaskStringRaw.contains("1")) {
			homozygousMask = new VariantMaskBitmaskImpl(new BigInteger(oneone + homoMaskStringRaw + oneone, 2));
		}
		if(heteroNoCallMaskStringRaw.contains("1")) {
			heterozygousNoCallMask = new VariantMaskBitmaskImpl(new BigInteger(oneone + heteroNoCallMaskStringRaw + oneone, 2));
		}
		if(homoNoCallMaskStringRaw.contains("1")) {
			homozygousNoCallMask = new VariantMaskBitmaskImpl(new BigInteger(oneone + homoNoCallMaskStringRaw + oneone, 2));
		}

	}

	public VariableVariantMasks() {
	}


	@JsonProperty("ho")
	public VariantMask homozygousMask;

	@JsonProperty("he")
	public VariantMask heterozygousMask;

	@JsonProperty("hon")
	public VariantMask homozygousNoCallMask;

	@JsonProperty("hen")
	public VariantMask heterozygousNoCallMask;

	@JsonProperty("l")
	public int length;



	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		VariableVariantMasks that = (VariableVariantMasks) o;
		return Objects.equals(homozygousMask, that.homozygousMask) && Objects.equals(heterozygousMask, that.heterozygousMask) && Objects.equals(homozygousNoCallMask, that.homozygousNoCallMask) && Objects.equals(heterozygousNoCallMask, that.heterozygousNoCallMask);
	}

	@Override
	public int hashCode() {
		return Objects.hash(homozygousMask, heterozygousMask, homozygousNoCallMask, heterozygousNoCallMask);
	}

	public BigInteger emptyBitmask(int length) {
		BigInteger emptyBitmask = emptyBitmaskMap.get(length);
		if (emptyBitmask == null) {
			String emptyVariantMask = "";
			for (int i = 0; i < length; i++) {
				emptyVariantMask = emptyVariantMask + "0";
			}
			BigInteger newEmptyBitmask = new BigInteger("11" + emptyVariantMask + "11", 2);
			emptyBitmaskMap.put(length, newEmptyBitmask);
			return newEmptyBitmask;
		}
		return emptyBitmask;
	}

	/**
	 * Appends one mask to another. This assumes the masks are both padded with '11' on each end
	 * to prevent overflow issues.
	 */
	public BigInteger appendMask(BigInteger mask1, int mask1Length, BigInteger mask2, int mask2length) {
		if (mask1 == null && mask2 == null) {
			return null;
		}
		if (mask1 == null) {
			// todo: unit test this funcitonality
			mask1 = emptyBitmask(mask1Length);
		}
		if (mask2 == null) {
			mask2 = emptyBitmask(mask2length);
		}
		String binaryMask1 = mask1.toString(2);
		String binaryMask2 = mask2.toString(2);
		String appendedString = binaryMask1.substring(0, binaryMask1.length() - 2) +
				binaryMask2.substring(2);
		return new BigInteger(appendedString, 2);
	}

	public VariableVariantMasks append(VariableVariantMasks variantMasks) {
		VariableVariantMasks appendedMasks = new VariableVariantMasks();
		appendedMasks.homozygousMask = appendMask(this.homozygousMask, variantMasks.homozygousMask);
		appendedMasks.heterozygousMask = appendMask(this.heterozygousMask, variantMasks.heterozygousMask);
		appendedMasks.homozygousNoCallMask = appendMask(this.homozygousNoCallMask, variantMasks.homozygousNoCallMask);
		appendedMasks.heterozygousNoCallMask = appendMask(this.heterozygousNoCallMask, variantMasks.heterozygousNoCallMask);
		return appendedMasks;
	}

	private VariantMask appendMask(VariantMask homozygousMask, VariantMask homozygousMask1) {
		return null;
	}
}
