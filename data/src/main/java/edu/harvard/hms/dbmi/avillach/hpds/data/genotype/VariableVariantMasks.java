package edu.harvard.hms.dbmi.avillach.hpds.data.genotype;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;
import java.math.BigInteger;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

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

	public static int SPARSE_VARIANT_THRESHOLD = 5;

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

	public VariableVariantMasks(char[][] maskValues) {
		String heteroMaskStringRaw = new String(maskValues[ZERO_ONE_CHAR_INDEX]);
		String homoMaskStringRaw = new String(maskValues[ONE_ONE_CHAR_INDEX]);
		String heteroNoCallMaskStringRaw = new String(maskValues[HETERO_NOCALL_VARIANT_CHAR_INDEX]);
		String homoNoCallMaskStringRaw = new String(maskValues[HOMO_NOCALL_CHAR_INDEX]);

		heterozygousMask = variantMaskFromRawString(heteroMaskStringRaw);
		homozygousMask = variantMaskFromRawString(homoMaskStringRaw);
		heterozygousNoCallMask = variantMaskFromRawString(heteroNoCallMaskStringRaw);
		homozygousNoCallMask = variantMaskFromRawString(homoNoCallMaskStringRaw);
	}

	private VariantMask variantMaskFromRawString(String maskStringRaw) {
		if (!maskStringRaw.contains("1")) {
			return new VariantMaskSparseImpl(Set.of());
		}

		VariantMask variantMask;
		BigInteger bitmask = new BigInteger(oneone + maskStringRaw + oneone, 2);
		if (bitmask.bitCount() - 4 > VariableVariantMasks.SPARSE_VARIANT_THRESHOLD) {
			variantMask = new VariantMaskBitmaskImpl(bitmask);
		} else {
			Set<Integer> patientIndexes = new HashSet<>();
			for(int i = 2; i < bitmask.bitLength() - 2; i++) {
				if (bitmask.testBit(i)) {
					patientIndexes.add(i);
				}
			}
			variantMask = new VariantMaskSparseImpl(patientIndexes);
		}
		return variantMask;
	}

	public VariableVariantMasks() {
	}

	public VariableVariantMasks(int length) {
		this.length = length;
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

	public static BigInteger emptyBitmask(int length) {
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
		appendedMasks.homozygousMask = appendMask(this.homozygousMask, variantMasks.homozygousMask, this.length, variantMasks.length);
		appendedMasks.heterozygousMask = appendMask(this.heterozygousMask, variantMasks.heterozygousMask, this.length, variantMasks.length);
		appendedMasks.homozygousNoCallMask = appendMask(this.homozygousNoCallMask, variantMasks.homozygousNoCallMask, this.length, variantMasks.length);
		appendedMasks.heterozygousNoCallMask = appendMask(this.heterozygousNoCallMask, variantMasks.heterozygousNoCallMask, this.length, variantMasks.length);
		return appendedMasks;
	}

	public static VariantMask appendMask(VariantMask variantMask1, VariantMask variantMask2, int length1, int length2) {
		if (variantMask1 instanceof  VariantMaskSparseImpl) {
			if (variantMask2 instanceof VariantMaskSparseImpl) {
				return append((VariantMaskSparseImpl) variantMask1, (VariantMaskSparseImpl) variantMask2, length1, length2);
			} else if (variantMask2 instanceof  VariantMaskBitmaskImpl) {
				return append((VariantMaskSparseImpl) variantMask1, (VariantMaskBitmaskImpl) variantMask2, length1, length2);
			} else {
				throw new RuntimeException("Unknown VariantMask implementation");
			}
		}
		// todo: bitmask
		else {
			throw new RuntimeException("Unknown VariantMask implementation");
		}
	}

	private static VariantMask append(VariantMaskSparseImpl variantMask1, VariantMaskBitmaskImpl variantMask2, int length1, int length2) {
		BigInteger mask1 = emptyBitmask(length1);
		for (Integer patientId : variantMask1.patientIndexes) {
			mask1 = mask1.setBit(patientId);
		}
		String binaryMask1 = mask1.toString(2);
		String binaryMask2 = variantMask2.bitmask.toString(2);
		String appendedString = binaryMask2.substring(0, binaryMask1.length() - 2) +
				binaryMask1.substring(2);
		return new VariantMaskBitmaskImpl(new BigInteger(appendedString, 2));
	}
	private static VariantMask append(VariantMaskSparseImpl variantMask1, VariantMaskSparseImpl variantMask2, int length1, int length2) {
		if (variantMask1.patientIndexes.size() + variantMask2.patientIndexes.size() > SPARSE_VARIANT_THRESHOLD) {
			// todo: performance test this vs byte array
			BigInteger mask = emptyBitmask(length1 + length2);
			for (Integer patientId : variantMask1.patientIndexes) {
				mask = mask.setBit(patientId + 2);
			}
			// todo: explain this. it is not intuitive
			for (Integer patientId : variantMask2.patientIndexes) {
				mask = mask.setBit(patientId + length1 + 2);
			}
			return new VariantMaskBitmaskImpl(mask);
		}
		else {
			Set<Integer> patientIndexSet = new HashSet<>();
			patientIndexSet.addAll(variantMask1.patientIndexes);
			patientIndexSet.addAll(variantMask2.patientIndexes);
			return new VariantMaskSparseImpl(patientIndexSet);
		}
	}

	public static Set<Integer> patientMaskToPatientIdSet(VariantMask patientMask, List<String> patientIds) {
		if (patientMask instanceof VariantMaskBitmaskImpl) {
			Set<Integer> ids = new HashSet<>();
			String bitmaskString = ((VariantMaskBitmaskImpl) patientMask).getBitmask().toString(2);
			for(int x = 2;x < bitmaskString.length()-2;x++) {
				if('1'==bitmaskString.charAt(x)) {
					String patientId = patientIds.get(x-2).trim();
					ids.add(Integer.parseInt(patientId));
				}
			}
			return ids;
		} else if (patientMask instanceof VariantMaskSparseImpl) {
			return ((VariantMaskSparseImpl) patientMask).getPatientIndexes().stream()
					.map(patientIds::get)
					.map(String::trim)
					.map(Integer::parseInt)
					.collect(Collectors.toSet());
		}
		throw new IllegalArgumentException("Unknown VariantMask implementation");
	}

/*	if (mask1 == null) {
		mask1 = variantStore1.emptyBitmask();
	}
        if (mask2 == null) {
		mask2 = variantStore2.emptyBitmask();
	}
	String binaryMask1 = mask1.toString(2);
	String binaryMask2 = mask2.toString(2);
	String appendedString = binaryMask1.substring(0, binaryMask1.length() - 2) +
			binaryMask2.substring(2);*/
}
