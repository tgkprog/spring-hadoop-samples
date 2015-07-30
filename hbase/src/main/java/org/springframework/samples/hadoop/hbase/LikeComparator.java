package org.springframework.samples.hadoop.hbase;

import org.apache.hadoop.hbase.filter.ByteArrayComparable;

public class LikeComparator extends ByteArrayComparable {

	private String val = null;
	private byte[] valb = null;

	public LikeComparator(byte[] value) {
		super(value);
		valb = value;
		val = new String(valb);// charset??
	}

	// ?TODO
	@Override
	public byte[] toByteArray() {
		System.err.println("toByteArray LikeComparator toByteArray " + this);
		return valb;
	}
	
	public static ByteArrayComparable parseFrom(byte[] pbBytes){
		System.err.println("parseFrom LikeComparator toByteArray " + new String(pbBytes));
		return new LikeComparator(pbBytes);
	}

	@Override
	public int compareTo(byte[] value, int offset, int length) {
		String that = new String(value);// charset??
		if (that.indexOf(val) > -1) {
			return 0;
		}
		return that.compareTo(val);
	}

}
