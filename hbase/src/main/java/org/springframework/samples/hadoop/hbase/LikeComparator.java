package org.springframework.samples.hadoop.hbase;

import org.apache.hadoop.hbase.filter.ByteArrayComparable;
import org.apache.hadoop.hbase.protobuf.generated.ComparatorProtos;
import org.apache.hadoop.hbase.util.ByteStringer;

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
		ComparatorProtos.BinaryComparator.Builder builder = ComparatorProtos.BinaryComparator.newBuilder();
		builder.setComparable(convert1());
		return builder.build().toByteArray();

	}

	ComparatorProtos.ByteArrayComparable convert1() {
		ComparatorProtos.ByteArrayComparable.Builder builder = ComparatorProtos.ByteArrayComparable.newBuilder();
		if (valb != null)
			builder.setValue(ByteStringer.wrap(valb));
		return builder.build();
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
