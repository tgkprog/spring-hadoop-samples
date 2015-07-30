package org.springframework.samples.hadoop.hbase;

import java.util.List;

import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.ByteArrayComparable;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.hadoop.hbase.HbaseTemplate;
import org.springframework.data.hadoop.hbase.RowMapper;
import org.springframework.data.hadoop.hbase.TableCallback;
import org.springframework.stereotype.Repository;

@Repository
public class UserRepository {

	@Autowired
	private HbaseTemplate hbaseTemplate;

	private String tableName = "users";

	public static byte[] CF_INFO = Bytes.toBytes("cfInfo");

	private byte[] qUser = Bytes.toBytes("user");
	private byte[] qEmail = Bytes.toBytes("email");
	private byte[] qPassword = Bytes.toBytes("password");

	public List<User> findAll() {
		return hbaseTemplate.find(tableName, "cfInfo", new RowMapper<User>() {
			@Override
			public User mapRow(Result result, int rowNum) throws Exception {
				return new User(Bytes.toString(result.getValue(CF_INFO, qUser)), Bytes.toString(result.getValue(CF_INFO, qEmail)), Bytes
						.toString(result.getValue(CF_INFO, qPassword)));
			}
		});

	}

	public List<User> findEmailLike(String s) {

		//ByteArrayComparable comparer = new LikeComparator(s.getBytes());
		//new SingleColumnValueFilter(CF_INFO, qEmail, CompareOp.EQUAL, comparer);
		org.apache.hadoop.hbase.filter.Filter filt = new SingleColumnValueFilter(CF_INFO, qEmail, CompareOp.EQUAL, s.getBytes());
		//SingleColumnValueFilter filt2 = new SingleColumnValueFilter();
		org.apache.hadoop.hbase.client.Scan scan = new Scan();
		scan.setFilter(filt);
		scan.addFamily(CF_INFO);

		return hbaseTemplate.find(tableName, scan, new RowMapper<User>() {
			@Override
			public User mapRow(Result result, int rowNum) throws Exception {
				return new User(Bytes.toString(result.getValue(CF_INFO, qUser)), Bytes.toString(result.getValue(CF_INFO, qEmail)), Bytes
						.toString(result.getValue(CF_INFO, qPassword)));
			}
		});

	}

	public User save(final String userName, final String email, final String password) {
		return hbaseTemplate.execute(tableName, new TableCallback<User>() {
			public User doInTable(HTableInterface table) throws Throwable {
				User user = new User(userName, email, password);
				Put p = new Put(Bytes.toBytes(user.getName()));
				p.add(CF_INFO, qUser, Bytes.toBytes(user.getName()));
				p.add(CF_INFO, qEmail, Bytes.toBytes(user.getEmail()));
				p.add(CF_INFO, qPassword, Bytes.toBytes(user.getPassword()));
				table.put(p);
				return user;

			}
		});
	}

}
