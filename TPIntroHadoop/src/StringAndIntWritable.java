import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class StringAndIntWritable implements Comparable<StringAndIntWritable>, Writable {
	Text tag = new Text();
	IntWritable number = new IntWritable();
	
	public StringAndIntWritable() {}
	
	public void set(Text tag, IntWritable number) {
		this.tag = tag;
		this.number = number;
	}

	@Override
	public int compareTo(StringAndIntWritable o) {
		return o.number.get() - number.get();
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		tag.readFields(in);
		number.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		tag.write(out);
		number.write(out);
	}

}
