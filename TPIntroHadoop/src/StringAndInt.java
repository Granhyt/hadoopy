public class StringAndInt implements Comparable<StringAndInt> {
	String tag;
	int number;
	
	public StringAndInt() {}
	
	public StringAndInt(String tag, int number) {
		this.tag = tag;
		this.number = number;
	}

	@Override
	public int compareTo(StringAndInt o) {
		return o.number - number;
	}

}
