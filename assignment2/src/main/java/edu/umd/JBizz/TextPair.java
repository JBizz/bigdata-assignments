package edu.umd.JBizz;

import java.io.*;
import org.apache.hadoop.io.*;

public class TextPair implements WritableComparable<TextPair> {

	private Text first;
	private Text second;

	public TextPair(){
		set(new Text(), new Text());
	}

	public TextPair(String first, String second){
		set(new Text(first), new Text(second));
	}

	public TextPair(Text first, Text second){
		set(first, second);
	}

	public void set(Text first, Text second){
		this.first = first;
		this.second = second;
	}

	public Text getFirst(){
		return first;
	}

	public Text getSecond(){
		return second;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		first.write(out);
		second.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		first.readFields(in);
		second.readFields(in);
	}

	@Override
	public int hashCode() {
		return first.hashCode();
	}

	@Override
	public boolean equals(Object o) {
		if(o instanceof TextPair) {
			TextPair tp = (TextPair) o;
			return first.equals(tp.first) && second.equals(tp.second);
		}
		return false;
	}

	@Override
    public int compareTo(TextPair tp){
        if(second.equals("*") && tp.getSecond().equals("*")){
            return 0;
          }
          else{
            if (second.equals("*")){
              return -1;
            }
            if (tp.getSecond().equals("*")){
              return 1;
            }
          }
        if(first.equals(tp.getFirst())){
            return second.compareTo(tp.getSecond());
        }
        return first.compareTo(tp.getFirst());
    }
	public String toString(){
		return first + "\t" + second;
	}

	@Override
	public int compareTo(TextPair tp){
		if(second.equals("*") && tp.getSecond.equals("*")){
	        return 0;
	      }
	      else{
	        if (second.equals("*")){
	          return -1;
	        } 
	        if (tp.getSecond.equals("*")){
	          return 1;
	        }
	      }
		if(first.equals(tp.getFirst)){
			return second.equals(tp.getSecond());
		}
		return first.equals(tp.getFirst());
	}
}