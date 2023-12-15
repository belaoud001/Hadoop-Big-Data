package org.guru.Tp.partie2.Question2_2;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class StringAndInt implements Comparable<StringAndInt>, Writable {

    public String tag;
    public Integer occurence;

    public StringAndInt() {}

    public StringAndInt(String tag) {
        this.tag = tag;
        this.occurence = 1;
    }

    public StringAndInt(String tag, int occurence) {
        this.tag = tag;
        this.occurence = occurence;
    }

    @Override
    public int compareTo(StringAndInt stringAndInt) {
        return stringAndInt.occurence.compareTo(occurence);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        tag = dataInput.readUTF();
        occurence = dataInput.readInt();
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(tag);
        dataOutput.writeInt(occurence);
    }

    @Override
    public String toString() {
        return tag + " : " + occurence;
    }

}