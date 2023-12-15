package org.guru.Tp.partie2.Question2_1;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class StringAndInt implements Comparable<StringAndInt>, Writable {

    public String tag;
    public Integer occurrence;

    public StringAndInt() {}

    public StringAndInt(String tag) {
        this.tag = tag;
        this.occurrence = 1;
    }

    public StringAndInt(String tag, int occurence) {
        this.tag = tag;
        this.occurrence = occurence;
    }

    @Override
    public int compareTo(StringAndInt stringAndInt) {
        return stringAndInt.occurrence.compareTo(occurrence);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        tag = dataInput.readUTF();
        occurrence = dataInput.readInt();
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(tag);
        dataOutput.writeInt(occurrence);
    }

    @Override
    public String toString() {
        return tag + " : " + occurrence;
    }

}
