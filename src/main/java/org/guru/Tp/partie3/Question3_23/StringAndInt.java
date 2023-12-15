package org.guru.Tp.partie3.Question3_23;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class StringAndInt implements WritableComparable<StringAndInt> {

    public String country = "";
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

    public StringAndInt(String country, String tag, int occurence) {
        this.country = country;
        this.tag = tag;
        this.occurrence = occurence;
    }

    @Override
    public int compareTo(StringAndInt stringAndInt) {
        int compareResult = stringAndInt.occurrence.compareTo(occurrence);

        return compareResult == 0 ? tag.compareTo(stringAndInt.tag) : compareResult;
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        country = dataInput.readUTF();
        tag = dataInput.readUTF();
        occurrence = dataInput.readInt();
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(country);
        dataOutput.writeUTF(tag);
        dataOutput.writeInt(occurrence);
    }

    @Override
    public String toString() {
        return tag + " : " + occurrence;
    }

}