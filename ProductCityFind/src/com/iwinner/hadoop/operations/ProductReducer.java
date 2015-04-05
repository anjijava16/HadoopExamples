package com.iwinner.hadoop.operations;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ProductReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
	public void reduce(Text text, Iterable<IntWritable> value,Reducer<Text, IntWritable, Text, IntWritable>.Context context)	throws IOException, InterruptedException {
	int sum=0;
	for(IntWritable intSum:value){
		sum=sum+intSum.get();
	}
	context.write(text,new IntWritable(sum));
}
}
