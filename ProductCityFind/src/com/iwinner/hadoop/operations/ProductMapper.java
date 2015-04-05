package com.iwinner.hadoop.operations;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ProductMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	private Text text=new Text();
	protected void map(LongWritable key, Text value,Mapper<LongWritable, Text, Text, IntWritable>.Context context)throws IOException, InterruptedException {
		// 2012-01-01      09:05   Kansas City     Women's Clothing        293.16  Cash
        String inputValue=value.toString();
        String inputSplit[]=inputValue.trim().split("\t");
        if(inputSplit.length==6){
        	String cityName=inputSplit[2];
        	text.set(cityName);
        	context.write(text, new IntWritable(1))	;
        }
	}

}
 