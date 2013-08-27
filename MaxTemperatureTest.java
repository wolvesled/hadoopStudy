import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;

import org.junit.*;

public class MaxTemperatureMapperTest {

	@Test
	public void processesValidRecord() throws IOException, InterruptedException {
		Text value = new Text("0043011990999991950051518004+68750+023550FM-12+038299999V0203201N00261220001CN9999999N9-00111+99999999999");
		//                               Year ^^^^                                                        Temperature ^^^^^
		new MapDriver<LongWritable, Text, Text, IntWritable>()
			.withMapper(new MaxTemperatureMapper())
			.withInputValue(value)
			.withOutput(new Text("1950"), new IntWritable(-11))
			.runTest();
	
	@Test
	public void ignoreMissingTemperatureRecord() throws IOException, InterruptedException {
		Text value = new Text("0043011990999991950051518004+68750+023550FM-12+038299999V0203201N00261220001CN9999999N9+99991+99999999999");
		//                               Year ^^^^                                                        Temperature ^^^^^
		new MapDriver<LongWritable, Text, Text, IntWritable>()
			.withMapper(new MaxTemperatureMapper())
			.withInputValue(value)
			.runTest();
	}
}

public class MaxTemperatureReducerTest {

	@Test
	public void returnsMaximumIntegerInValues() throws IOException, InterruptedException {
		new ReducerDriver<Text, IntWritable, Text, IntWritable>()
			.withReducer(new MaxTemperatureReducer())
			.withInputKey(new Text("1950"))
			.withInputValues(Arrays.asList(new IntWritable(10), new IntWritable(5)))
			.withOutput(new Text("1950"), new Intwritable(10))
			.runTest();
	}
}
