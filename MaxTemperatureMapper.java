import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MaxTemeratureMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	/*
	private static final int MISSING = 9999;
	*/
	
	private NcdcRecordParser parser = new NcdcRecordParser();
	
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	
		parser.parse(value);
		if (parser.isValidTemperature()) {
			context.write(new Text(parser.getYear()), new IntWritable(parser.getAirTemperature()));
		}
		
		/*
		String line = value.toString();
		String year = line.substring(15, 19);
		int airTemperature;
		if (line.charAt(87) == '+') { // parseInt doesn't like leading plus signs before Java 7
			airTemperature = Integer.parseInt(line.substring(88, 92));
		} else {
			airTemperature = Integer.parseInt(line.substring(87, 92));
		}
		String quality = line.substring(92,93);
		if (airTemperature != MISSING && quality.matches("[01459]")) {
			context.write(new Text(year), new IntWritable(airTemperature));
		}
		*/
	}
}
