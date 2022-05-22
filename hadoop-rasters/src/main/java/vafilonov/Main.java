package vafilonov;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import vafilonov.hadooprasters.mapreduce.map.StringMetadataMapper;
import vafilonov.hadooprasters.mapreduce.input.metadata.FileMetadataInputFormat;
import vafilonov.hadooprasters.core.util.JobUtils;
import vafilonov.hadooprasters.frontend.validation.BaseInputDatasetConfigValidator;
import vafilonov.hadooprasters.frontend.validation.ConfigValidator;
import vafilonov.hadooprasters.frontend.model.json.JobInputConfig;

import java.io.File;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static vafilonov.hadooprasters.core.util.PropertyConstants.TEMP_DIR;

public class Main {
	
	public static void main(String[] args) throws Exception {
		ConfigValidator<JobInputConfig> validator = new BaseInputDatasetConfigValidator();

		ObjectMapper mapper = new JsonMapper();
		JobInputConfig jobconf = mapper.readValue(
				new File(Main.class.getClassLoader().getResource("json/test_config.json").getFile()),
				JobInputConfig.class
		);
		System.out.println(jobconf.toString());


		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://localhost:9000");
		System.out.println(conf.get("fs.defaultFS"));
		System.out.println(conf.get(TEMP_DIR.getProperty()));

		String FS = conf.get("fs.defaultFS");
		System.out.println(conf.get("hadoop.tmp.dir"));

		Job job = Job.getInstance(conf, "Metadata test");

		System.out.println(JobUtils.uploadCacheFileToHDFS(new Path(Main.class.getClassLoader().getResource("json/test_config.json").toURI()), conf, "Biba"));

		GenericOptionsParser optionParser = new GenericOptionsParser(conf, args);
		String[] remainingArgs = optionParser.getRemainingArgs();
		if ((remainingArgs.length != 2) && (remainingArgs.length != 4)) {
			System.err.println("Usage: wordcount <in> <out> [-skip skipPatternFile]");
			System.exit(2);
		}
		Job job = Job.getInstance(conf, "Metadata test");
		job.setJarByClass(Main.class);
		job.setMapperClass(StringMetadataMapper.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		URI uri = URI.create(FS + "/libraries/libgdalalljni.so#libgdalalljni.so");
		job.addCacheFile(uri);


		job.setInputFormatClass(FileMetadataInputFormat.class);

		List<String> otherArgs = new ArrayList<String>();
		for (int i=0; i < remainingArgs.length; ++i) {
			if ("-skip".equals(remainingArgs[i])) {
				job.addCacheFile(new Path(remainingArgs[++i]).toUri());
				job.getConfiguration().setBoolean("wordcount.skip.patterns", true);
			} else {
				otherArgs.add(remainingArgs[i]);
			}
		}

		FileInputFormat.addInputPath(job, new Path(FS + otherArgs.get(0)));
		Path p =  new Path(FS + otherArgs.get(1) + "/try" + (1 + new Random().nextInt(1000)));
		FileOutputFormat.setOutputPath(job, p);
		System.out.println(p);
		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}
}
