package partition_new_method;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import partition.Reducer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;

public class Main {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        //1、获取job
        Configuration conf = new Configuration();
        conf.set("mapreduce.reduce.java.opts", "-Xmx4g");
        if (args.length > 2) {
            //设置网格文件路径到配置中，通过命令行指定网格文件
            conf.set("gridFilePath", args[2]);
        }
        Job job = Job.getInstance(conf);

        //2、设置jar包路径
        job.setJarByClass(Main.class);

        //3、关联mapper和reducer
        job.setMapperClass(Mapper.class);
        job.setReducerClass(Reducer.class);

        //4、设置map输出的kv类型
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);

        //5、设置最终输出的kv类型
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        // 读取args[2]指定的文件来设置Reducer的数量
        if (args.length > 2) {
            int numReduceTasks = getLineCount(args[2], conf);
            //网格的行数即是reducer的数量
            job.setNumReduceTasks(numReduceTasks);
            System.out.println("number of reducers: " + numReduceTasks);
        }

        //6、设置输入路径和输出路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        //7、提交job
        boolean result = job.waitForCompletion(true);

        System.exit(result ? 0 : 1);
    }

    //计算得到网格文件的行数
    private static int getLineCount(String filePath, Configuration conf) throws IOException {
        //通过第二个命令行输入打开指定文件
        FileSystem fs = FileSystem.get(URI.create(filePath), conf);
        Path path = new Path(filePath);
        //逐行读取
        BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)));
        //lineCount记录行数
        int lineCount = 0;
        while (br.readLine() != null) {
            lineCount++;
        }
        br.close();
        return lineCount;
    }
}
