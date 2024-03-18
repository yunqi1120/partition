package partition_new_method;

import com.github.davidmoten.rtree.Entry;
import com.github.davidmoten.rtree.RTree;
import com.github.davidmoten.rtree.geometry.Geometries;
import com.github.davidmoten.rtree.geometry.Geometry;
import com.github.davidmoten.rtree.geometry.Rectangle;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;
import rx.Observable;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;

public class Mapper extends org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, IntWritable, Text> {

    private RTree<Integer, Geometry> rtree;
    private WKTReader wktReader;

    //初始化，创建分区并添加到rtree中
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        rtree = RTree.create();
        wktReader = new WKTReader();

        Configuration conf = context.getConfiguration();
        FileSystem fs = FileSystem.get(URI.create("hdfs://namenode:8020"), conf);
        //从命令行输入中读取网格文件路径
        String gridFilePathStr = conf.get("gridFilePath");
        Path gridFilePath = new Path(gridFilePathStr);
        BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(gridFilePath)));


        String line;
        while ((line = br.readLine()) != null) {
            String[] parts = line.split("\\s+");
            if (parts.length != 5) continue;
            int partitionIndex = Integer.parseInt(parts[0]);//网格索引
            //网格位置
            double lowerLeftX = Double.parseDouble(parts[1]);
            double lowerLeftY = Double.parseDouble(parts[2]);
            double upperRightX = Double.parseDouble(parts[3]);
            double upperRightY = Double.parseDouble(parts[4]);
            Rectangle rect = Geometries.rectangle(lowerLeftX, lowerLeftY, upperRightX, upperRightY);
            //将网格添加到rtree中
            rtree = rtree.add(partitionIndex, rect);
        }
        br.close();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        long startTime = System.currentTimeMillis();

        String line = value.toString();

        //一行=编号+WKT
        String[] parts = line.split(",", 2);
        if (parts.length != 2) {
            return;
        }

        String wkt = parts[1];
        try {
            //WKT->JTS
            org.locationtech.jts.geom.Geometry jtsGeometry = wktReader.read(wkt);

            //转化成rtree可以处理的形式
            Rectangle rtreeGeometry = null;

            //点与多边形->mbr
            if (jtsGeometry instanceof Point || jtsGeometry instanceof Polygon) {
                if (jtsGeometry instanceof Point) {
                    Point point = (Point) jtsGeometry;
                    rtreeGeometry = Geometries.rectangle(point.getX(), point.getY(), point.getX(), point.getY());
                } else {
                    Envelope env = jtsGeometry.getEnvelopeInternal();
                    rtreeGeometry = Geometries.rectangle(env.getMinX(), env.getMinY(), env.getMaxX(), env.getMaxY());
                }
                //比较点和多边形的mbr与rtree节点的重叠部分，确定分区
                if (rtreeGeometry != null) {
                    Observable<Entry<Integer, Geometry>> results = rtree.search(rtreeGeometry);
                    for (Entry<Integer, Geometry> entry : results.toBlocking().toIterable()) {
                        context.write(new IntWritable(entry.value()), new Text(parts[0] + "," + wkt));  //输出网格编号和原始数据（包括WKT编号）
                    }
                }
            }
        } catch (ParseException e) {
            e.printStackTrace();
        }

        long endTime = System.currentTimeMillis();
        long duration = endTime - startTime;

        System.out.println("Map task duration: " + duration + " ms");

    }
}
