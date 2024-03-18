package partition;

import com.github.davidmoten.rtree.Entry;
import com.github.davidmoten.rtree.RTree;
import com.github.davidmoten.rtree.geometry.Geometries;
import com.github.davidmoten.rtree.geometry.Geometry;
import com.github.davidmoten.rtree.geometry.Rectangle;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;
import rx.Observable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Reducer extends org.apache.hadoop.mapreduce.Reducer<IntWritable, Text, IntWritable, Text> {

    private RTree<Integer, Geometry> rtree;
    private WKTReader wktReader = new WKTReader();
    //多边形ID映射到对应的多边形
    private Map<Integer, Polygon> polygonMap = new HashMap<>();
    //每个多边形包含的点的数量
    private Map<Integer, Integer> pointCountMap = new HashMap<>();
    //value只能遍历一次，用于将多边形存入rtree中，此处points用于存储点的坐标
    private List<String> points = new ArrayList<>();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        rtree = RTree.create();
    }

    @Override
    protected void reduce(IntWritable key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        rtree = RTree.create();
        polygonMap.clear();
        pointCountMap.clear();
        points.clear();

        //第一次遍历：处理多边形
        for (Text value : values) {
            String line = value.toString();
            String[] parts = line.split(",", 2);
            if (parts.length != 2) {
                continue;
            }

            int id = Integer.parseInt(parts[0]);
            String wkt = parts[1];

            //读入的若为多边形，则计算其mbr，将其添加到rtree中
            if (wkt.startsWith("POLYGON")) {
                try {
                    Polygon polygon = (Polygon) wktReader.read(wkt);
                    /*
                    if (!polygon.isValid()) {//isvalid判断有无自交情况
                        context.write(new IntWritable(id), new Text("无效多边形，已跳过"));
                        continue; //跳过无效多边形
                    }
                     */
                    Envelope env = polygon.getEnvelopeInternal();
                    Rectangle mbr = Geometries.rectangle(env.getMinX(), env.getMinY(), env.getMaxX(), env.getMaxY());
                    rtree = rtree.add(id, mbr); //使用多边形id作为rtree的值
                    polygonMap.put(id, polygon);//多边形id映射到多边形本身

                } catch (ParseException e) {
                    e.printStackTrace();
                }
            } else if (wkt.startsWith("POINT")) {
                points.add(line);
            }
        }

        //第二次遍历：处理点
        for (String pointLine : points) {
            String[] parts = pointLine.split(",", 2);
            if (parts.length != 2) {
                continue;
            }

            String pointWkt = parts[1];
            try {
                Point point = (Point) wktReader.read(pointWkt);
                //在由多边形mbr构造的rtree中检查当前点是否与之有交点
                Observable<Entry<Integer, Geometry>> results = rtree.search(Geometries.point(point.getX(), point.getY()))
                        .map(entry -> entry);
                //对于所有相交的对象，进一步检查点与该mbr对应多边形的位置关系
                for (Entry<Integer, Geometry> entry : results.toBlocking().toIterable()) {
                    Integer polygonId = entry.value(); //从rtree获得多边形ID
                    Polygon polygon = polygonMap.get(polygonId);
                    if (polygonId != null && polygon.contains(point)) {//jts判断点与多边形对象的包含关系
                    //if (polygonId != null ) {
                        pointCountMap.put(polygonId, pointCountMap.getOrDefault(polygonId, 0) + 1);
                    }
                }
            } catch (ParseException e) {
                e.printStackTrace();
            }
        }
        //输出每个多边形和其包含的点的数量
        for (Map.Entry<Integer, Integer> entry : pointCountMap.entrySet()) {
            context.write(key, new Text("多边形ID: " + entry.getKey() + ", 包含点的数量: " + entry.getValue()));
        }
    }
}
