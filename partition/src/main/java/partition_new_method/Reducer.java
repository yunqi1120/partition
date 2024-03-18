package partition_new_method;

import com.github.davidmoten.rtree.Entry;
import com.github.davidmoten.rtree.RTree;
import com.github.davidmoten.rtree.geometry.Geometries;
import com.github.davidmoten.rtree.geometry.Geometry;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import rx.Observable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Reducer extends org.apache.hadoop.mapreduce.Reducer<IntWritable, Text, IntWritable, Text> {

    private RTree<Integer, Geometry> rtree;
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

            int id = Integer.parseInt(parts[0]);//将读入的多边形格式转化为需要的格式
            String wkt = parts[1];

            //读入的若为多边形，则计算其mbr，将其添加到rtree中
            if (wkt.startsWith("POLYGON")) {
                Polygon polygon = parsePolygon(wkt);
                Polygon.BoundingBox mbr = polygon._boundingBox;
                rtree = rtree.add(id, Geometries.rectangle(mbr.xMin, mbr.yMin, mbr.xMax, mbr.yMax));
                polygonMap.put(id, polygon);//多边形id映射到多边形本身
            } else if (wkt.startsWith("POINT")) {
                points.add(line);
            }
        }

        //第二次遍历：处理点
        for (String pointLine : points) {
            String[] parts = pointLine.split(",", 2);
            Point point = parsePoint(parts[1]);//将读入的点格式转化为需要的格式

            //在由多边形mbr构造的rtree中检查当前点是否与之有交点
            Observable<Entry<Integer, Geometry>> results = rtree.search(Geometries.point(point.x, point.y))
                    .map(entry -> entry);
            //对于所有相交的对象，进一步检查点与该mbr对应多边形的位置关系
            for (Entry<Integer, Geometry> entry : results.toBlocking().toIterable()) {
                Integer polygonId = entry.value();
                Polygon polygon = polygonMap.get(polygonId);
                if (polygonId != null && polygon.contains(point)) {//自定义函数判断点与多边形对象的包含关系
                    pointCountMap.put(polygonId, pointCountMap.getOrDefault(polygonId, 0) + 1);
                }
            }
        }
        //输出每个多边形和其包含的点的数量
        for (Map.Entry<Integer, Integer> entry : pointCountMap.entrySet()) {
            context.write(key, new Text("多边形ID: " + entry.getKey() + ", 包含点的数量: " + entry.getValue()));
        }

    }

    private Polygon parsePolygon(String wkt) {
        //移除字符串中的"POLYGON(("和"))"
        wkt = wkt.replace("POLYGON((", "").replace("))", "");
        //以逗号为分隔符，得到各个顶点坐标
        String[] pointsStr = wkt.split(",");
        //创建一个多边形的构建器对象，添加顶点
        Polygon.Builder builder = Polygon.Builder();
        for (String ptStr : pointsStr) {
            //以空格为分隔符，得到横纵坐标
            String[] coords = ptStr.split(" ");
            double x = Double.parseDouble(coords[0]);
            double y = Double.parseDouble(coords[1]);
            builder.addVertex(new Point(x, y));
        }
        return builder.close().build();
    }

    private Point parsePoint(String wkt) {
        //移除字符串中的"POINT("和")"
        wkt = wkt.replace("POINT(", "").replace(")", "");
        //以空格为分隔符，得到点的横纵坐标
        String[] coords = wkt.split(" ");
        double x = Double.parseDouble(coords[0]);
        double y = Double.parseDouble(coords[1]);
        return new Point(x, y);
    }

}
