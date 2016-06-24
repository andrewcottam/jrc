package jrc;

import java.io.IOException;
import java.nio.ByteBuffer;

import jrc.esri.CellUdtf;

import com.esri.core.geometry.ogc.OGCGeometry;
import com.esri.hadoop.hive.GeometryUtils;
import com.google.common.base.Charsets;
import com.google.common.io.Resources;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.BytesWritable;
import org.json.JSONException;

public class CellUdtfTest {

    public static void main(String... args) throws HiveException, IOException, JSONException {
    OGCGeometry ogcGeometry =
//      OGCGeometry.fromText("POLYGON ((-179.8 -89.8, -179.2 -89.8, -179.2 -89.2, -179.8 -89.2, -179.8 -89.8))");
//      OGCGeometry.fromText("POLYGON ((0.2 0.2, 0.8 0.2, 0.8 0.8, 0.2 0.8, 0.2 0.2))");
//      OGCGeometry.fromText("POLYGON ((-10 -10, 10 -10, 10 10, -10 10, -10 -10))");
      //OGCGeometry.fromText("POLYGON ((-180 -90, 180 -90, 180 90, -180 90))");
//    OGCGeometry.fromText("POLYGON ((170 0, -170 0, -170 10, 170 10, 170 0))");

    OGCGeometry.fromGeoJson(Resources.toString(Resources.getResource("single_geometry_json.json"), Charsets.US_ASCII)); //enclosed format
//    OGCGeometry.fromGeoJson(Resources.toString(Resources.getResource("common_redpoll.json"), Charsets.US_ASCII));
//    OGCGeometry.fromGeoJson(Resources.toString(Resources.getResource("species17975.json"), Charsets.US_ASCII));
    
//	Path path = new Path("E:/cottaan/My Documents/github repos/jrc/src/test/resources/species17975.json");
//	JobConf conf = new JobConf();
//	FileSplit split = new FileSplit(path, 0, 40, new String[0]);
//	UnenclosedJsonRecordReader reader = new UnenclosedJsonRecordReader(split, conf);
//	System.out.println(reader.toString());

    ByteBuffer byteBuffer = ogcGeometry.asBinary();
    byte[] byteArray = new byte[byteBuffer.remaining()];
    byteBuffer.get(byteArray, 0, byteArray.length);
    BytesWritable writable = new BytesWritable(byteArray);
    CellUdtf udf = new CellUdtf();

    ObjectInspector[] oi = {
      PrimitiveObjectInspectorFactory.javaDoubleObjectInspector,
      PrimitiveObjectInspectorFactory.writableBinaryObjectInspector
    };
    udf.initialize(oi);


    Object[] udfArgs = {
      0.1,
      writable
    };
    //TODO Currently getting nullpointerexceptions in this but it works in a real Hive query!!!
    udf.process(udfArgs);

//    List<Long> evaluate = udf.evaluate(0.01, writable);
  }
}
