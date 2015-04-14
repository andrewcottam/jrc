package jrc;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import jrc.geotools.CellAreaUdf;
import jrc.geotools.CellIntersectsUdf;

import org.apache.hadoop.io.BytesWritable;
import org.json.JSONException;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.operation.TransformException;

import com.esri.core.geometry.ogc.OGCGeometry;
import com.esri.hadoop.hive.GeometryUtils;
import com.google.common.base.Charsets;
import com.google.common.io.Resources;
import com.vividsolutions.jts.io.ParseException;

public class CellIntersectsUdfTest {

	public static void main(String... args) throws FactoryException, TransformException {
		CellIntersectsUdf cellIntersectsUdf = new CellIntersectsUdf();
		OGCGeometry ogcGeometry;
		try {
			ogcGeometry = OGCGeometry.fromGeoJson(Resources.toString(Resources.getResource("single_geometry_json.json"), Charsets.US_ASCII));
			ByteBuffer byteBuffer = ogcGeometry.asBinary();
			byte[] byteArray = new byte[byteBuffer.remaining()];
			byteBuffer.get(byteArray, 0, byteArray.length);
			BytesWritable geom = new BytesWritable(byteArray);
			try {
				BytesWritable result = cellIntersectsUdf.evaluate(0.1, 54929, geom);
				System.out.println(GeometryUtils.geometryFromEsriShape(result).asGeoJson());
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		} catch (JSONException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
	}
}
