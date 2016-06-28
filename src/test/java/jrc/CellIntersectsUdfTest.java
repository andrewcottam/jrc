package jrc;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.hadoop.io.BytesWritable;
import org.json.JSONException;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.operation.TransformException;

import com.esri.core.geometry.ogc.OGCGeometry;
import com.google.common.base.Charsets;
import com.google.common.io.Resources;
import com.vividsolutions.jts.io.ParseException;

import jrc.geotools.CellIntersectsUdf;

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
				BytesWritable result = cellIntersectsUdf.evaluate(1, 42287, geom); //was 54929
				byte[] resultByteArray = result.getBytes();
				ByteBuffer bb = ByteBuffer.wrap(resultByteArray);
				OGCGeometry g = OGCGeometry.fromBinary(bb);
				System.out.println(g.asText());
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
