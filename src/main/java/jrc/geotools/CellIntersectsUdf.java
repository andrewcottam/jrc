package jrc.geotools;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.Polygon;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKBReader;
import com.vividsolutions.jts.io.WKBWriter;
import jrc.CellCalculator;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.BytesWritable;

/**
 * Returns the intersection geometry between a cell and a given geometry.
 * Expects WKB, returns WKB.
 */
public class CellIntersectsUdf extends UDF {

  private static final Log LOG = LogFactory.getLog(CellIntersectsUdf.class);

  private final WKBReader reader = new WKBReader();
  private final WKBWriter writer = new WKBWriter();
  private final BytesWritable result = new BytesWritable();
  private final CellCalculator<Polygon> cellCalculator = new GeotoolsCellCalculator();
  private boolean firstRun = true;

  public BytesWritable evaluate(double cellSize, long cell, BytesWritable b) throws ParseException {
    if (b == null || b.getLength() == 0) {
      LOG.warn("Argument is null or empty");
      return null;
    }

    if (firstRun) {
      cellCalculator.setCellSize(cellSize);
      firstRun = false;
    }

    Geometry geom = reader.read(b.getBytes());
//    System.out.println("CellIntersectsUdf: " + String.valueOf(cell));
//    LOG.info("CellIntersectsUdf: " + String.valueOf(cell));
    Polygon envelope = cellCalculator.getCellEnvelope(cell);
    Geometry intersection = geom.intersection(envelope); //returns a jts geometry
    byte[] resultBytes = writer.write(intersection);
    result.set(resultBytes, 0, resultBytes.length);
    return result;
  }

}
