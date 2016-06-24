package jrc.esri;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import com.esri.core.geometry.Envelope;
import com.esri.core.geometry.Geometry;
import com.esri.core.geometry.Operator;
import com.esri.core.geometry.OperatorContains;
import com.esri.core.geometry.OperatorFactoryLocal;
import com.esri.core.geometry.OperatorIntersects;
import com.esri.core.geometry.SpatialReference;
import com.esri.core.geometry.ogc.OGCGeometry;
import jrc.CellCalculator;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BinaryObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.LongWritable;

import static jrc.CellCalculator.MAX_LAT;
import static jrc.CellCalculator.MAX_LON;
import static jrc.CellCalculator.MIN_LAT;
import static jrc.CellCalculator.MIN_LON;

/**
 * Calculates all the cell intersecting with a geometry at a certain cell size.
 * Expects WKB.
 */
public class CellUdtf extends GenericUDTF {

	private static final SpatialReference SPATIAL_REFERENCE = SpatialReference.create(4326);
	private static final Log LOG = LogFactory.getLog(CellUdtf.class);
	private final OperatorIntersects intersectsOperator = (OperatorIntersects) OperatorFactoryLocal.getInstance().getOperator(Operator.Type.Intersects);
	private final OperatorContains containsOperator = (OperatorContains) OperatorFactoryLocal.getInstance().getOperator(Operator.Type.Contains);
	private final Object[] result = new Object[2];
	private final LongWritable cellWritable = new LongWritable();
	private final BooleanWritable fullyCoveredWritable = new BooleanWritable();
	private final CellCalculator<Envelope> cellCalculator = new EsriCellCalculator();
	private DoubleObjectInspector doi;
	private BinaryObjectInspector boi;
	private boolean firstRun = true;

	@Override
	public StructObjectInspector initialize(ObjectInspector[] argOIs) throws UDFArgumentException {
		if (argOIs.length != 2) {
			throw new UDFArgumentLengthException("cell() takes two arguments: cell size and geometry");
		}

		List<String> fieldNames = new ArrayList<String>();
		List<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();

		if (argOIs[0].getCategory() != ObjectInspector.Category.PRIMITIVE || !argOIs[0].getTypeName().equals(serdeConstants.DOUBLE_TYPE_NAME)) {
			throw new UDFArgumentException("cell(): cell_size has to be a double");
		}

		if (argOIs[1].getCategory() != ObjectInspector.Category.PRIMITIVE || !argOIs[1].getTypeName().equals(serdeConstants.BINARY_TYPE_NAME)) {
			throw new UDFArgumentException("cell(): geom has to be binary");
		}

		doi = (DoubleObjectInspector) argOIs[0];
		boi = (BinaryObjectInspector) argOIs[1];

		fieldNames.add("cell");
		fieldNames.add("fully_covered");
		fieldOIs.add(PrimitiveObjectInspectorFactory.writableLongObjectInspector);
		fieldOIs.add(PrimitiveObjectInspectorFactory.writableBooleanObjectInspector);
		result[0] = cellWritable;
		result[1] = fullyCoveredWritable;

		return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
	}

	@Override
	public void process(Object[] args) throws HiveException {
		if (firstRun) {
			cellCalculator.setCellSize(doi.get(args[0]));
			firstRun = false;
		}

		// 1. Create bounding box
		OGCGeometry ogcGeometry = OGCGeometry.fromBinary(ByteBuffer.wrap(boi.getPrimitiveWritableObject(args[1]).getBytes()));
		if (ogcGeometry == null) {
			LOG.warn("Geometry is null");
			return;
		}

		if (ogcGeometry.isEmpty()) {
			LOG.warn("Geometry is empty");
			return;
		}

		if (!"Polygon".equals(ogcGeometry.geometryType()) && !"MultiPolygon".equals(ogcGeometry.geometryType())) {
			LOG.warn("Geometry is not a polygon: " + ogcGeometry.geometryType());
			return;
		}

		Envelope envBound = new Envelope();
		ogcGeometry.getEsriGeometry().queryEnvelope(envBound);
		if (envBound.isEmpty()) {
			LOG.warn("Envelope is empty");
			return;
		}

		getCellsEnclosedBy(envBound.getYMin(), envBound.getYMax(), envBound.getXMin(), envBound.getXMax(), ogcGeometry.getEsriGeometry());
	}

	@Override
	public void close() throws HiveException {

	}

	private void getCellsEnclosedBy(double minLat, double maxLat, double minLon, double maxLon, Geometry ogcGeometry) throws HiveException {

		if (LOG.isDebugEnabled()) {
			LOG.debug("Establishing cells enclosed by (lon/lat), min: " + minLon + "/" + minLat + ", max: " + maxLon + "/" + maxLat);
		}

		// Create a 1 cell buffer around the area in question
		minLat = Math.max(MIN_LAT, minLat - cellCalculator.getCellSize());
		minLon = Math.max(MIN_LON, minLon - cellCalculator.getCellSize());

		maxLat = Math.min(MAX_LAT, maxLat + cellCalculator.getCellSize());
		maxLon = Math.min(MAX_LON, maxLon + cellCalculator.getCellSize());
		System.out.println("longitude from " + minLon + " to " + maxLon);
		System.out.println("latitude from " + minLat + " to " + maxLat);
		long lower = cellCalculator.toCellId(minLat, minLon);
		long upper = cellCalculator.toCellId(maxLat, maxLon);

		// Clip to the cell limit
		lower = Math.max(0, lower);
		upper = Math.min(cellCalculator.getMaxLonCell() * cellCalculator.getMaxLatCell() - 1, upper);

		LOG.info("Checking cells between " + lower + " and " + upper + " this is where it gets stuck!");
		System.out.println("Checking cells between " + lower + " and " + upper + " this is where it gets stuck!");
		long omitLeft = lower % cellCalculator.getMaxLonCell();
		long omitRight = upper % cellCalculator.getMaxLonCell();
		if (omitRight == 0) {
			omitRight = cellCalculator.getMaxLonCell();
		}

		intersectsOperator.accelerateGeometry(ogcGeometry, SPATIAL_REFERENCE, Geometry.GeometryAccelerationDegree.enumHot);

		for (long i = lower; i <= upper; i++) {
			if (i % cellCalculator.getMaxLonCell() >= omitLeft && i % cellCalculator.getMaxLonCell() <= omitRight) {
				Envelope cell = cellCalculator.getCellEnvelope(i);
				if (intersects(cell, ogcGeometry)) {
					System.out.println("Cell: " + String.valueOf(i) + " intersects");
					LOG.info("Cell: " + String.valueOf(i) + " intersects");
					if (contains(cell, ogcGeometry)) {
						cellWritable.set(i);
						fullyCoveredWritable.set(true);
						System.out.println("Cell: " + String.valueOf(i) + " contains");
						LOG.info("Cell: " + String.valueOf(i) + " contains");
						forward(result);
					} else {
						cellWritable.set(i);
						fullyCoveredWritable.set(false);
						System.out.println("Cell: " + String.valueOf(i) + " doesnt contain");
						LOG.info("Cell: " + String.valueOf(i) + " doesnt contain");
						forward(result);
					}
				}
			}
		}
		LOG.info("OK done");
		System.out.println("OK done");
	}

	private boolean intersects(Envelope cell, Geometry ogcGeometry) {
		return intersectsOperator.execute(ogcGeometry, cell, SPATIAL_REFERENCE, null);
	}

	private boolean contains(Envelope cell, Geometry ogcGeometry) {
		return containsOperator.execute(ogcGeometry, cell, SPATIAL_REFERENCE, null);
	}

}
