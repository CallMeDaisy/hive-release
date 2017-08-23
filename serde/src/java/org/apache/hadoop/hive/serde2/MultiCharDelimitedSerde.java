
package org.apache.hadoop.hive.serde2;
/**
 * Licensed to the inspur.com
 */

import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.type.HiveChar;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.common.type.HiveVarchar;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.AbstractPrimitiveJavaObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.CharTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.hive.serde2.typeinfo.VarcharTypeInfo;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
/**
 * MultiCharDelimitedSerde: use the parameter of "field_delimited" to split row data to Deserialization data.
 * This class doesn't support data serialization.
 * The initialization fails if the "field_delimited" parameter is not provided.
 */
public class MultiCharDelimitedSerde extends AbstractSerDe {
	public static final Log LOG = LogFactory.getLog(MultiCharDelimitedSerde.class);
	int numColumns; 
	String delimited; 
	StructObjectInspector rowOI;
	List<Object> row; 
	List<String> columnNames; 
	List<TypeInfo> columnTypes;
	long partialMatchedRowsCount = 0L; 
	boolean alreadyLoggedPartialMatch = false; 

	private String getDelimited(Properties tbl) throws SerDeException {
		this.delimited = tbl.getProperty("field_delimited");
		if (Strings.isNullOrEmpty(this.delimited)) {
			throw new SerDeException("This table does not have serde property \"field_delimited\"! You need to specify this parameter.");
		}
		return this.delimited;
	}

	public void initialize(Configuration configuration, Properties tbl) throws SerDeException {
		this.delimited = getDelimited(tbl);
    
		String columnNameProperty = tbl.getProperty("columns");
		String columnTypeProperty = tbl.getProperty("columns.types");
		this.columnNames = Arrays.asList(columnNameProperty.split(","));
		this.columnTypes = TypeInfoUtils.getTypeInfosFromTypeString(columnTypeProperty);
		assert (this.columnNames.size() == this.columnTypes.size());
		this.numColumns = this.columnNames.size();
      
		List columnOIs = new ArrayList(this.columnNames.size());
		for (int c = 0; c < this.numColumns; c++) {
			TypeInfo typeInfo = (TypeInfo) this.columnTypes.get(c);
			if ((typeInfo instanceof PrimitiveTypeInfo)) {
				PrimitiveTypeInfo pti = (PrimitiveTypeInfo) typeInfo;
				AbstractPrimitiveJavaObjectInspector oi = PrimitiveObjectInspectorFactory
						.getPrimitiveJavaObjectInspector(pti);
				columnOIs.add(oi);
			} else {
				throw new SerDeException(getClass().getName() + " doesn't allow column [" + c + "] named "
						+ (String) this.columnNames.get(c) + " with type " + this.columnTypes.get(c));
			}

		}
        //rowOI used to store each column structure
		this.rowOI = ObjectInspectorFactory.getStandardStructObjectInspector(this.columnNames, columnOIs,
				Lists.newArrayList(Splitter.on('\000').split(tbl.getProperty("columns.comments"))));

		this.row = new ArrayList(this.numColumns);
		for (int c = 0; c < this.numColumns; c++)
			this.row.add(null);
	}

	public ObjectInspector getObjectInspector() throws SerDeException {
		return this.rowOI;
	}
    
	public Object deserialize(Writable writable) throws SerDeException {
		
		Text rowText = (Text) writable;
		//The row data is segmented by the provided delimiter
		Iterable iterable = Splitter.on(this.delimited).split(rowText.toString());
		List list = Lists.newArrayList(iterable);

		if (list.size() < this.numColumns) {
			return null;
		}

		for (int c = 0; c < this.numColumns; c++) {
			try {
				String t = (String) list.get(c);
				TypeInfo typeInfo = (TypeInfo) this.columnTypes.get(c);
				PrimitiveTypeInfo pti = (PrimitiveTypeInfo) typeInfo;
				switch (pti.getPrimitiveCategory()) {
				case STRING:
					row.set(c, t);
					break;
				case BYTE:
					Byte b;
					b = Byte.valueOf(t);
					row.set(c, b);
					break;
				case SHORT:
					Short s;
					s = Short.valueOf(t);
					row.set(c, s);
					break;
				case INT:
					Integer i;
					i = Integer.valueOf(t);
					row.set(c, i);
					break;
				case LONG:
					Long l;
					l = Long.valueOf(t);
					row.set(c, l);
					break;
				case FLOAT:
					Float f;
					f = Float.valueOf(t);
					row.set(c, f);
					break;
				case DOUBLE:
					Double d;
					d = Double.valueOf(t);
					row.set(c, d);
					break;
				case BOOLEAN:
					Boolean bool;
					bool = Boolean.valueOf(t);
					row.set(c, bool);
					break;
				case TIMESTAMP:
					Timestamp ts;
					ts = Timestamp.valueOf(t);
					row.set(c, ts);
					break;
				case DATE:
					Date date;
					date = Date.valueOf(t);
					row.set(c, date);
					break;
				case DECIMAL:
					HiveDecimal bd = HiveDecimal.create(t);
					row.set(c, bd);
					break;
				case CHAR:
					HiveChar hc = new HiveChar(t, ((CharTypeInfo) typeInfo).getLength());
					row.set(c, hc);
					break;
				case VARCHAR:
					HiveVarchar hv = new HiveVarchar(t, ((VarcharTypeInfo) typeInfo).getLength());
					row.set(c, hv);
					break;
				default:
					throw new SerDeException("Unsupported type " + typeInfo);
				}
			} catch (RuntimeException e) {
				this.partialMatchedRowsCount += 1L;
				if (!this.alreadyLoggedPartialMatch) {
					LOG.warn("" + this.partialMatchedRowsCount + " partially unmatched rows are found, "
							+ " cannot find group " + c + ": " + rowText);

					this.alreadyLoggedPartialMatch = true;
				}
				this.row.set(c, null);
			}
		}
		return this.row;
	}

	public Class<? extends Writable> getSerializedClass() {
		return Text.class;
	}

	public Writable serialize(Object o, ObjectInspector objectInspector) throws SerDeException {
		//not support serialization
		throw new UnsupportedOperationException("CustomMultiCharactorSerDe doesn't support the serialize() method");
	}

	public SerDeStats getSerDeStats() {
		//not support statistics
		return null;
	}
}
