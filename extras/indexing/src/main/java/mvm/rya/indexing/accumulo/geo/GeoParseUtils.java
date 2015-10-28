package mvm.rya.indexing.accumulo.geo;

import org.apache.log4j.Logger;
import org.openrdf.model.Literal;
import org.openrdf.model.Statement;

import com.vividsolutions.jts.io.ParseException;

public class GeoParseUtils {
    static final Logger logger = Logger.getLogger(GeoParseUtils.class);

	public static String getWellKnownText(Statement statement) throws ParseException {
	    org.openrdf.model.Value v = statement.getObject();
	    if (!(v instanceof Literal)) {
	        throw new ParseException("Statement does not contain Literal: " + statement.toString());
	    }
	
	    Literal lit = (Literal) v;
	    if (!GeoConstants.XMLSCHEMA_OGC_WKT.equals(lit.getDatatype())) {
	        logger.warn("Literal is not of type " + GeoConstants.XMLSCHEMA_OGC_WKT + ": " + statement.toString());
	    }
	
	    return lit.getLabel().toString();
	}

}
