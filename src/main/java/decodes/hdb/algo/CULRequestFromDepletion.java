package decodes.hdb.algo;

import decodes.hdb.dbutils.DBAccess;
import decodes.hdb.dbutils.DataObject;
import decodes.sql.DbKey;
import decodes.tsdb.DbCompException;
import decodes.tsdb.algo.AWAlgoType;
import decodes.util.PropertySpec;
import ilex.var.NamedVariable;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.regex.Pattern;
//AW:IMPORTS_END

//AW:JAVADOC

/**
Computes Diversion Request from Depletion Request, Supply from Consumptive Use
 Coeffs are stored in the ref_site_coef table
 Input is Consumptive Use
 Output is Supply Request

 select coef.coef_idx, coef.coef from
 ref_site_coef coef, hdb_site_datatype sd,
 hdb_datatype_feature df, hdb_attr_feature af,
 hdb_feature feat, hdb_attr_feature coef_af
 where
 coef.site_id = 2029 and
 coef.attr_id = af.attr_id and
 df.feature_id = af.feature_id and
 df.feature_class_id = (
 select feature_class_id from hdb_feature_class where feature_class_name = 'Consumptive Use Type'
 ) and
 df.datatype_id = sd.datatype_id and
 feat.feature_name = 'percent consumptive use of diversion request' and
 feat.feature_id = coef_af.feature_id and
 coef_af.attr_id = af.attr_id and
 feat.feature_class_id = (
 select feature_class_id from hdb_feature_class where feature_class_name = 'Attribute Group'
 ) and
 sd.site_datatype_id = 35146 ;

 */
//AW:JAVADOC_END
public class CULRequestFromDepletion
    extends decodes.tsdb.algo.AW_AlgorithmBase
{
    private final static String CONUSE = "conuse";
    private final static String REQUEST = "request";
    //AW:INPUTS
    public double conuse;    //AW:TYPECODE=i
    String[] _inputNames = { CONUSE };
//AW:INPUTS_END

//AW:LOCALVARS

    double[] coeffs;
    private static final Pattern stringPattern = Pattern.compile("^[\\w\\h,]*$"); //only alphanumeric+_, and spaces allowed

    PropertySpec[] specs =
        {
            new PropertySpec("basinId", PropertySpec.INT,
                    "(2029) SiteId of the coefficient storage in REF_SITE_COEF, default SiteId of UPPER COLORADO RIVER BASIN" ),
            new PropertySpec("coefficientFeature", PropertySpec.STRING,
                    "(percent consumptive use of diversion request) HDB_FEATURE entry to identify coefficient"),
            new PropertySpec("minValue", PropertySpec.NUMBER,
                        "(0.0) Minimum value for output diversion request")
        };

//AW:LOCALVARS_END

//AW:OUTPUTS
    public NamedVariable request = new NamedVariable(REQUEST, 0);
    String[] _outputNames = { REQUEST };
//AW:OUTPUTS_END

//AW:PROPERTIES
    public long basinId = 2029;
    public String coefficientFeature = "percent consumptive use of diversion request";
    public double minValue = 0D;
    String[] _propertyNames = { "basinId", "coefficientFeature", "minValue"};
//AW:PROPERTIES_END

    // Allow javac to generate a no-args constructor.

    /**
     * Algorithm-specific initialization provided by the subclass.
     */
    protected void initAWAlgorithm( )
        throws DbCompException
    {
//AW:INIT
        _awAlgoType = AWAlgoType.TIME_SLICE;
//AW:INIT_END

//AW:USERINIT
        // Code here will be run once, after the algorithm object is created.
//AW:USERINIT_END
    }
    
    /**
     * This method is called once before iterating all time slices.
     */
    protected void beforeTimeSlices()
        throws DbCompException
    {
//AW:BEFORE_TIMESLICES
        // protects against SQL injection string shenanigans, avoid Bobby problem?
        if ( !stringPattern.matcher( coefficientFeature ).matches()) {
            throw new DbCompException("Coefficient feature name not valid: "+coefficientFeature);
        }

        DbKey conSDI = getSDI(CONUSE);
        coeffs = new double[12];
        String status;
        DataObject dbobj = new DataObject();
        Connection conn = tsdb.getConnection();
        DBAccess db = new DBAccess(conn);

        String query = "select coef.coef_idx idx, coef.coef coef from " +
                "ref_site_coef coef, hdb_site_datatype sd, " +
                "hdb_datatype_feature df, hdb_attr_feature af, " +
                "hdb_feature feat, hdb_attr_feature coef_af " +
                "where " +
                "coef.site_id = " + basinId + " and " +
                "coef.attr_id = af.attr_id and " +
                "df.feature_id = af.feature_id and " +
                "df.feature_class_id = ( " +
                "select feature_class_id from hdb_feature_class where feature_class_name = 'Consumptive Use Type' " +
                ") and " +
                "df.datatype_id = sd.datatype_id and " +
                "feat.feature_name = '" + coefficientFeature + "' and " +
                "feat.feature_class_id = ( " +
                "select feature_class_id from hdb_feature_class where feature_class_name = 'Attribute Group' " +
                ") and " +
                "feat.feature_id = coef_af.feature_id and " +
                "coef_af.attr_id = af.attr_id and " + //two joins from hdb_attr_feature to hdb_attr
                "sd.site_datatype_id = " + conSDI + " " +
                "order by idx";

        status = db.performQuery(query, dbobj);
        if(status.startsWith("ERROR") || Integer.parseInt(dbobj.get("rowCount").toString()) != 12)
        {
            throw new DbCompException(status + " Something wrong with monthly coefficients! query: " + query + " dbobj: " + dbobj);
        }

        ArrayList<Object> monthlyCoefficients = (ArrayList<Object>) dbobj.get("coef");
        Iterator<Object> itMon = monthlyCoefficients.iterator();
        int i = 0;
        while(itMon.hasNext()) {
            coeffs[i++] = Double.parseDouble(itMon.next().toString());
        }
        if (coeffs.length != 12 )
        {
            throw new DbCompException("Failed to parse monthly coefficients into double array");
        }
//AW:BEFORE_TIMESLICES_END
    }

    /**
     * Do the algorithm for a single time slice.
     * AW will fill in user-supplied code here.
     * Base class will set inputs prior to calling this method.
     * User code should call one of the setOutput methods for a time-slice
     * output variable.
     *
     * @throws DbCompException (or subclass thereof) if execution of this
     *        algorithm is to be aborted.
     */
    protected void doAWTimeSlice()
        throws DbCompException
    {
//AW:TIMESLICE
        double out = conuse / (coeffs[_timeSliceBaseTime.getMonth()] / 100D);
        setOutput(request, Math.max(out, minValue));

//AW:TIMESLICE_END
    }

    /**
     * This method is called once after iterating all time slices.
     */
    protected void afterTimeSlices()
        throws DbCompException
    {
//AW:AFTER_TIMESLICES
        // This code will be executed once after each group of time slices.
        // For TimeSlice algorithms this is done once after all slices.
        // For Aggregating algorithms, this is done after each aggregate
        // period.

//AW:AFTER_TIMESLICES_END
    }

    /**
     * Required method returns a list of all input time series names.
     */
    public String[] getInputNames()
    {
        return _inputNames;
    }

    /**
     * Required method returns a list of all output time series names.
     */
    public String[] getOutputNames()
    {
        return _outputNames;
    }

    /**
     * Required method returns a list of properties that have meaning to
     * this algorithm.
     */
    public String[] getPropertyNames()
    {
        return _propertyNames;
    }

    @Override
    protected PropertySpec[] getAlgoPropertySpecs()
    {
        return specs;
    }

}
