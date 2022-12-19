package decodes.hdb.algo;

import decodes.hdb.dbutils.DBAccess;
import decodes.hdb.dbutils.DataObject;
import decodes.tsdb.DbCompException;
import decodes.tsdb.IntervalCodes;
import decodes.tsdb.IntervalIncrement;
import decodes.tsdb.ParmRef;
import decodes.tsdb.algo.AWAlgoType;
import decodes.util.PropertySpec;
import ilex.var.NamedVariable;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.regex.Pattern;
//AW:IMPORTS_END

//AW:JAVADOC

/**
Disaggregates input to output using static coeffs
 Coeffs are stored in the ref_site_coef table
 Input is longer interval
 Output is determined by output interval

 select coef.coef_idx idx, coef.coef coef from
 ref_site_coef coef, hdb_attr af
 where
 coef.site_id = 2029 and
 coef.attr_id = af.attr_id and
 af.attr_name = 'temporal disaggregation, annual to monthly, M and I (urban)'
 order by idx;

 */
//AW:JAVADOC_END
public class CULStaticCoeffTemporalDisagg
    extends decodes.tsdb.algo.AW_AlgorithmBase
{
    private final static String INPUT = "input";
    private final static String OUTPUT = "output";
    //AW:INPUTS
    public double input;    //AW:TYPECODE=i
    String[] _inputNames = {INPUT};
//AW:INPUTS_END

//AW:LOCALVARS

    double[] coeffs;
    private static final Pattern stringPattern = Pattern.compile("^[\\w\\h,()]*$"); //only alphanumeric+_, () and spaces allowed

    PropertySpec[] specs =
        {
            new PropertySpec("basinId", PropertySpec.INT,
                    "(2029) SiteId of the coefficient storage in REF_SITE_COEF, default SiteId of UPPER COLORADO RIVER BASIN" ),
            new PropertySpec("coefficientAttribute", PropertySpec.STRING,
                    "(temporal disaggregation, annual to monthly, M and I (urban)) HDB_ATTR entry to identify coefficient"),
            new PropertySpec("minValue", PropertySpec.NUMBER,
                        "(0.0) Minimum value for output")
        };

    private IntervalIncrement inIntvI;
    private IntervalIncrement outIntvI;
//AW:LOCALVARS_END

//AW:OUTPUTS
    public NamedVariable output = new NamedVariable(OUTPUT, 0);
    String[] _outputNames = {OUTPUT};
//AW:OUTPUTS_END

//AW:PROPERTIES
    public long basinId = 2029;
    public String coefficientAttribute = "temporal disaggregation, annual to monthly, M and I (urban)";
    public double minValue = 0D;
    String[] _propertyNames = { "basinId", "coefficientAttribute", "minValue" };
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
        ParmRef inRef = getParmRef(INPUT);
        if (inRef == null)
            throw new DbCompException("Cannot determine period of "+ INPUT);

        inIntvI = IntervalCodes.getIntervalCalIncr(inRef.compParm.getInterval());
        if (inIntvI == null || inIntvI.getCount() == 0)
            throw new DbCompException(
                    "Cannot DisAggregate from " + INPUT + " with instantaneous interval.");

        // protects against SQL injection string shenanigans, avoid Bobby problem?
        if ( !stringPattern.matcher(coefficientAttribute).matches()) {
            throw new DbCompException("Coefficient feature name not valid: "+ coefficientAttribute);
        }

        ParmRef outRef = getParmRef(OUTPUT);
        if (outRef == null)
            throw new DbCompException("Cannot determine period of " + OUTPUT);
        outIntvI = IntervalCodes.getIntervalCalIncr(outRef.compParm.getInterval());
        if (outIntvI == null || outIntvI.getCount() == 0)
            throw new DbCompException(
                    "Cannot Disaggregate " + OUTPUT + " with instantaneous interval.");

        String status;
        DataObject dbobj = new DataObject();
        Connection conn = tsdb.getConnection();
        DBAccess db = new DBAccess(conn);

        String query = "select coef.coef_idx idx, coef.coef coef from " +
                "ref_site_coef coef, hdb_attr af " +
                "where " +
                "coef.site_id = " + basinId + " and " +
                "coef.attr_id = af.attr_id and " +
                "af.attr_name = '" + coefficientAttribute + "' " +
                "order by idx";

        status = db.performQuery(query, dbobj);
        int count;
        if(status.startsWith("ERROR") || (count = Integer.parseInt(dbobj.get("rowCount").toString())) == 0) // avoid reading dbobj before error checking
        {
            throw new DbCompException(status + " Something wrong with coefficients! query: " + query + " dbobj: " + dbobj);
        }
        coeffs = new double[count];

        ArrayList<Object> coeffList = (ArrayList<Object>) dbobj.get("coef");
        Iterator<Object> itMon = coeffList.iterator();
        int i = 0;
        while(itMon.hasNext()) {
            coeffs[i++] = Double.parseDouble(itMon.next().toString());
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
        Date startT = new Date(_timeSliceBaseTime.getTime());
        aggCal.setTime(startT);
        aggCal.add(inIntvI.getCalConstant(), inIntvI.getCount());
        Date endT = aggCal.getTime();
        aggCal.setTime(startT);

        try {
            int i = 0;
            for(Date t = startT; t.before(endT);
                aggCal.add(outIntvI.getCalConstant(), outIntvI.getCount()),
                        t = aggCal.getTime())
            {
                double out = Math.max(input * coeffs[i++], minValue);
                debug3(comp.getAlgorithmName()+"-"+comp.getName()+" setting output: " +
                        t + " SDI: " + getSDI(OUTPUT) + " value: " + out);
                setOutput(output, out, t);
            }
        }
        catch (Exception e)
        {
            throw new DbCompException("Setting output failed, coeff length: " + coeffs.length + " Exception: " + e);
        }
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
