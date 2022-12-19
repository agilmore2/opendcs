package decodes.hdb.algo;

import decodes.hdb.HdbFlags;
import decodes.hdb.dbutils.DBAccess;
import decodes.hdb.dbutils.DataObject;
import decodes.hdb.dbutils.HDBAlgoTSUtils;
import decodes.sql.DbKey;
import decodes.tsdb.CTimeSeries;
import decodes.tsdb.DbCompException;
import decodes.tsdb.ParmRef;
import decodes.tsdb.TimeSeriesIdentifier;
import decodes.tsdb.algo.AWAlgoType;
import decodes.util.PropertySpec;
import ilex.var.NamedVariable;
import ilex.var.TimedVariable;
import opendcs.dai.TimeSeriesDAI;

import java.sql.Connection;
import java.util.*;

import static decodes.tsdb.VarFlags.TO_WRITE;

//AW:IMPORTS
// Place an import statements you need here.
//AW:IMPORTS_END

//AW:JAVADOC

/**
 This algorithm copies data to a child site with an optional multiplier

 input: value at parent site, multiplied by optional scaler to produce output value
 datatype: SDI at parent site determines datatype used to find output TS at child site with same datatype

 ROUNDING: determines if rounding to the 7th decimal point is desired, default FALSE

 Algorithm is as follows:
 query output timeseries determined by their parent site matching input site and specified output datatype

 working SQL query to find output timeseries:
 select outts.ts_id ts, r.value*multiplier output from
 hdb_site_datatype insd, hdb_site out,
 hdb_site_datatype outsd, cp_ts_id outts,
 hdb_site_datatype datasd,
 r_month r
 where
 insd.site_datatype_id = 25058 and
 insd.site_id = out.parent_site_id and
 outsd.site_id = out.site_id and
 datasd.site_datatype_id = 34954 AND
 datasd.datatype_id = outsd.datatype_id and
 outsd.site_datatype_id = outts.site_datatype_id and
 outts.interval = 'month' AND
 outts.table_selector = 'R_' AND
 outsd.site_datatype_id = r.site_datatype_id

 input value will be produced by CP and multiplied in doAWTimeSlice and stored in outputTimeseries

 afterTimeslice will write data to database

 */
//AW:JAVADOC_END
public class CULChildCopyMultiplier
        extends decodes.tsdb.algo.AW_AlgorithmBase
{
    public static final String INPUT = "input";
    public static final String DATATYPE = "datatype";
    //AW:INPUTS
    public double input;    //AW:TYPECODE=i
    public double datatype; //AW:TYPECODE=i
    String[] _inputNames = { INPUT, DATATYPE };
//AW:INPUTS_END

    //AW:LOCALVARS
    // Enter any local class variables needed by the algorithm.
    String alg_ver = "1.0";
    private int flag = TO_WRITE;
    String query;
    boolean do_setoutput = true;
    Connection conn = null;
    TimeSeriesDAI dao;
    HashMap<String, CTimeSeries> outputSeries = new HashMap<>();

    PropertySpec[] specs =
            {
                    new PropertySpec("multiplier", PropertySpec.NUMBER,
                            "(default=-1.0) Input is multiplied by this value."),
                    new PropertySpec("rounding", PropertySpec.BOOLEAN,
                            "(default=false) If true, then rounding is done on the output value."),
                    new PropertySpec("validation_flag", PropertySpec.STRING,
                            "(empty) Always set this validation flag in the output."),
                    new PropertySpec("flags", PropertySpec.STRING,
                            "(empty) Always set these dataflags in the output.")
            };



//AW:LOCALVARS_END

    //AW:OUTPUTS
    String[] _outputNames = {};

//AW:OUTPUTS_END

    //AW:PROPERTIES
    public double multiplier = -1D;
    public boolean rounding = false;
    public String validation_flag = "";
    public String flags = "";

    String[] _propertyNames = { "multiplier", "rounding", "validation_flag", "flags" };

    //AW:PROPERTIES_END

    // Allow javac to generate a no-args constructor.

    /**
     * Algorithm-specific initialization provided by the subclass.
     */
    protected void initAWAlgorithm( )
            throws DbCompException
    {
//AW:INIT
        _awAlgoType = AWAlgoType.TIME_SLICE; // doesn't matter and don't want automatic deletes

//AW:INIT_END

//AW:USERINIT
//AW:USERINIT_END
    }

    /**
     * This method is called once before iterating all time slices.
     */
    protected void beforeTimeSlices()
            throws DbCompException
    {
//AW:BEFORE_TIMESLICES
        // This code will be executed once before each group of time slices.
        // For TimeSlice algorithms this is done once before all slices.
        // For Aggregating algorithms, this is done before each aggregate
        // period.

        query = null;
        do_setoutput = true;
        conn = null;
        dao = tsdb.makeTimeSeriesDAO();
        do_setoutput = true;

        // handle flags and output series here because they're dynamic
        if (!flags.isEmpty())
            flag |= HdbFlags.hdbDerivation2flag(flags);
        if (!validation_flag.isEmpty())
            flag |= HdbFlags.hdbValidation2flag(validation_flag.charAt(1));

        // get the input and output parameters and see if its model data
        ParmRef inRef = getParmRef(INPUT);
        if (inRef == null) {
            warning("Unknown variable " + INPUT);
            return;
        }
        DbKey inSDI = inRef.timeSeries.getSDI();

        // get the connection and a few other classes so we can do some sql
        conn = tsdb.getConnection();

        String status;
        DataObject dbobj = new DataObject();
        dbobj.put("ALG_VERSION",alg_ver);
        DBAccess db = new DBAccess(conn);

        query = " select outts.ts_id ts from " +
                " hdb_site_datatype insd, hdb_site out, " +
                " hdb_site_datatype outsd, cp_ts_id outts, " +
                " hdb_site_datatype datasd " +
                " where " +
                " insd.site_datatype_id = " + inSDI + " and " +
                " insd.site_id = out.parent_site_id and " +
                " outsd.site_id = out.site_id and " +
                " datasd.site_datatype_id = " + getSDI(DATATYPE) + " AND " +
                " datasd.datatype_id = outsd.datatype_id and " +
                " outsd.site_datatype_id = outts.site_datatype_id and " +
                " outts.interval = '" + getInterval(DATATYPE) + "' AND " +
                " outts.table_selector = '" + getTableSelector(DATATYPE) + "'";

        status = db.performQuery(query,dbobj); // interface has no methods for parameters

        debug3(" SQL STRING:" + query + "   DBOBJ: " + dbobj + "STATUS:  " + status);
        // now see if the aggregate query worked if not abort!!!

        int count = Integer.parseInt(dbobj.get("rowCount").toString());
        if (status.startsWith("ERROR") || count != 1 || !HDBAlgoTSUtils.findOutputSeries(dbobj, dao, outputSeries))
        {
            warning(comp.getName()+"-"+alg_ver+" Aborted or incorrect output timeseries for site " + getSiteName(DATATYPE) + ": see following error message");
            warning(status);
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
        TimeZone tz = TimeZone.getTimeZone("MST");
        GregorianCalendar cal = new GregorianCalendar(tz);
        GregorianCalendar cal1 = new GregorianCalendar(); //uses correct timezone from OpenDCS properties
        cal1.setTime(_timeSliceBaseTime);
        cal.set(cal1.get(Calendar.YEAR),cal1.get(Calendar.MONTH),cal1.get(Calendar.DAY_OF_MONTH),0,0);

        // output is value supplied by CP times multiplier, there is only one timeseries
        if (outputSeries.size() == 1)
        {
            outputSeries.entrySet().stream().findFirst().get().getValue().addSample(new TimedVariable(cal.getTime(), input * multiplier, flag));
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
        // calculate number of days in the month in case the numbers are for month derivations
        debug1(comp.getAlgorithmName()+"-"+alg_ver+" BEGINNING OF AFTER TIMESLICES, SDI: " + getSDI(INPUT));

        for (CTimeSeries v : outputSeries.values()) {
            TimeSeriesIdentifier id = v.getTimeSeriesIdentifier();
            try {
                debug1(comp.getName() + "-" + alg_ver + "saving site: " + id.getSiteName() + " timeseries: " + id + " with size: " + v.size());
                v.setComputationId(comp.getId());
                dao.saveTimeSeries(v);
            } catch (Exception e) {
                warning("Exception during saving output to database:" + e);
            }
        }
        outputSeries.clear();
        dao.close();
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