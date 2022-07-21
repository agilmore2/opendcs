package decodes.hdb.algo;

import decodes.hdb.dbutils.DBAccess;
import decodes.hdb.dbutils.DataObject;
import decodes.hdb.dbutils.RBASEUtils;
import decodes.tsdb.DbCompException;
import decodes.tsdb.ParmRef;
import decodes.tsdb.algo.AWAlgoType;
import decodes.util.DecodesSettings;
import decodes.util.PropertySpec;
import ilex.var.NamedVariable;

import java.sql.Connection;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Pattern;

//AW:IMPORTS
// Place an import statements you need here.
//AW:IMPORTS_END

//AW:JAVADOC

/**
 This algorithm estimates a value that is unavailable from source data by
 averaging the last 5 years of data that is not an estimate
 Only applies to R_MONTH

 ESTIMATE_PROCESS: Algorithms used on the input data that are NOT considered source data
 ROUNDING: determines if rounding to the 7th decimal point is desired, default FALSE

 Algorithm is as follows:
 decide if want to check for update or just have database catch duplicates
 shortcut if input data is from an estimate?
 big query excluding estimated data
 triggered by changing input data
 changes output estimates


 working? SQL query to compute data:
 WITH d AS
 (SELECT extract(year from a.start_date_time) yr, extract(month from a.start_date_time) mon, a.value, avg(a.value) over
 (partition by extract(month from a.start_date_time)
 order by extract(year from a.start_date_time)
 rows between 4 preceding and current row) as aves
 FROM
 r_month A, r_base ab WHERE
 A.site_datatype_id = 29409 AND -- pull from input1
 -- join with r_base for input1 to check loading app
 A.site_datatype_id = ab.site_datatype_id AND
 ab.INTERVAL = 'month' AND
 ab.start_date_time = A.start_date_time AND
 ab.loading_application_id NOT IN
 (SELECT loading_application_id FROM
 hdb_loading_application WHERE loading_application_name IN ('CU_estimation_process')
 )
 ), -- d is query for source data and computes 5 year monthly averages
 mons as (
 select date_time, d.value from table(dates_between('01-jan-1984','01-dec-2021','month')), d
 where
 yr(+)  = extract(year from date_time) and
 mon(+) = extract(month from date_time)
 ) -- mons is the list of all dates and source values where they exist
 SELECT mons.date_time, aves --nvl(mons.value,nvl(aves,null)) -- only return estimated values
 FROM d, mons
 where mons.value is null and
 yr = (select max(yr) from d where mon = extract(month from mons.date_time) and
 yr <= extract(year from mons.date_time)) and
 mon = extract(month from mons.date_time)
 order by mons.date_time;


 */
//AW:JAVADOC_END
public class CULEstimateFromSource
        extends decodes.tsdb.algo.AW_AlgorithmBase
{
    //AW:INPUTS
    public double input;	//AW:TYPECODE=i
    String[] _inputNames = { "input" };
//AW:INPUTS_END

    //AW:LOCALVARS
    // Enter any local class variables needed by the algorithm.
    String alg_ver = "1.0";
    String query;
    int count = 0;
    boolean do_setoutput = true;
    Connection conn = null;

    private static final Pattern loadappPattern = Pattern.compile("^\\w+$"); //only alphanumeric+_ allowed

    PropertySpec[] specs =
            {
                    new PropertySpec("rounding", PropertySpec.BOOLEAN,
                            "(default=false) If true, then rounding is done on the output value."),
                    new PropertySpec("validation_flag", PropertySpec.STRING,
                            "(empty) Always set this validation flag in the output."),
                    new PropertySpec("estimation_process", PropertySpec.STRING,
                            "(CU_estimation_process) Which loading application produces estimates that should be ignored."),
            };



//AW:LOCALVARS_END

    //AW:OUTPUTS
    public NamedVariable output = new NamedVariable("output", 0);
    String[] _outputNames = { "output"};
//AW:OUTPUTS_END

    //AW:PROPERTIES
    public boolean rounding = false;
    public String estimation_process = "CU_estimation_process";
    public String validation_flag = "";
    public String flags;

    String[] _propertyNames = { "estimation_process", "validation_flag", "rounding" };
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

        // protects against SQL injection string shenanigans, avoid Bobby problem?
        if ( !loadappPattern.matcher( estimation_process ).matches()) {
            warning("Loading application name not valid: "+estimation_process);
            return;
        }

        query = null;
        count = 0;
        do_setoutput = true;
        flags = "";
        conn = null;
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
        // Enter code to be executed at each time-slice.
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
        debug1(comp.getAlgorithmName()+"-"+alg_ver+" BEGINNING OF AFTER TIMESLICES: for period: " +
                _aggregatePeriodBegin + " SDI: " + getSDI("input"));
        do_setoutput = true;

        // get the input and output parameters and see if its model data
        ParmRef parmRef = getParmRef("input");
        if (parmRef == null) {
            warning("Unknown variable 'INPUT'");
            return;
        }

        String input_interval = parmRef.compParm.getInterval();
        if (input_interval == null || !input_interval.equals("month"))
            warning("Wrong input interval for " + comp.getAlgorithmName());

        String table_selector = parmRef.compParm.getTableSelector();
        if (table_selector == null || !table_selector.equals("R_"))
            warning("Invalid table selector for algorithm, only R_ supported");

        TimeZone tz = TimeZone.getTimeZone("GMT");
        GregorianCalendar cal = new GregorianCalendar(tz);
        GregorianCalendar cal1 = new GregorianCalendar(); //uses correct timezone from OpenDCS properties
        cal1.setTime(_aggregatePeriodBegin);
        cal.set(cal1.get(Calendar.YEAR),cal1.get(Calendar.MONTH),cal1.get(Calendar.DAY_OF_MONTH),0,0);
        SimpleDateFormat sdf = new SimpleDateFormat("dd-MMM-yyyy");
        sdf.setTimeZone(
                TimeZone.getTimeZone(DecodesSettings.instance().aggregateTimeZone));

        // get the connection  and a few other classes so we can do some sql
        conn = tsdb.getConnection();

        String status;
        DataObject dbobj = new DataObject();
        dbobj.put("ALG_VERSION",alg_ver);
        conn = tsdb.getConnection();
        DBAccess db = new DBAccess(conn);
        RBASEUtils rbu = new RBASEUtils(dbobj,conn);

        String select_clause = " SELECT mons.date_time, aves ";
        if (rounding)
        {
            select_clause = " SELECT mons.date_time, round(aves,7) aves "; // 7 used by other HDB aggregates
        }

        query = " WITH d AS " +
                " (SELECT extract(year from a.start_date_time) yr, extract(month from a.start_date_time) mon, a.value, avg(a.value) over " +
                " (partition by extract(month from a.start_date_time) " +
                " order by extract(year from a.start_date_time) " +
                " rows between 4 preceding and current row) as aves " +
                " FROM " +
                " r_month a, r_base ab WHERE " +
                " a.site_datatype_id = " + getSDI("input") + " AND -- pull from input1 " +
                " -- join with r_base for input1 to check loading app " +
                " a.site_datatype_id = ab.site_datatype_id AND " +
                " ab.INTERVAL = 'month' AND " +
                " ab.start_date_time = A.start_date_time AND " +
                " ab.loading_application_id NOT IN " +
                " (SELECT loading_application_id FROM " +
                " hdb_loading_application WHERE loading_application_name IN ('compedit','" + estimation_process + "') " +
                " ) " +
                " ), -- d is query for source data and computes 5 year monthly averages " +
                " mons as ( " +
                " select date_time, d.value from table(dates_between('" + sdf.format(comp.getValidStart()) + "','" + sdf.format(comp.getValidEnd()) + "','month')), d " +
                " where " +
                " yr(+)  = extract(year from date_time) and " +
                " mon(+) = extract(month from date_time) " +
                " ) -- mons is the list of all dates and source values where they exist" +
                select_clause + // SELECT mons.date_time, aves -- or rounding version
                " FROM d, mons " +
                " where mons.value is null and " +
                " yr = (select max(yr) from d where mon = extract(month from mons.date_time) and " +
                " yr <= extract(year from mons.date_time)) and " +
                " mon = extract(month from mons.date_time) " +
                " order by mons.date_time";


        status = db.performQuery(query,dbobj); // interface has no methods for parameters

        debug3(" SQL STRING:" + query + "   DBOBJ: " + dbobj.toString() + "STATUS:  " + status);
        // now see if the aggregate query worked if not abort!!!

        if (status.startsWith("ERROR"))
        {
            warning(comp.getAlgorithmName()+"-"+alg_ver+" Aborted: see following error message");
            warning(status);
            return;
        }
        // now retrieve records from coeff computation
        ArrayList<Object> dates  = (ArrayList<Object>) dbobj.get("date_time");
        ArrayList<Object> aves = (ArrayList<Object>) dbobj.get("aves");

//              otherwise we have some records so continue...

        // set the output if all is successful and set the flags appropriately
        if (do_setoutput) {
            Iterator<Object> it1 = dates.iterator();
            Iterator<Object> it2 = aves.iterator();
            while (it1.hasNext() && it2.hasNext()) {
                Date mon = new Date( it1.next().toString());
                double ave = Double.parseDouble(it2.next().toString());
                cal.setTime(mon); // Months are 0 indexed in Java dates

                debug3("FLAGS: " + flags);
                if (flags != null)
                    setHdbDerivationFlag(output, flags);
                //
                /* added to allow users to automatically set the Validation column */
                if (validation_flag.length() > 0)
                    setHdbValidationFlag(output, validation_flag.charAt(1));

                info("Setting output for " + debugSdf.format(cal.getTime()));
                setOutput(output, ave, cal.getTime());
            }
        }
        // not handling deletions
        else
        {
        }

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