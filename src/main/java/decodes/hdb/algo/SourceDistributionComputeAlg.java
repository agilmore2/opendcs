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
 This algorithm computes the weighted monthly coefficients from source data
 for use in disaggregation from annual to monthly values for other years
 Only applies to R_

 ESTIMATE_PROCESS: Algorithms used on the input data that are NOT considered source data
 NO_ROUNDING: determines if rounding to the 5th decimal point is desired, default FALSE

 Algorithm is as follows:
 query average of all input data grouped by month
 store 12 values in r_month table in a specific year, 1985 by convention, set by computation output variables
 decide if want to check for update or just have database catch duplicates

 working SQL query to compute data:
 WITH t AS
 (SELECT vals.VALUE, EXTRACT(MONTH FROM vals.start_date_time) mon, EXTRACT(YEAR FROM vals.start_date_time) yr
 FROM hdb_site_datatype sd, r_month vals, r_base base, hdb_loading_application app
 WHERE
 site_id = 4856 AND datatype_id = 1374 AND
 sd.site_datatype_id = vals.site_datatype_id and
 sd.site_datatype_id = base.site_datatype_id AND
 base.INTERVAL = 'month' AND base.start_date_time = vals.start_date_time AND
 base.loading_application_id = app.loading_application_id and
 app.loading_application_name != 'CU_estimation_process' -- string from property
 ),
 yrs AS (SELECT yr, sum(VALUE) tot, count(VALUE) mons FROM t GROUP BY yr)
 SELECT mon, round(avg(VALUE/tot),4)*100 coef FROM t, yrs
 WHERE t.yr = yrs.yr AND
 yrs.mons = 12 -- comment out if ignore_partials false
 group by mon order by mon;

 querying r_month for actual values to ensure validation has occurred.
 querying r_base to get loading application info
 will use ignore_partials by default

 */
//AW:JAVADOC_END
public class SourceDistributionComputeAlg
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
                    new PropertySpec("ignore_partials", PropertySpec.BOOLEAN,
                            "(default=true) If true, then only compute coefficients for years with all 12 months."),
                    new PropertySpec("rounding", PropertySpec.BOOLEAN,
                            "(default=false) If true, then rounding is done on the output value."),
                    new PropertySpec("validation_flag", PropertySpec.STRING,
                            "(empty) Always set this validation flag in the output."),
                    new PropertySpec("estimation_process", PropertySpec.STRING,
                            "(CU_estimation_process) Which loading application produces estimates that should be ignored."),
                    new PropertySpec("coeff_year", PropertySpec.INT,
                            "(1985) What year to write coefficients into"),
            };



//AW:LOCALVARS_END

    //AW:OUTPUTS
    public NamedVariable output = new NamedVariable("output", 0);
    String[] _outputNames = { "output"};
//AW:OUTPUTS_END

    //AW:PROPERTIES
    public boolean ignore_partials = true;
    public boolean rounding = false;
    public String estimation_process = "CU_estimation_process";
    public String validation_flag = "";
    public Integer coeff_year = 1985;
    public String flags;

    String[] _propertyNames = { "ignore_partials", "estimation_process",
            "validation_flag", "no_rounding", "coeff_year" };
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
//
        // get the input and output parameters and see if its model data
        ParmRef parmRef = getParmRef("input");
        if (parmRef == null) {
            warning("Unknown aggregate control output variable 'INPUT'");
            return;
        }

        String input_interval = parmRef.compParm.getInterval();
        if (input_interval == null || !input_interval.equals("month"))
            warning("Wrong input interval for " + comp.getAlgorithmName());

        String table_selector = parmRef.compParm.getTableSelector();
        if (table_selector == null || !table_selector.equals("R_"))
            warning("Invalid table selector for algorithm, only R_ supported");

        parmRef = getParmRef("output1");
        if (parmRef == null)
            warning("Unknown aggregate control output variable 'OUTPUT'");
//

        TimeZone tz = TimeZone.getTimeZone("GMT");
        GregorianCalendar cal = new GregorianCalendar(tz);
        GregorianCalendar cal1 = new GregorianCalendar();
        cal1.setTime(_aggregatePeriodBegin);
        cal.set(cal1.get(Calendar.YEAR),cal1.get(Calendar.MONTH),cal1.get(Calendar.DAY_OF_MONTH),0,0);


//
        // get the connection  and a few other classes so we can do some sql
        conn = tsdb.getConnection();
        SimpleDateFormat sdf = new SimpleDateFormat("dd-MM-yyyy HH:mm");
        sdf.setTimeZone(
                TimeZone.getTimeZone(
                        DecodesSettings.instance().aggregateTimeZone));
//                        DbCompConfig.instance().getAggregateTimeZone()));
        String status;
        DataObject dbobj = new DataObject();
        dbobj.put("ALG_VERSION",alg_ver);
        conn = tsdb.getConnection();
        DBAccess db = new DBAccess(conn);
        RBASEUtils rbu = new RBASEUtils(dbobj,conn);
//
        String require_12_clause = "";
        if (ignore_partials)
        {
            require_12_clause = " yrs.mons = 12 ";
        }
        String select_clause = " SELECT mon, avg(VALUE/tot) coef FROM t, yrs";
        if (rounding)
        {
            select_clause = " SELECT mon, round(avg(VALUE/tot),7) coef FROM t, yrs"; // 7 used by other HDB aggregates
        }

        query = " WITH t AS" +
                " (SELECT vals.VALUE, EXTRACT(MONTH FROM vals.start_date_time) mon, EXTRACT(YEAR FROM vals.start_date_time) yr" +
                " FROM hdb_site_datatype sd, r_month vals, r_base base, hdb_loading_application app" +
                " WHERE" +
                " sd.site_datatype_id = "+getSDI("input")+" and" +
                " sd.site_datatype_id = vals.site_datatype_id and" +
                " sd.site_datatype_id = base.site_datatype_id AND" +
                " base.INTERVAL = 'month' AND base.start_date_time = vals.start_date_time AND" +
                " base.loading_application_id = app.loading_application_id and" +
                " app.loading_application_name != '"+estimation_process+"' -- string from property, Bobby problem?" +
                " )," +
                " yrs AS (SELECT yr, sum(VALUE) tot, count(VALUE) mons FROM t GROUP BY yr)" +
                select_clause + // SELECT mon, avg(VALUE/tot) coef FROM t, yrs -- or rounding version
                " WHERE t.yr = yrs.yr AND" +
                require_12_clause + // yrs.mons = 12 --adds requirement that only whole years are used to compute coefficients
                " group by mon order by mon;";


        status = db.performQuery(query,dbobj); // interface has no methods for parameters

        debug3(" SQL STRING:" + query + "   DBOBJ: " + dbobj.toString() + "STATUS:  " + status);
        // now see if the aggregate query worked if not abort!!!

        if (status.startsWith("ERROR"))
        {
            warning("DynamicAggregatesAlg-"+alg_ver+" Aborted: see following error message");
            warning(status);
            return;
        }
        // now retrieve records from coeff computation
        ArrayList<Object> mons  = (ArrayList<Object>) dbobj.get("mon");
        ArrayList<Object> coeffs = (ArrayList<Object>) dbobj.get("coef");

//
        //  delete any existing resultant value if not enough values returned
        if (mons.size() != 12)
        {
            debug3(comp.getAlgorithmName() + "-" + alg_ver + " Aborted: not enough month values " + _aggregatePeriodBegin + " SDI: " + getSDI("input"));
            do_setoutput = false;
        }

//              otherwise we have some records so continue...

        // set the output if all is successful and set the flags appropriately
        if (do_setoutput) {
            Iterator<Object> it1 = mons.iterator();
            Iterator<Object> it2 = coeffs.iterator();
            while (it1.hasNext() && it2.hasNext()) {
                int mon = Integer.parseInt(it1.next().toString());
                double coeff = Double.parseDouble(it2.next().toString());
                cal.set(coeff_year, mon, 1, 0, 0);

                debug3("FLAGS: " + flags);
                if (flags != null)
                    setHdbDerivationFlag(output, flags);
                //
                /* added to allow users to automatically set the Validation column */
                if (validation_flag.length() > 0)
                    setHdbValidationFlag(output, validation_flag.charAt(1));

                info("Setting output for " + debugSdf.format(cal.getTime()));
                setOutput(output, coeff, cal.getTime());
            }
        }
        // delete any existing value if this calculation failed
        else
        {
            info("Deleting output for " + debugSdf.format(cal.getTime()));
            for (Object mon: mons)
            {
                cal.set(coeff_year,Integer.parseInt(mon.toString()),1,0,0);
                deleteOutput(output,cal.getTime());
            }
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
