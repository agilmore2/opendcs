package decodes.hdb.algo;

import decodes.hdb.dbutils.DBAccess;
import decodes.hdb.dbutils.DataObject;
import decodes.sql.DbKey;
import decodes.tsdb.DbCompException;
import decodes.tsdb.ParmRef;
import decodes.tsdb.algo.AWAlgoType;
import decodes.util.PropertySpec;
import ilex.var.NamedVariable;

import java.sql.Connection;
import java.util.*;
import java.util.regex.Pattern;

//AW:IMPORTS
// Place an import statements you need here.
//AW:IMPORTS_END

//AW:JAVADOC

/**
 This algorithm computes the average ratio between two timeseries for use in estimating the
 distribution of the timeseries when only the total is known.
 Only applies to R_YEAR

 ESTIMATE_PROCESS: Algorithms used on the input data that are NOT considered source data
 ROUNDING: determines if rounding to the 7th decimal point is desired, default FALSE

 Algorithm is as follows:
 query average ratio of one timeseries to the sum of the two grouped by month
 store 1 value in r_year table in a specific year, 1985 by convention, set by computation output variables
 decide if want to check for update or just have database catch duplicates

 working? SQL query to compute data:
  WITH t AS
 (SELECT extract(year from a.start_date_time) yr, A.VALUE/(A.VALUE+b.VALUE) ratio
 FROM
 r_year A, r_year b, r_base ab, r_base bb WHERE
 A.site_datatype_id = 25058 AND -- pull from input1 (livestock)
 b.site_datatype_id = 25059 AND -- pull from input2 (stockpond)
 A.start_date_time = b.start_date_time AND
 A.start_date_time BETWEEN TO_DATE('1976-01-01','yyyy-mm-dd') AND TO_DATE('1980-01-01','yyyy-mm-dd') AND -- select the range of source data used to disagg
 -- join with r_base for input1 to check loading app
 A.site_datatype_id = ab.site_datatype_id AND
 ab.INTERVAL = 'year' AND
 ab.start_date_time = A.start_date_time AND
 ab.loading_application_id NOT IN
  (SELECT loading_application_id FROM
  hdb_loading_application WHERE loading_application_name IN ('CU_Agg_Disagg')
  ) AND
 -- same for input2
 b.site_datatype_id = bb.site_datatype_id AND
 bb.INTERVAL = 'year' AND
 bb.start_date_time = b.start_date_time AND
 bb.loading_application_id NOT IN
  (SELECT loading_application_id FROM
  hdb_loading_application WHERE loading_application_name IN ('CU_Agg_Disagg')
  )
 )
 SELECT  NVL(avg(ratio),0) FROM t -- calculate a ratio of 0 if one of the series doesn't exist
 ;

 querying r_year for actual values to ensure validation has occurred.
 querying r_base to get loading application info

 */
//AW:JAVADOC_END
public class CULRatioComputeAlg
        extends decodes.tsdb.algo.AW_AlgorithmBase
{
    //AW:INPUTS
    public double sector1;    //AW:TYPECODE=i
    public double sector2;    //AW:TYPECODE=i
    String[] _inputNames = { "sector1", "sector2" };
//AW:INPUTS_END

    //AW:LOCALVARS
    // Enter any local class variables needed by the algorithm.
    String alg_ver = "1.0";
    String query;
    boolean do_setoutput = true;
    Connection conn = null;

    private static final Pattern loadappPattern = Pattern.compile("^\\w*$"); //only alphanumeric+_ allowed

    PropertySpec[] specs =
            {
                    new PropertySpec("rounding", PropertySpec.BOOLEAN,
                            "(default=false) If true, then rounding is done on the output value."),
                    new PropertySpec("validation_flag", PropertySpec.STRING,
                            "(empty) Always set this validation flag in the output."),
                    new PropertySpec("flags", PropertySpec.STRING,
                            "(empty) Always set these dataflags in the output."),
                    new PropertySpec("estimation_process", PropertySpec.STRING,
                            "(CU_Agg_Disagg) Which loading application produces estimates that should be ignored along with CU_FillMissing."),
                    new PropertySpec("src_startyr", PropertySpec.INT,
                            "(1976) Start year of source data used to calculate disagg coefficients"),
                    new PropertySpec("src_endyr", PropertySpec.INT,
                            "(1985) End year of source data used to calculate disagg coefficients"),                    
                    new PropertySpec("coeff_year", PropertySpec.INT,
                            "(1985) What year to write coefficients into"),
            };



//AW:LOCALVARS_END

    //AW:OUTPUTS
    public NamedVariable ratio = new NamedVariable("ratio", 0);
    String[] _outputNames = { "ratio"};
//AW:OUTPUTS_END

    //AW:PROPERTIES
    public boolean rounding = false;
    public String estimation_process = "CU_Agg_Disagg";
    public String validation_flag = "";
    public long coeff_year = 1985;
    public long src_startyr = 1976;
    public long src_endyr = 1985;
    public String flags = "";

    String[] _propertyNames = { "estimation_process", "validation_flag", "rounding", "coeff_year", "flags","src_startyr","src_endyr" };
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
            throw new DbCompException("Loading application name not valid: "+estimation_process);
        }

        query = null;
        do_setoutput = true;
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
                _aggregatePeriodBegin + " SDI: " + getSDI("sector1"));
        do_setoutput = true;
        ParmRef sector1Ref = getParmRef("sector1");
        ParmRef sector2Ref = getParmRef("sector2");

        // get the input and output parameters and see if its model data
        if (sector1Ref == null) {
            warning("Unknown variable 'INPUT1'");
            return;
        }
        if (sector2Ref == null) {
            warning("Unknown variable 'INPUT2'");
            return;
        }

        String input_interval1 = sector1Ref.compParm.getInterval();
        if (input_interval1 == null || !input_interval1.equals("year"))
            warning("Wrong input1 interval for " + comp.getAlgorithmName());
        String table_selector1 = sector1Ref.compParm.getTableSelector();
        if (table_selector1 == null || !table_selector1.equals("R_"))
            warning("Invalid table selector for algorithm, only R_ supported");

        String input_interval2 = sector2Ref.compParm.getInterval();
        if (input_interval2 == null || !input_interval2.equals("year"))
            warning("Wrong input2 interval for " + comp.getAlgorithmName());
        String table_selector2 = sector2Ref.compParm.getTableSelector();
        if (table_selector2 == null || !table_selector2.equals("R_"))
            warning("Invalid table selector for algorithm, only R_ supported");

        ParmRef ratioRef = getParmRef("ratio");
        if (ratioRef == null)
            warning("Unknown output variable 'ratio'");

        TimeZone tz = TimeZone.getTimeZone("MST");
        GregorianCalendar cal = new GregorianCalendar(tz);
        GregorianCalendar cal1 = new GregorianCalendar(); //uses correct timezone from OpenDCS properties
        cal1.setTime(_timeSliceBaseTime);
        cal.set(cal1.get(Calendar.YEAR),cal1.get(Calendar.MONTH),cal1.get(Calendar.DAY_OF_MONTH),0,0);

        // get the connection  and a few other classes so we can do some sql
        conn = tsdb.getConnection();

        String status;
        DataObject dbobj = new DataObject();
        dbobj.put("ALG_VERSION",alg_ver);
        conn = tsdb.getConnection();
        DBAccess db = new DBAccess(conn);

        DbKey sector1SDI = sector1Ref.timeSeries.getSDI();
        DbKey sector2SDI = sector2Ref.timeSeries.getSDI();

        String select_clause = " SELECT NVL(avg(ratio),0) ratio FROM t";
        if (rounding)
        {
            select_clause = " SELECT NVL(round(avg(ratio),7),0) ratio FROM t"; // 7 used by other HDB aggregates
        }

        query = " WITH t AS\n" +
                " (SELECT extract(year from a.start_date_time) yr, A.VALUE/(A.VALUE+b.VALUE) ratio\n" +
                " FROM\n" +
                " r_year A, r_year b, r_base ab, r_base bb WHERE\n" +
                " A.site_datatype_id = " + sector1SDI + " AND -- pull from input1\n" +
                " b.site_datatype_id = " + sector2SDI + " AND -- pull from input2\n" +
                " A.start_date_time = b.start_date_time AND\n" +
                " A.start_date_time BETWEEN TO_DATE('" + src_startyr + "-01-01','yyyy-mm-dd') AND TO_DATE('" + src_endyr + "-01-01','yyyy-mm-dd') AND\n" +
                " -- join with r_base for input1 to check loading app\n" +
                " A.site_datatype_id = ab.site_datatype_id AND\n" +
                " ab.INTERVAL = 'year' AND\n" +
                " ab.start_date_time = A.start_date_time AND\n" +
                " ab.loading_application_id NOT IN\n" +
                "  (SELECT loading_application_id FROM\n" +
                "  hdb_loading_application WHERE loading_application_name IN ('" + estimation_process + "','CU_FillMissing')\n" +
                "  ) AND\n" +
                " -- same for input2\n" +
                " b.site_datatype_id = bb.site_datatype_id AND\n" +
                " bb.INTERVAL = 'year' AND\n" +
                " bb.start_date_time = b.start_date_time AND\n" +
                " bb.loading_application_id NOT IN\n" +
                "  (SELECT loading_application_id FROM\n" +
                "  hdb_loading_application WHERE loading_application_name IN ('" + estimation_process + "','CU_FillMissing')\n" +
                "  )\n" +
                " )\n" +
                select_clause; // SELECT avg(ratio) ratio FROM t -- or rounding version


        status = db.performQuery(query,dbobj); // interface has no methods for parameters

        debug3(" SQL STRING:" + query + "   DBOBJ: " + dbobj + "STATUS:  " + status);
        // now see if the aggregate query worked if not abort!!!

        if (status.startsWith("ERROR") || ((Integer) dbobj.get("rowCount")) != 1)
        {
            warning(comp.getName()+"-"+alg_ver+" Aborted: see following error message");
            warning(status);
            return;
        }
        // now retrieve ratio
        double rat = Double.parseDouble((String) dbobj.get("ratio"));

        // set the output if all is successful and set the flags appropriately
        cal.set((int)coeff_year, 0, 1, 0, 0); // Months are 0 indexed in Java dates
        if (do_setoutput) {
            debug3("FLAGS: " + flags);
            if (!flags.isEmpty())
                setHdbDerivationFlag(ratio, flags);
            //
            /* added to allow users to automatically set the Validation column */
            if (!validation_flag.isEmpty())
                setHdbValidationFlag(ratio, validation_flag.charAt(1));

            debug3("Setting output for " + debugSdf.format(cal.getTime()));
            setOutput(ratio, rat, cal.getTime());
        }
        // delete any existing value if this calculation failed
        else
        {
            info("Deleting output for " + debugSdf.format(cal.getTime()));
            cal.set((int)coeff_year,0,1,0,0);
            deleteOutput(ratio,cal.getTime());
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