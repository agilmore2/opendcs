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
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

//AW:IMPORTS
// Place an import statements you need here.
//AW:IMPORTS_END

//AW:JAVADOC

/**
 Copy monthly averages of one timeseries to another timeseries.
 Output timeseries determined by parameters

 Specific to r_month data

The following queries were used to develop this algorithm:
Compute monthly averages:
SELECT EXTRACT(MONTH FROM start_date_time) month, AVG(value) avg FROM r_base
WHERE interval = 'month'
AND site_datatype_id = 29571 -- betatakin avg temp
AND loading_application_id NOT IN
    (
    SELECT loading_application_id FROM hdb_loading_application WHERE
    loading_application_name IN('CU_Agg_Disagg','compedit')
    )
GROUP BY EXTRACT(MONTH FROM start_date_time)
ORDER BY month

Dates to be filled:
WITH allDates AS
(
SELECT ADD_MONTHS(DATE '1971-01-01',level - 1) dt
FROM dual
CONNECT BY LEVEL <= 
(SELECT MONTHS_BETWEEN(DATE '2020-12-01',DATE '1971-01-01') + 1 FROM dual)
)
SELECT dt FROM allDates WHERE dt NOT IN
(
SELECT start_date_time FROM r_base
WHERE interval = 'month'
AND site_datatype_id = 29571 -- betatakin avg temp
AND loading_application_id NOT IN
    (
    SELECT loading_application_id FROM hdb_loading_application WHERE
    loading_application_name IN('CU_Agg_Disagg','compedit')
    )
)
ORDER BY dt

 */
//AW:JAVADOC_END
public class CULCopyAverageToTimeseries
    extends decodes.tsdb.algo.AW_AlgorithmBase
{
//AW:INPUTS
    public double input; //AW:TYPECODE=i
    String[] _inputNames = { "input" };
//AW:INPUTS_END

//AW:LOCALVARS
    // Enter any local class variables needed by the algorithm.
    String query;
    Connection conn = null;
    ParmRef ref = null;
    DbKey sdi = null;
    
    PropertySpec[] specs = 
        {
                new PropertySpec("fillStartYr", PropertySpec.INT,
                    "(1971) First year to fill data"),
                new PropertySpec("estimation_process", PropertySpec.STRING,
                        "(CU_Agg_Disagg) Which loading application produces estimates that should be ignored."),

        };
//AW:LOCALVARS_END

//AW:OUTPUTS
    public NamedVariable output = new NamedVariable("output",0);
    String[] _outputNames = { "output" };
//AW:OUTPUTS_END

//AW:PROPERTIES
    public long fillStartYr = 1971;
    public String estimation_process = "CU_Agg_Disagg";
    String[] _propertyNames = { "fillStartYr", "estimation_process" };
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
        // This code will be executed once before each group of time slices.
        // For TimeSlice algorithms this is done once before all slices.
        // For Aggregating algorithms, this is done before each aggregate
        // period.
        ref = getParmRef("input");
        sdi = getSDI("input");
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
        
        TimeZone tz = TimeZone.getTimeZone(aggregateTimeZone);

        conn = tsdb.getConnection();
        String status;
        DataObject dbobj = new DataObject();
        conn = tsdb.getConnection();
        DBAccess db = new DBAccess(conn);
        
        // Query to get monthly averages from source data
        query = "SELECT EXTRACT(MONTH FROM r.start_date_time) month, AVG(r.value) avg FROM r_month r, r_base b "
                + "WHERE b.interval = 'month' "
                + "AND r.site_datatype_id = " + sdi + " "
                + "AND r.site_datatype_id = b.site_datatype_id "
                + "AND r.start_date_time = b.start_date_time "
                + "AND b.loading_application_id NOT IN "
                +    "( "
                +    "SELECT loading_application_id FROM hdb_loading_application WHERE "
                +    "loading_application_name IN('" + estimation_process + "','CU_FillMissing') "
                +    ") "
                + "GROUP BY EXTRACT(MONTH FROM r.start_date_time) "
                + "ORDER BY month ";
        
        status = db.performQuery(query,dbobj);
        debug3(" SQL STRING:" + query + "   DBOBJ: " + dbobj + "STATUS:  " + status);
        
        if (status.startsWith("ERROR") || ((Integer) dbobj.get("rowCount")) != 12)
        {
            // query didn't return 12 averages
            warning(comp.getName()+" Aborted: see following error message");
            warning(status);
            return;
        }
        
        ArrayList<Object> monthlyAvgs = (ArrayList<Object>) dbobj.get("avg");
        
        // Query to find all months to fill
        query = "select date_time dt from table(dates_between(DATE '" + fillStartYr + "-01-01',trunc(sysdate, 'month'), 'month')) "
              + "ORDER BY dt";
        
        status = db.performQuery(query,dbobj);
        debug3(" SQL STRING:" + query + "   DBOBJ: " + dbobj + "STATUS:  " + status);
        
        if (status.startsWith("ERROR") || ((Integer) dbobj.get("rowCount")) == 0)
        {
            warning(comp.getName()+" Aborted: see following error message");
            warning(status);
            return;
        }
        
        // Occasionally a site will have exactly one month to fill, which can't be directly cast to arrayList. Handle separately
        ArrayList<Object> monthsToFill;
        if (((Integer) dbobj.get("rowCount")) == 1)
        {
            monthsToFill = new ArrayList<>(1);
            monthsToFill.add(dbobj.get("dt"));
        } else
        {
            monthsToFill = (ArrayList<Object>) dbobj.get("dt");
        }
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss.S",Locale.ENGLISH);
        formatter.setTimeZone(tz);
        for(Iterator<Object> iter = monthsToFill.iterator(); iter.hasNext();)
        {
            try {
                Date dt = formatter.parse((String) iter.next());
                double avg = Double.parseDouble((String) monthlyAvgs.get(dt.getMonth())); // seems more complicated than it should need to be, but sometimes SQL was returning results in scientific notation
                setOutput(output,avg,dt);
            } catch (ParseException e) {
                // improve error handling
                warning(comp.getName() + "Error setting output");
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
