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
import ilex.var.TimedVariable;
import opendcs.dai.TimeSeriesDAI;

import java.sql.Connection;
import java.util.*;

import static decodes.tsdb.VarFlags.TO_WRITE;
import static java.lang.Math.abs;

//AW:IMPORTS
// Place an import statements you need here.
//AW:IMPORTS_END

//AW:JAVADOC

/**
 This algorithm computes County HUC CU from the basin total multipled by a computed ratio
 Only applies to R_YEAR

 total: input value at state/trib level
 ratio: reference datatype used to find ratios timeseries stored on basin constituent level

 ROUNDING: determines if rounding to the 7th decimal point is desired, default FALSE
 ObjectType: detemines objecttype of output timeseries

 Algorithm is as follows:
 query output timeseries determined by basin_id and objecttype
 query ratios for every output timeseries from 1985 by convention, datatype determined by input ratio parmref

 working SQL query to compute data:
 select outts.ts_id ts, r.value ratio, out.site_id site from 
 hdb_site_datatype insd, hdb_site out,
 hdb_site_datatype outsd, cp_ts_id outts,
 hdb_site_datatype inratsd, hdb_site_datatype ratiosd,
 r_year r
 where
 insd.site_datatype_id = 25058 and
 insd.site_id = out.basin_id and  
 insd.datatype_id = outsd.datatype_id and  
 outsd.site_id = out.site_id and
 outsd.site_datatype_id = outts.site_datatype_id and  
 outts.interval = 'year' AND  
 outts.table_selector = 'R_' AND
 inratsd.site_datatype_id = 34954 AND
 inratsd.datatype_id = ratiosd.datatype_id and
 ratiosd.site_id = out.site_id and
 ratiosd.site_datatype_id = r.site_datatype_id AND
 extract(year from r.start_date_time) = 1985

 querying r_year for actual values to ensure validation has occurred.


 */
//AW:JAVADOC_END
public class CULRatioFill
        extends decodes.tsdb.algo.AW_AlgorithmBase
{
    //AW:INPUTS
    public double total;    //AW:TYPECODE=i
    public double ratio;    //AW:TYPECODE=i
    String[] _inputNames = { "total", "ratio" };
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
    HashMap<String, Double> ratioMap = new HashMap<>();

    PropertySpec[] specs =
            {
                    new PropertySpec("rounding", PropertySpec.BOOLEAN,
                            "(default=false) If true, then rounding is done on the output value."),
                    new PropertySpec("validation_flag", PropertySpec.STRING,
                            "(empty) Always set this validation flag in the output."),
                    new PropertySpec("flags", PropertySpec.STRING,
                            "(empty) Always set these dataflags in the output."),
                    new PropertySpec("coeff_year", PropertySpec.INT,
                            "(1985) What year to write coefficients into"),
            };



//AW:LOCALVARS_END

    //AW:OUTPUTS
    String[] _outputNames = {};

//AW:OUTPUTS_END

    //AW:PROPERTIES
    public boolean rounding = false;
    public String validation_flag = "";
    public long coeff_year = 1985;
    public String flags = "";

    String[] _propertyNames = { "validation_flag", "rounding", "coeff_year", "flags" };

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
        outputSeries.clear();

        // handle flags and output series here because they're dynamic
        if (!flags.isEmpty())
            flag |= HdbFlags.hdbDerivation2flag(flags);
        if (!validation_flag.isEmpty())
            flag |= HdbFlags.hdbValidation2flag(validation_flag.charAt(1));

        // get the input and output parameters and see if its model data
        ParmRef totalRef = getParmRef("total");
        if (totalRef == null) {
            warning("Unknown variable 'total'");
            return;
        }
        DbKey totalSDI = totalRef.timeSeries.getSDI();

        ParmRef ratioRef = getParmRef("ratio");
        if (ratioRef == null) {
            warning("Unknown variable 'ratio'");
            return;
        }
        DbKey ratioSDI = ratioRef.timeSeries.getSDI();

        String input_interval1 = totalRef.compParm.getInterval();
        if (input_interval1 == null || !input_interval1.equals("year"))
            warning("Wrong total interval for " + comp.getAlgorithmName());
        String table_selector1 = totalRef.compParm.getTableSelector();
        if (table_selector1 == null || !table_selector1.equals("R_"))
            warning("Invalid table selector for algorithm, only R_ supported");

        // get the connection  and a few other classes so we can do some sql
        conn = tsdb.getConnection();

        String status;
        DataObject dbobj = new DataObject();
        dbobj.put("ALG_VERSION",alg_ver);
        DBAccess db = new DBAccess(conn);

        query = "select outts.ts_id ts, r.value ratio from " +
                "hdb_site_datatype insd, hdb_site out, " +
                "hdb_site_datatype outsd, cp_ts_id outts, " +
                "hdb_site_datatype inratsd, hdb_site_datatype ratiosd, " +
                "r_year r " +
                "where " +
                "insd.site_datatype_id = " + totalSDI + " and " +
                "insd.site_id = out.basin_id and " +
                "insd.datatype_id = outsd.datatype_id and " +
                "outsd.site_id = out.site_id and " +
                "outsd.site_datatype_id = outts.site_datatype_id and " +
                "outts.interval = '" + getInterval("ratio") + "' and " +
                "outts.table_selector = '" + getTableSelector("ratio") + "' and " +
                "inratsd.site_datatype_id = " + ratioSDI + " AND " +
                "inratsd.datatype_id = ratiosd.datatype_id and " +
                "ratiosd.site_id = out.site_id and " +
                "ratiosd.site_datatype_id = r.site_datatype_id AND " +
                "extract(year from r.start_date_time) = " + coeff_year;

        status = db.performQuery(query,dbobj); // interface has no methods for parameters

        debug3(" SQL STRING:" + query + "   DBOBJ: " + dbobj + "STATUS:  " + status);
        // now see if the aggregate query worked if not abort!!!

        int count = Integer.parseInt(dbobj.get("rowCount").toString());
        if (status.startsWith("ERROR") || count < 1 || !HDBAlgoTSUtils.findOutputSeries(dbobj, dao, outputSeries))
        {
            throw new DbCompException(comp.getName()+"-"+alg_ver+" Aborted or no output timeseries for basin " + getSiteName("total") + ":" + status);
        }

        ArrayList<Object> ratios;
        ArrayList<Object> tsids;
        Object r = dbobj.get("ratio");
        Object t = dbobj.get("ts");
        if (count == 1)
        {
            ratios = new ArrayList<>();
            tsids = new ArrayList<>();
            ratios.add(r);
            tsids.add(t);
        }
        else
        {
            ratios = (ArrayList<Object>)r;
            tsids = (ArrayList<Object>)t;
        }

        Iterator<Object> itR = ratios.iterator();
        Iterator<Object> itT = tsids.iterator();
        try {
            while(itR.hasNext() && itT.hasNext()) {
                String id = itT.next().toString();
                ratioMap.putIfAbsent(id, Double.valueOf(itR.next().toString()));
            }
        } catch (Exception e) {
            warning(e.toString());
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

        for (Map.Entry<String, Double> entry : ratioMap.entrySet()) {
            String id = entry.getKey();
            Double rat = entry.getValue();
            // total is value suppled by CP for this state/trib
            outputSeries.get(id).addSample(new TimedVariable(cal.getTime(), total * rat, flag));
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
        debug1(comp.getAlgorithmName()+"-"+alg_ver+" BEGINNING OF AFTER TIMESLICES, SDI: " + getSDI("total"));

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