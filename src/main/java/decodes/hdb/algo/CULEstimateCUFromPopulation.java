package decodes.hdb.algo;

import decodes.hdb.HdbFlags;
import decodes.hdb.dbutils.DBAccess;
import decodes.hdb.dbutils.DataObject;
import decodes.hdb.dbutils.HDBAlgoTSUtils;
import decodes.sql.DbKey;
import decodes.tsdb.*;
import decodes.tsdb.algo.AWAlgoType;
import decodes.util.PropertySpec;
import ilex.util.TextUtil;
import ilex.var.NamedVariable;
import ilex.var.TimedVariable;
import opendcs.dai.TimeSeriesDAI;

import java.sql.Connection;
import java.text.SimpleDateFormat;
import java.util.*;

import static decodes.tsdb.VarFlags.TO_WRITE;

//AW:IMPORTS
// Place an import statements you need here.
//AW:IMPORTS_END

//AW:JAVADOC

/**
 This algorithm estimates annual M&I consumptive use from the previous USGS estimate of state-huc CU multiplied by
 the current census estimate of annual state-trib population divided by the USGS estimate of state-trib pop for the same estimate year.
 Computations will be specific to state-tribs and will have groups for the component state-hucs. Estimates will be retrieved by
 using the CP "PREV" property and a very large window to find the last value.

 Only applies to R_YEAR

 Property:
 ROUNDING: determines if rounding to the 7th decimal point is desired, default FALSE

 Algorithm is as follows:
 In beforeTimeslice, examine input data for any triggered values that are from the estimates.
 For these timeslices, add additional timeslices to extend reach of estimates to their proper length.
 Query the database to find the number of additional years to add.

 working? SQL query to compute years needed:
 with d as (select to_date('01-jan-1995') d from dual), --time of est
 est as (select min(a.start_date_time) est from r_year a, d where
 a.site_datatype_id(+) = 31498 and a.start_date_time(+) > d.d ), -- sdi of est
 cur as (select max(b.start_date_time) cur from r_year b, d where
 b.site_datatype_id = 31507 and b.start_date_time > d.d ) -- sdi of current pop
 select months_between(nvl(est.est,cur.cur),d.d)/12 years from est, cur, d;

 */
//AW:JAVADOC_END
public class CULEstimateCUFromPopulation
        extends decodes.tsdb.algo.AW_AlgorithmBase
{
    //AW:INPUTS
    public static final String EST_MI_CU = "est_mi_cu";
    public static final String EST_POP = "est_pop";
    public static final String CUR_POP = "cur_pop";
    public static final String CUR_MI_CU = "cur_mi_cu";
    long curPopDI;
    long estPopDI;
    long estCUDI;
    long curCUDI;
    public double est_mi_cu;    //AW:TYPECODE=i
    public double est_pop;    //AW:TYPECODE=i
    public double cur_pop;    //AW:TYPECODE=i

    String[] _inputNames = {EST_MI_CU, EST_POP, CUR_POP};
//AW:INPUTS_END

    //AW:LOCALVARS
    // Enter any local class variables needed by the algorithm.
    String alg_ver = "1.0";
    String query;
    String dateQuery;
    String tribPopQuery;
    String hucPopQuery;
    String status;

    Connection conn = null;
    DBAccess db = null;
    DataObject dbobj = new DataObject();

    TimeSeriesDAI dao = null;
    HashMap<String,CTimeSeries> outputSeries = new HashMap<>();
    SimpleDateFormat sdf = new SimpleDateFormat("dd-MM-yyyy HH:mm");

    PropertySpec[] specs =
            {
                    new PropertySpec("rounding", PropertySpec.BOOLEAN,
                            "(default=false) If true, then rounding is done on the output value."),
                    new PropertySpec("validation_flag", PropertySpec.STRING,
                            "(empty) Always set this validation flag in the output."),
            };

//AW:LOCALVARS_END

    //AW:OUTPUTS
    public NamedVariable cur_mi_cu = new NamedVariable(CUR_MI_CU, 0);
    String[] _outputNames = {CUR_MI_CU};
//AW:OUTPUTS_END

    //AW:PROPERTIES
    public boolean rounding = false;
    public String validation_flag = "";
    public String flags;

    String[] _propertyNames = { "validation_flag", "rounding" };
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
        conn = tsdb.getConnection();
        db = new DBAccess(conn);
        dao = tsdb.makeTimeSeriesDAO();
        dbobj.put("ALG_VERSION",alg_ver);
        outputSeries.clear();
        debug3("beforeTimeSlices");

        TreeSet<Date> newDates = new TreeSet<>();

        ParmRef epref = getParmRef(EST_POP);
        if (epref != null)
            newDates.addAll(estimateTriggeredDates(epref));
        epref = getParmRef(EST_MI_CU);
        if (epref != null)
            newDates.addAll(estimateTriggeredDates(epref));

        if (!newDates.isEmpty())
        {
            try
            {
                dao.fillTimeSeries(getParmRef(CUR_POP).timeSeries,newDates);
            }
            catch (NullPointerException | BadTimeSeriesException ignored) {} //not an error if triggered at state-huc level
            catch (DbIoException e)
            {
                warning(comp.getAlgorithmName()+"-"+alg_ver+" problem filling timeseries for triggered estimate inputs. "+ e);
            }
        }
        baseTimes.addAll(newDates);

        flags = "";
        conn = null;
        curPopDI = getParmRef(CUR_POP).compParm.getDataTypeId().getValue();
        estPopDI = getParmRef(EST_POP).compParm.getDataTypeId().getValue();
        estCUDI = getParmRef(EST_MI_CU).compParm.getDataTypeId().getValue();
        curCUDI = getParmRef(CUR_MI_CU).compParm.getDataTypeId().getValue();

        tribPopQuery = "est_pop as ( " +
                "select value pop  " +
                "from r_year y, hdb_site_datatype sd, " +
                "hdb_site_datatype tribsd, d where " +
                getSDI(CUR_POP) + " = tribsd.site_datatype_id and " + //-- input sdi for total pop
                "y.start_date_time = ( " +
                "select max(y.start_date_time) from  " +
                "r_year y where y.site_datatype_id = sd.site_datatype_id and " +
                "y.start_date_time <= d.d) and " + //-- date_time of input data
                "sd.site_id = tribsd.site_id and " +
                "sd.datatype_id = " + estPopDI + " and " +
                "y.site_datatype_id = sd.site_datatype_id " +
                "), " +
                "cur_pop as ( " +
                "select value pop  " +
                "from r_year y, d " +
                "where " +
                getSDI(CUR_POP) + " = y.site_datatype_id and  " + // input sdi for total pop
                "y.start_date_time = d.d " + // -- date_time of input data
                ") ";
        hucPopQuery = "est_pop as ( " +
                "select value pop " +
                "from r_year y, hdb_site_datatype estcusd, " +
                "hdb_site statehuc, hdb_site_datatype tribsd, d  " +
                "where " + getSDI(EST_MI_CU) + " = estcusd.site_datatype_id and  " +
                "y.start_date_time = (  " +
                "select max(y.start_date_time) from r_year y  " +
                "where y.site_datatype_id = tribsd.site_datatype_id " +
                "and y.start_date_time <= d.d) and " +
                "y.site_datatype_id = tribsd.site_datatype_id and " +
                "statehuc.site_id = estcusd.site_id and " +
                "statehuc.parent_site_id = tribsd.site_id and " +
                "tribsd.datatype_id = " + estPopDI + "), " +
                "cur_pop as (select value pop " +
                "from r_year y, hdb_site_datatype estcusd, " +
                "hdb_site statehuc, hdb_site_datatype tribsd, d, est_pop  " +
                "where " + getSDI(EST_MI_CU) + " = estcusd.site_datatype_id and  " +
                "y.start_date_time = d.d and " +
                "y.site_datatype_id = tribsd.site_datatype_id and " +
                "statehuc.site_id = estcusd.site_id and " +
                "statehuc.parent_site_id = tribsd.site_id and " +
                "tribsd.datatype_id = " + curPopDI + ")";
//AW:BEFORE_TIMESLICES_END
    }

    /**
     * Handles cases when estimate is a triggered input, and therefore we need to compute for the timeseries until
     * the next estimate.
     *
     * @param epref The input parameter to extend basetimes for
     * @return TreeSet<Date> of new dates for timeseries filling and timeslice computations
     */
    private TreeSet<Date> estimateTriggeredDates(ParmRef epref) {
        int n = epref.timeSeries.size(); //vars is private, can't use an iterator
        TreeSet<Date> newDates = new TreeSet<>();
        for(int i=0; i<n; i++)
        {
            TimedVariable tv = epref.timeSeries.sampleAt(i);
            if ((tv.getFlags() &
                    (VarFlags.DB_ADDED | VarFlags.DB_DELETED)) != 0) // is a trigger
            {

                GregorianCalendar cal = new GregorianCalendar();
                // Projects from current timestep to next timestep or to the end of the cur pop input data
                query = "with d as (select to_date('" + sdf.format(tv.getTime()) + "','dd-MM-yyyy HH24:MI') d from dual), " +
                        "est as (select min(a.start_date_time) est from r_year a, d where " +
                        "a.site_datatype_id = " + getSDI(epref.role) + " and a.start_date_time > d.d ), " +
                        "cur as (select max(b.start_date_time) cur from r_year b, d where " +
                        "b.site_datatype_id = " + getSDI(CUR_POP) + " and b.start_date_time > d.d) " +
                        "select floor(months_between(nvl(est.est,nvl(cur.cur,trunc(sysdate,'YEAR'))),d.d)/12) years from est, cur, d";

                status = db.performQuery(query,dbobj); // interface has no methods for parameters

                debug3(" SQL STRING:" + query + "   DBOBJ: " + dbobj.toString() + "STATUS:  " + status);

                if (status.startsWith("ERROR"))
                {
                    warning(comp.getAlgorithmName()+"-"+alg_ver+" problem handling triggered estimate inputs, see following error message");
                    warning(status);
                    break;
                }
                cal.setTime(tv.getTime());
                int years = 0;
                if (Integer.parseInt(dbobj.get("rowCount").toString()) > 0) {
                    years = Integer.parseInt(dbobj.get("years").toString());
                }
                for (int j=1; j<years ;j++) { //years between this estimate and the next. We don't need to add the year with the next estimate
                    cal.add(Calendar.YEAR,1);
                    newDates.add(cal.getTime());
                }
            }
        }
        return newDates;
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

        dateQuery = "with d as (select to_date('" + sdf.format(_timeSliceBaseTime) + "', 'dd-MM-yyyy HH24:MI') d from dual), ";

        // all timeslices affected by our triggered variable are in the list, just need to figure out if we're operating
        // on state-trib or state-huc level
        if (!isMissing(cur_pop)) //triggered at state-trib, so cur pop will be found unless in 1985. Want it to fail then anyway.
        { // state-trib level, so need to create timeseries for output of each state-huc

            query = dateQuery +
                    "est_mi_cu as (select ts_id output_cu_tsid, value cu  " +
                    "from r_year y, hdb_site statehuc, " +
                    "hdb_site_datatype tribsd, hdb_site_datatype estcusd, " +
                    "hdb_site_datatype outsd, cp_ts_id id, d where " +
                    getSDI(CUR_POP) + " = tribsd.site_datatype_id and " + // -- input sdi for total pop
                    "y.start_date_time = ( " +
                    "select max(y.start_date_time) from  " +
                    "r_year y where y.site_datatype_id = estcusd.site_datatype_id and " +
                    "y.start_date_time <= d.d) and " + //-- date_time of input data
                    "statehuc.parent_site_id = tribsd.site_id and " +
                    "statehuc.site_id = estcusd.site_id and " +
                    "estcusd.datatype_id = " + estCUDI + " and " +
                    "y.site_datatype_id = estcusd.site_datatype_id and " +
                    "statehuc.site_id = outsd.site_id and " +
                    "outsd.datatype_id = " + curCUDI + " and " +
                    "id.site_datatype_id = outsd.site_datatype_id and " +
                    "id.interval = 'year' and " +
                    "id.table_selector = 'R_' " +
                    "), " +
                    tribPopQuery;

        } else // will never see both levels in the same computation timeslice, because estimate and current pops not at state-huc level
        { //state-huc level, query for output tsid and est cu. may already have these, but for consistency
            query = dateQuery +
                    "est_mi_cu as (select ts_id output_cu_tsid, value cu " +
                    "from r_year y, hdb_site_datatype estcusd, " +
                    "hdb_site_datatype outsd, cp_ts_id id, d " +
                    "where " + getSDI(EST_MI_CU) + "= estcusd.site_datatype_id and " +
                    "y.start_date_time = ( " +
                    "select max(y.start_date_time) from r_year y " +
                    "where y.site_datatype_id = estcusd.site_datatype_id " +
                    "and y.start_date_time <= d.d) and " +
                    "y.site_datatype_id = estcusd.site_datatype_id and " +
                    "estcusd.site_id = outsd.site_id and " +
                    "outsd.datatype_id = " + curCUDI + " and " +
                    "id.site_datatype_id = outsd.site_datatype_id and " +
                    "id.interval = 'year' and " +
                    "id.table_selector = 'R_' ), " +
                    hucPopQuery;
        }

        if(rounding)
            //out = BigDecimal.valueOf(out).setScale(7).doubleValue(); //rounding in Java
            query += "select output_cu_tsid ts, round(est_mi_cu.cu*cur_pop.pop/est_pop.pop,7) cur_mi_cu from est_mi_cu, est_pop, cur_pop";
        else
            query += "select output_cu_tsid ts, est_mi_cu.cu*cur_pop.pop/est_pop.pop cur_mi_cu from est_mi_cu, est_pop, cur_pop";

        debug3("TimeSlice Query: " + query);
        status = db.performQuery(query,dbobj);
        if (status.startsWith("ERROR") || !HDBAlgoTSUtils.findOutputSeries(dbobj, dao, outputSeries) ||
                outputSeries.isEmpty())
        {
            warning(comp.getName() + "-" + alg_ver + " TimeSlice aborted at: " + debugSdf.format(this._timeSliceBaseTime) +
                    " See following error message:");
            warning(status);
            throw new DbCompException("Error retrieving timeseries for output, cannot continue");
        }

        if (status.startsWith("ERROR")) // unreachable, will throw exception above
        {

            warning(status);
            // deletions?!?
            //outputSeries.addSample(new TimedVariable(_timeSliceBaseTime, 0, TO_DELETE));
        }
        else
        {
            int count = Integer.parseInt(dbobj.get("rowCount").toString());
            ArrayList<Object> outputs;
            ArrayList<Object> values;
            Object o = dbobj.get("ts");
            Object v = dbobj.get(CUR_MI_CU);
            if (count == 1)
            {
                outputs = new ArrayList<>();
                values = new ArrayList<>();
                outputs.add(o);
                values.add(v);
            }
            else
            {
                outputs = (ArrayList<Object>)o;
                values = (ArrayList<Object>)v;
            }

            Iterator<Object> itO = outputs.iterator();
            Iterator<Object> itV = values.iterator();
            try {
                while(itO.hasNext() && itV.hasNext()) {
                    String id = itO.next().toString();
                    debug3("Output Timeseries ID: " + id);
                    Double value_out = new Double(itV.next().toString());
                    debug3(comp.getName() + "-" + alg_ver + " Setting output for timeslice: " + debugSdf.format(this._timeSliceBaseTime) +
                            " value: " + value_out);
                    TimedVariable tv = new TimedVariable(_timeSliceBaseTime, value_out, TO_WRITE);
                    if (flags.length() > 0)
                        tv.setFlags(tv.getFlags() | HdbFlags.hdbDerivation2flag(flags));
                    if (validation_flag.length() > 0)
                        tv.setFlags(tv.getFlags() | HdbFlags.hdbValidation2flag(validation_flag.charAt(1)));
                    if (tsdb.isHdb() && TextUtil.str2boolean(comp.getProperty("OverwriteFlag")))
                    {
                        //waiting on release of overwrite flag upstream
                        //v.setFlags(v.getFlags() | HdbFlags.HDBF_OVERWRITE_FLAG);
                    }
                    TimeSeriesIdentifier tsid = dao.getTimeSeriesIdentifier(DbKey.createDbKey(Long.parseLong(id)));
                    outputSeries.get(tsid.getSite().getId().toString()).addSample(tv);
                }
            } catch (Exception e) {
                warning(e.toString());
            }
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
        // set the outputs. If some timesteps failed, must be marked for delete above
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