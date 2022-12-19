package decodes.hdb.algo;

import decodes.hdb.HdbFlags;
import decodes.hdb.dbutils.DBAccess;
import decodes.hdb.dbutils.DataObject;
import decodes.hdb.dbutils.RBASEUtils;
import decodes.sql.DbKey;
import decodes.tsdb.*;
import decodes.tsdb.algo.AWAlgoType;
import decodes.util.DecodesSettings;
import decodes.util.PropertySpec;
import ilex.util.TextUtil;
import ilex.var.NamedVariable;
import ilex.var.TimedVariable;
import opendcs.dai.TimeSeriesDAI;

import java.sql.Connection;
import java.text.SimpleDateFormat;
import java.util.*;

import static decodes.tsdb.VarFlags.TO_DELETE;
import static decodes.tsdb.VarFlags.TO_WRITE;

//AW:IMPORTS
// Place an import statements you need here.
//AW:IMPORTS_END

//AW:JAVADOC

/**
 This algorithm spatially aggregates sites to a target spatial resolution defined by
 a property. Does any aggregating function other than SUM make sense?

 OUTPUT_SITE_METHOD: Type of spatial resolution to use. Options are Parent, HUC, Basin, and ? for compdepends.
 PEER_SITE_METHOD: Method to select sites to aggregate. Options are Parent, HUC, Basin, and compdepends.
 NO_ROUNDING: determines if rounding to the 7th decimal point is desired, default FALSE

 Algorithm is as follows:
 Sum all sites found using the peer method.
 Find output sdi with method specified.
 Store as time-slice output on the determined site.

 Query hdb_site references for parent or basin site_id, HUC site_name with CU&L objecttype, and ?

 working SQL queries to compute data:

 WITH t AS
 (SELECT r.value FROM r_month r, hdb_site_datatype sourcesd, hdb_site_datatype destsd,
 hdb_site trig, hdb_site members
 WHERE
 sourcesd.site_datatype_id = 29409 AND
 trig.site_id = sourcesd.site_id AND
 --members.hydrologic_unit = trig.hydrologic_unit AND
 --members.state_id = trig.state_id AND
 --members.basin_id = trig.basin_id AND
 --members.parent_id = trig.parent_id AND
 members.site_id = destsd.site_id AND
 sourcesd.datatype_id = destsd.datatype_id AND
 trig.objecttype_id = members.objecttype_id AND
 r.site_datatype_id = destsd.site_datatype_id AND
 r.start_date_time = '01-JAN-1990')
 SELECT sum(t.VALUE) FROM t;


 WITH t AS
 (SELECT r.value FROM r_month r, cp_comp_depends dep,
 cp_ts_id id
 WHERE
 dep.computation_id = 5429 AND
 dep.ts_id = cp_ts_id.ts_id AND
 r.site_datatype_id = id.site_datatype_id AND
 r.start_date_time = '01-JAN-1990')
 SELECT sum(t.VALUE) FROM t;

 SELECT
 outputsd.site_datatype_id
 FROM hdb_site_datatype sourcesd, hdb_site_datatype outputsd,
 hdb_site trig, hdb_site output
 WHERE
 sourcesd.site_datatype_id = 29409 AND
 sourcesd.site_id = trig.site_id AND
 --outputsd.site_id = trig.parent_site_id AND
 --outputsd.site_id = trig.basin_id AND
 --output.site_name = trig.hydrologic_unit AND
 outputsd.site_id = output.site_id AND
 outputsd.datatype_id = sourcesd.datatype_id
 ;

 */
//AW:JAVADOC_END
public class DynamicSpatialAggregateAlg
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
    String selectClause;
    Connection conn = null;
    DBAccess db = null;
    DataObject dbobj;
    TimeSeriesDAI dao = null;
    CTimeSeries outputSeries = null;

    PropertySpec[] specs =
            {
                    new PropertySpec("ignore_partials", PropertySpec.BOOLEAN,
                            "(default=true) If false, then attempt to compute coefficients for years without all 12 months. Not recommended."),
                    new PropertySpec("rounding", PropertySpec.BOOLEAN,
                            "(default=false) If true, then rounding is done on the output value."),
                    new PropertySpec("validation_flag", PropertySpec.STRING,
                            "(empty) Always set this validation flag in the output."),
                    new PropertySpec("flags", PropertySpec.STRING,
                            "(empty) Always set these dataflags in the output."),
                    new PropertySpec("output_site_method", PropertySpec.STRING,
                            "(HUC) Which method should be used to find the output site."),
                    new PropertySpec("peer_site_method", PropertySpec.STRING,
                            "(HUC) Which method should be used to find the sites to summarize"),
            };



//AW:LOCALVARS_END

    //AW:OUTPUTS
    public NamedVariable output = new NamedVariable("output", 0);
    String[] _outputNames = { "output"};
//AW:OUTPUTS_END

    //AW:PROPERTIES
    public boolean ignore_partials = true;
    public boolean rounding = false;
    public String validation_flag = "";
    public String flags = "";
    public String output_site_method = "HUC";
    public String peer_site_method = "HUC";

    String[] _propertyNames = { "ignore_partials", "rounding", "validation_flag", "flags",
            "output_site_method", "peer_site_method" };

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

        dbobj = new DataObject();
        dbobj.put("ALG_VERSION",alg_ver);


        //get output TS_ID
        query = " SELECT\n" +
                " id.ts_id ts\n" +
                " FROM hdb_site_datatype sourcesd, hdb_site_datatype outputsd,\n" +
                " hdb_site trig, hdb_site output, cp_ts_id id, hdb_objecttype obj\n" +
                " WHERE\n" +
                " sourcesd.site_datatype_id = " + getSDI("input") + " AND\n" +
                " sourcesd.site_id = trig.site_id AND\n" +
                " outputsd.site_id = output.site_id AND\n" +
                " outputsd.datatype_id = sourcesd.datatype_id AND\n" + // want this limited to only output same datatype?
                " id.site_datatype_id = outputsd.site_datatype_id AND\n" +
                " id.interval = '" + getInterval("input") + "' AND " +
                " id.table_selector = '" + getTableSelector("input") + "' AND " +
                " output.objecttype_id = obj.objecttype_id AND\n";

        if (output_site_method.equalsIgnoreCase("HUC"))
        {
            query += " output.site_name = trig.hydrologic_unit AND \n" +
                     " obj.objecttype_tag = 'huc'\n";
        }
        else if (output_site_method.equalsIgnoreCase("Basin"))
        {
            query += " outputsd.site_id = trig.basin_id\n";
        }
        else if (output_site_method.equalsIgnoreCase("Parent"))
        {
            query += " outputsd.site_id = trig.parent_site_id\n";
        }
        else
        {
            warning(comp.getName() + "-" + alg_ver + " Aborted: output_site_method property incorrect! " + output_site_method);
            return;
        }
        debug3("Before TimeSlice Query: " + query);

        conn = tsdb.getConnection();
        db = new DBAccess(conn);

        String status = db.performQuery(query,dbobj);
        if (status.startsWith("ERROR"))
        {
            throw new DbCompException(comp.getName() + "-" + alg_ver + " Aborted: see following error message: " + status);
        }

        if (Integer.parseInt(dbobj.get("rowCount").toString()) == 0)
        {
            throw new DbCompException(comp.getName() + "-" + alg_ver + " No timeseries returned for " + getSiteName("input"));
        }

        dao = tsdb.makeTimeSeriesDAO();
        DbKey ts = DbKey.createDbKey(Long.parseLong(dbobj.get("ts").toString()));
        debug3("Output Timeseries ID: " + ts);
        TimeSeriesIdentifier tsid;
        try {
            tsid = dao.getTimeSeriesIdentifier(ts);
            outputSeries = dao.makeTimeSeries(tsid);
        } catch (DbIoException | NoSuchObjectException e) {
            dao.close();
            throw new DbCompException(e.toString());
        }

        // now build query for sums
        boolean foundPeers = true;
        query = " WITH t AS\n" +
                " (SELECT r.value FROM r_" + getInterval("input") + " r, hdb_site_datatype sourcesd, hdb_site_datatype destsd,\n" +
                " hdb_site trig, hdb_site members\n" +
                " WHERE\n" +
                " sourcesd.site_datatype_id = " + getSDI("input")+ " AND\n" +
                " trig.site_id = sourcesd.site_id AND\n";

        if (peer_site_method.equalsIgnoreCase("HUC"))
        {
            query += " members.hydrologic_unit = trig.hydrologic_unit AND\n";
        }
        else if (peer_site_method.equalsIgnoreCase("Basin"))
        {
            query += " members.basin_id = trig.basin_id AND\n";
        }
        else if (peer_site_method.equalsIgnoreCase("Parent"))
        {
            query += " members.parent_site_id = trig.parent_site_id AND\n";
        }
        else
        {
            foundPeers = false;
        }
        query +=" members.site_id = destsd.site_id AND\n" +
                " sourcesd.datatype_id = destsd.datatype_id AND\n" +
                " trig.objecttype_id = members.objecttype_id AND\n" +
                " r.site_datatype_id = destsd.site_datatype_id AND\n";

        if ( !foundPeers && peer_site_method.equalsIgnoreCase("CompDepends"))
        {
            foundPeers = true;
            query = " WITH t AS\n" + //overwrite query completely
                    " (SELECT r.value FROM r_" + getInterval("input") + " r, cp_comp_depends dep,\n" +
                    " cp_ts_id id\n" +
                    " WHERE\n" +
                    " dep.computation_id = " + comp.getId() + " AND\n" +
                    " dep.ts_id = cp_ts_id.ts_id AND\n" +
                    " r.site_datatype_id = id.site_datatype_id AND\n";
        }

        if (!foundPeers)
        {
            outputSeries = null;
            throw new DbCompException(comp.getName() + "-" + alg_ver + " Aborted: peer_site_method property incorrect! " + output_site_method);
        }

        selectClause = " SELECT sum(VALUE) value FROM t";
        if (rounding)
        {
            selectClause = " SELECT round(sum(VALUE),7) value FROM t"; // 7 used by other HDB aggregates
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
        debug3(comp.getName() + "-" + alg_ver + " BEGINNING OF doAWTimeSlice for period: " +
                _timeSliceBaseTime + " SDI: " + getSDI("input"));

        if (outputSeries == null) return;

        SimpleDateFormat sdf = new SimpleDateFormat("dd-MM-yyyy HH:mm");
        sdf.setTimeZone(
                TimeZone.getTimeZone(
                        DecodesSettings.instance().aggregateTimeZone));
        String status;

        String dateQuery = " r.start_date_time = to_date('" + sdf.format(_timeSliceBaseTime) + "'," +
                           "'dd-MM-yyyy HH24:MI'))\n"; // last ) to close WITH statement

        String sliceQuery = query + dateQuery + selectClause;
        debug3("TimeSlice Query: " + sliceQuery);

        status = db.performQuery(sliceQuery,dbobj);
        if (status.startsWith("ERROR") || Integer.parseInt(dbobj.get("rowCount").toString()) != 1)
        {
            warning(comp.getName() + "-" + alg_ver + " TimeSlice aborted at: " + debugSdf.format(this._timeSliceBaseTime) +
                    " See following error message:");
            warning(status);
            outputSeries.addSample(new TimedVariable(_timeSliceBaseTime, 0, TO_DELETE));
        }
        else
        {
            Double value_out = new Double(dbobj.get("value").toString());
            debug3(comp.getName() + "-" + alg_ver + "Setting output for timeslice " + debugSdf.format(this._timeSliceBaseTime) + ": " + value_out);

            TimedVariable v = new TimedVariable(_timeSliceBaseTime, value_out, TO_WRITE);
            if (flags.length() > 0)
                v.setFlags(v.getFlags() | HdbFlags.hdbDerivation2flag(flags));
            if (validation_flag.length() > 0)
                v.setFlags(v.getFlags() | HdbFlags.hdbValidation2flag(validation_flag.charAt(1)));
            if (tsdb.isHdb() && TextUtil.str2boolean(comp.getProperty("OverwriteFlag")))
            {
                //waiting on release of overwrite flag upstream
                //v.setFlags(v.getFlags() | HdbFlags.HDBF_OVERWRITE_FLAG);
            }
            outputSeries.addSample(v);
        }

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
        if (outputSeries == null) return;

        // set the outputs. If some timesteps failed, they will be marked for delete and be deleted here.
        try {
            debug1(comp.getName() + "-" + alg_ver + "saving timeseries: " + outputSeries.getBriefDescription());
            outputSeries.setComputationId(comp.getId());
            dao.saveTimeSeries(outputSeries);
        } catch (DbIoException | BadTimeSeriesException e) {
            warning("Exception during saving output to database:" + e);
        }
        dao.close();
        outputSeries = null; //clean up
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
