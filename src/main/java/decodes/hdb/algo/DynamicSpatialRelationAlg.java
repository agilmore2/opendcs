package decodes.hdb.algo;

import decodes.hdb.HdbFlags;
import decodes.hdb.dbutils.DBAccess;
import decodes.hdb.dbutils.DataObject;
import decodes.sql.DbKey;
import decodes.tsdb.*;
import decodes.tsdb.algo.AWAlgoType;
import decodes.util.DecodesSettings;
import decodes.util.PropertySpec;
import ilex.util.TextUtil;
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
 This algorithm spatially aggregates sites to a target site defined by
 an attribute id and a weight in ref_spatial_relation.

 ATTRIBUTE: HDB_ATTR entry to use:
 If attr_value_type is string or date, each site contributes 100% to site_b
 If attr_value_type is number, the ref_spatial_relation VALUE field is used to weight contributions
 ROUNDING: determines if rounding to the 5th decimal point is desired, default FALSE

 Algorithm is as follows:
 Sum all sites found using same attribute and site_b, using weights if necessary
 Find output sdi as site_b for same datatype
 Store as time-slice output on the determined site.

 working SQL queries to compute data:

  WITH t AS    
  (SELECT r.value value, sr_as.value weight, sr_as.attr_id, sr_as.b_site_id output_site  
  FROM r_year r, ref_spatial_relation sr_trig,  ref_spatial_relation sr_as,  hdb_site_datatype trig, hdb_site_datatype members  
  WHERE  trig.site_datatype_id = 31213 AND  -- Moffact county population
  sr_trig.a_site_id = trig.site_id AND  
  sr_trig.attr_id = 452 AND -- % site A population located in site b
  sr_as.attr_id = sr_trig.attr_id AND  
  sr_as.b_site_id = sr_trig.b_site_id AND  
  members.site_id = sr_as.a_site_id AND  
  members.datatype_id = trig.datatype_id AND  
  r.site_datatype_id = members.site_datatype_id AND  
  r.start_date_time = to_date('01-01-1986 00:00','dd-MM-yyyy HH24:MI'))  
 SELECT output_site,  sum(t.VALUE*decode(a.attr_value_type,'number', t.weight,1.0)) value,  sum(t.weight) total
 FROM t, hdb_attr a  where a.attr_id = t.attr_id  group by output_site

 To find output sdis:

 SELECT
 id.ts_id ts, outputsdis.site_id
 FROM hdb_site_datatype sourcesd, ref_spatial_relation sr,
 hdb_site_datatype outputsdis, cp_ts_id id 
 WHERE
sourcesd.site_datatype_id = 31213 AND -- -- CO/GREEN RIVER TRIB Population
sr.attr_id = 
(SELECT attr_id FROM hdb_attr WHERE attr_name = 'percent site_a population located within site_b') AND
 sr.a_site_id = sourcesd.site_id AND
 outputsdis.site_id = sr.b_site_id AND
outputsdis.datatype_id = sourcesd.datatype_id AND
 id.site_datatype_id = outputsdis.site_datatype_id AND
 id.interval = 'year' AND
 id.table_selector = 'R_'
 ;

 */
//AW:JAVADOC_END
public class DynamicSpatialRelationAlg
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
    String status;
    String selectClause;

    Connection conn = null;
    DBAccess db = null;
    DataObject dbobj = new DataObject();
    TimeSeriesDAI dao = null;
    HashMap<String,CTimeSeries> outputSeries = new HashMap<>();

    PropertySpec[] specs =
            {
                    new PropertySpec("rounding", PropertySpec.BOOLEAN,
                            "(default=false) If true, then rounding is done on the output value"),
                    new PropertySpec("validation_flag", PropertySpec.STRING,
                            "(empty) Always set this validation flag in the output"),
                    new PropertySpec("flags", PropertySpec.STRING,
                            "(empty) Always set these dataflags in the output"),
                    new PropertySpec("attribute", PropertySpec.STRING,
                            "(empty) Which attribute should be used for aggregation"),
            };



//AW:LOCALVARS_END

    //AW:OUTPUTS
    String[] _outputNames = {};

//AW:OUTPUTS_END

    //AW:PROPERTIES
    public boolean rounding = false;
    public String validation_flag = "";
    public String flags = "";
    public String attribute = null;

    String[] _propertyNames = { "rounding", "validation_flag", "flags", "attribute"};

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

        dbobj.put("ALG_VERSION",alg_ver);
        conn = tsdb.getConnection();
        db = new DBAccess(conn);
        dao = tsdb.makeTimeSeriesDAO();
        outputSeries.clear();

        //get output TS_IDs
        query = " SELECT " +
                " id.ts_id ts, outputsdis.site_id " +
                " FROM hdb_site_datatype sourcesd, ref_spatial_relation sr, " +
                " hdb_site_datatype outputsdis, cp_ts_id id " +
                " WHERE " +
                " sourcesd.site_datatype_id = " + getSDI("input") + " AND " +
                " sr.attr_id = " + 
                " (SELECT attr_id FROM hdb_attr WHERE attr_name = '" + attribute + "') AND" +
                " sr.a_site_id = sourcesd.site_id AND " +
                " outputsdis.site_id = sr.b_site_id AND " +
                " outputsdis.datatype_id = sourcesd.datatype_id AND " + // assumes output datatype = input datatype
                " id.site_datatype_id = outputsdis.site_datatype_id AND " +
                " id.interval = '" + getInterval("input") + "' AND " +
                " id.table_selector = '" + getTableSelector("input") + "'";

        debug3("Before TimeSlice Query: " + query);
        status = db.performQuery(query,dbobj);

        if (status.startsWith("ERROR") || !findOutputSeries(dbobj)) //unique findOutputSeries because need Site ID mapping
        {
            warning(comp.getName() + "-" + alg_ver + " Aborted: see following error message");
            warning(status);
            throw new DbCompException("Error retrieving timeseries for output, cannot continue");
        }
        if(outputSeries.isEmpty())
        {
        	// If the query succeeds, but finds 0 output tsid's this isn't necessarily an error
        	// For example, many minor reservoirs are not aggregated to a state/huc/cp
        	// For these cases, we just want to record that the input tsid has no outputs and move on to the next input
        	// Warning already handled in the findOutputSeries function
        	return;
        }

        // now build query for sums
        query = "  WITH t AS " +
                " (SELECT r.value value, sr_as.value weight, sr_as.attr_id, sr_as.b_site_id output_site " +
                " FROM r_" + getInterval("input") + " r, ref_spatial_relation sr_trig, " +
                " ref_spatial_relation sr_as, " +
                " hdb_site_datatype trig, hdb_site_datatype members " +
                " WHERE " +
                " trig.site_datatype_id = " + getSDI("input") + " AND " +
                " sr_trig.a_site_id = trig.site_id AND " +
                " sr_trig.attr_id = " + 
                " (SELECT attr_id FROM hdb_attr WHERE attr_name = '" + attribute + "') AND " +
                " sr_as.attr_id = sr_trig.attr_id AND " +
                " sr_as.b_site_id = sr_trig.b_site_id AND " +
                " members.site_id = sr_as.a_site_id AND " +
                " members.datatype_id = trig.datatype_id AND " +
                " r.site_datatype_id = members.site_datatype_id AND ";

        selectClause = " SELECT output_site, ";
        if (rounding)
        {
            selectClause += " round(sum(t.VALUE*decode(a.attr_value_type,'number', t.weight,1.0)),7) value"; // 7 used by other HDB aggregates
        } else
        {
            selectClause += " sum(t.VALUE*decode(a.attr_value_type,'number', t.weight,1.0)) value, ";
        }
        selectClause += " sum(t.weight) weight " +
                        " FROM t, hdb_attr a " +
                        " where a.attr_id = t.attr_id " +
                        " group by output_site";

//AW:BEFORE_TIMESLICES_END
    }

    /**
     * runs supplied query for output timeseries site ids and ts_ids, updates global outputSeries hashmap
     * @param dbobj result from a db query
     * @return false if failed
     */
    private boolean findOutputSeries(DataObject dbobj) {

        // Need to handle multiple TS_IDs!
        int count = Integer.parseInt(dbobj.get("rowCount").toString());
        ArrayList<Object> tsids;

        if (count == 0)
        {
            warning(comp.getName() + "-" + alg_ver + " Aborted: zero output TS_IDs, site: " + getSiteName("input") + "dbobj: " +dbobj);
            return true; // not an error
        }
        else if (count == 1)
        {
            tsids = new ArrayList<>();
            tsids.add(dbobj.get("ts"));
        }
        else
        {
            tsids = (ArrayList<Object>) dbobj.get("ts");
        }

        Iterator<Object> itId = tsids.iterator();
        try {
            while(itId.hasNext()) {
                DbKey id = DbKey.createDbKey(Long.parseLong(itId.next().toString()));
                debug3("Output Timeseries ID: " + id);
                TimeSeriesIdentifier tsid = dao.getTimeSeriesIdentifier(id);
                outputSeries.putIfAbsent(tsid.getSite().getId().toString(),dao.makeTimeSeries(tsid));
            }
        } catch (Exception e) {
            warning(e.toString());
            return false;
        }
        return true;
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
        if (outputSeries.isEmpty())
        {
            return;
        }

        debug3(comp.getName() + "-" + alg_ver + " BEGINNING OF doAWTimeSlice for period: " +
                _timeSliceBaseTime + " SDI: " + getSDI("input"));

        SimpleDateFormat sdf = new SimpleDateFormat("dd-MM-yyyy HH:mm");
        sdf.setTimeZone(TimeZone.getTimeZone(DecodesSettings.instance().aggregateTimeZone));
        String status;

        String dateQuery = " r.start_date_time = to_date('" + sdf.format(_timeSliceBaseTime) + "'," +
                           "'dd-MM-yyyy HH24:MI'))\n"; // last ) to close WITH statement

        String sliceQuery = query + dateQuery + selectClause;
        debug3("TimeSlice Query: " + sliceQuery);

        status = db.performQuery(sliceQuery,dbobj); // query returns output site id, value, and total weight
        int count = Integer.parseInt(dbobj.get("rowCount").toString());
        if (status.startsWith("ERROR") || count != outputSeries.size())
        {
            throw new DbCompException(comp.getName() + "-" + alg_ver + " TimeSlice aborted at: " + debugSdf.format(this._timeSliceBaseTime) +
                    " See following error message: " + status);
            // deletions?!?
            //outputSeries.addSample(new TimedVariable(_timeSliceBaseTime, 0, TO_DELETE));
        }
        else
        {
            ArrayList<Object> outputs;
            ArrayList<Object> values;
            ArrayList<Object> weights;
            Object o = dbobj.get("output_site");
            Object v = dbobj.get("value");
            Object w = dbobj.get("weight");
            if (count == 1)
            {
                outputs = new ArrayList<>();
                values = new ArrayList<>();
                weights = new ArrayList<>();
                outputs.add(o);
                values.add(v);
                weights.add(w);
            }
            else
            {
                outputs = (ArrayList<Object>)o;
                values = (ArrayList<Object>)v;
                weights = (ArrayList<Object>)w;
            }

            try {
                Iterator<Object> itO = outputs.iterator();
                Iterator<Object> itV = values.iterator();
                Iterator<Object> itW = weights.iterator();

                while(itO.hasNext() && itV.hasNext() && itW.hasNext()) {
                    String id = itO.next().toString();
                    debug3("Output Site ID: " + id);
                    Double value_out = new Double(itV.next().toString());
                    Double weight_out = new Double(itW.next().toString());
                    debug3(comp.getName() + "-" + alg_ver + "Setting output for timeslice: " + debugSdf.format(this._timeSliceBaseTime) +
                            " value: " + value_out + " weight: " + weight_out);
                    TimedVariable tv = new TimedVariable(_timeSliceBaseTime, value_out, TO_WRITE);
                    if (!flags.isEmpty())
                        tv.setFlags(tv.getFlags() | HdbFlags.hdbDerivation2flag(flags));
                    if (!validation_flag.isEmpty())
                        tv.setFlags(tv.getFlags() | HdbFlags.hdbValidation2flag(validation_flag.charAt(1)));
                    if (tsdb.isHdb() && TextUtil.str2boolean(comp.getProperty("OverwriteFlag")))
                    {
                        //waiting on release of overwrite flag upstream
                        //v.setFlags(v.getFlags() | HdbFlags.HDBF_OVERWRITE_FLAG);
                    }
                    outputSeries.get(id).addSample(tv);
                }
            } catch (Exception e) {
                warning(e.toString());
            }
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

        if (outputSeries.isEmpty())
        {
            return;
        }

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
