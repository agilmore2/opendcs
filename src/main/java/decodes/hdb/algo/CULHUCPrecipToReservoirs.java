package decodes.hdb.algo;

import static decodes.tsdb.VarFlags.TO_WRITE;

import java.sql.Connection;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.TimeZone;

import decodes.hdb.dbutils.HDBAlgoTSUtils;
import ilex.var.NamedVariableList;
import ilex.util.TextUtil;
import ilex.var.NamedVariable;
import decodes.sql.DbKey;
import decodes.tsdb.DbAlgorithmExecutive;
import decodes.tsdb.DbCompException;
import decodes.tsdb.DbIoException;
import decodes.tsdb.VarFlags;
import decodes.tsdb.algo.AWAlgoType;
import decodes.util.DecodesSettings;
import decodes.util.PropertySpec;
import decodes.hdb.HdbFlags;
import decodes.hdb.dbutils.DBAccess;
import decodes.hdb.dbutils.DataObject;
import decodes.tsdb.CTimeSeries;
import decodes.tsdb.ParmRef;
import ilex.var.TimedVariable;
import opendcs.dai.TimeSeriesDAI;
import decodes.tsdb.TimeSeriesIdentifier;

//AW:IMPORTS
// Place an import statements you need here.
//AW:IMPORTS_END

//AW:JAVADOC
/**
Copies HUC precip to all reservoirs within that HUC, which is later used to calculate evaporation at each reservoir

Query to find output tsids:
SELECT 
id.ts_id ts,outputs.site_id output_site
FROM hdb_site sources,hdb_site_datatype sourcesd,
hdb_site_datatype outputsdis,hdb_site outputs, cp_ts_id id 
WHERE 
sourcesd.site_datatype_id = 31248 AND  -- HUC 14010001 precip sdi
sourcesd.site_id = sources.site_id AND
outputsdis.site_id = outputs.site_id AND
outputs.objecttype_id = 
(SELECT objecttype_id FROM hdb_objecttype WHERE objecttype_name = 'cul site - major reservoir') AND
outputs.hydrologic_unit = sources.site_name AND -- output reservoirs have the same hydrologic unit as the source sites name
outputsdis.datatype_id = sourcesd.datatype_id AND
id.site_datatype_id = outputsdis.site_datatype_id AND
id.interval = 'month' AND
id.table_selector = 'R_'



 */
//AW:JAVADOC_END
public class CULHUCPrecipToReservoirs
    extends decodes.tsdb.algo.AW_AlgorithmBase
{
//AW:INPUTS
    public double HUCPrecip;    //AW:TYPECODE=i
    String[] _inputNames = { "HUCPrecip" };
//AW:INPUTS_END

//AW:LOCALVARS
    // Enter any local class variables needed by the algorithm.
    
    String query;
    String status;

    Connection conn = null;
    DBAccess db = null;
    DataObject dbobj = new DataObject();
    TimeSeriesDAI dao = null;
    HashMap<String,CTimeSeries> outputSeries = new HashMap<>();
    
//AW:LOCALVARS_END

//AW:OUTPUTS
    String[] _outputNames = {};
//AW:OUTPUTS_END

//AW:PROPERTIES
    PropertySpec[] specs =
        {
                new PropertySpec("sector", PropertySpec.STRING,
                        "('minor' or 'major') Specify the type of reservoir evap computation"),
        };
    public String sector = "";
    String[] _propertyNames = { "sector" };
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
        conn = tsdb.getConnection();
        db = new DBAccess(conn);
        dao = tsdb.makeTimeSeriesDAO();
        outputSeries.clear();
        
        // Get output reservoir precip TSID's (all reservoirs within the triggering HUC)
        
        query = "SELECT   "
                + "id.ts_id ts, outputs.site_id output_site   "
                + "FROM hdb_site sources,hdb_site_datatype sourcesd,  "
                + "hdb_site_datatype outputsdis,hdb_site outputs, cp_ts_id id   "
                + "WHERE   "
                + "sourcesd.site_datatype_id = " + getSDI("HUCPrecip") + " AND    "
                + "sourcesd.site_id = sources.site_id AND  "
                + "outputsdis.site_id = outputs.site_id AND  "
                + "outputs.objecttype_id =   "
                + "(SELECT objecttype_id FROM hdb_objecttype WHERE objecttype_name = 'cul site - " + sector + " reservoir') AND  "
                + "outputs.hydrologic_unit = sources.site_name AND   " //TODO: join hucs and use HUC objecttype?
                + "outputsdis.datatype_id = sourcesd.datatype_id AND  "
                + "id.site_datatype_id = outputsdis.site_datatype_id AND  "
                + "id.interval = '" + getInterval("HUCPrecip") + "' AND  "
                + "id.table_selector = '" + getTableSelector("HUCPrecip") + "'";
        
        status = db.performQuery(query,dbobj);
        if (status.startsWith("ERROR") || !HDBAlgoTSUtils.findOutputSeries(dbobj, dao, outputSeries))
        {
            warning(comp.getName() + " Aborted: see following error message");
            throw new DbCompException("Unable to find output timeseries! " + status);
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

           debug3(comp.getName() + " - " + " BEGINNING OF doAWTimeSlice for period: " +
                    _timeSliceBaseTime + " SDI: " + getSDI("HUCPrecip"));

            SimpleDateFormat sdf = new SimpleDateFormat("dd-MM-yyyy HH:mm");
            sdf.setTimeZone(TimeZone.getTimeZone(DecodesSettings.instance().aggregateTimeZone));

            ArrayList<Object> outputs;
            Object o = dbobj.get("ts");
            int count = Integer.parseInt(dbobj.get("rowCount").toString());
            
            if (count == 0) {
                return;
            }
            else if (count == 1)
            {
                outputs = new ArrayList<>();
                outputs.add(o);
            }
            else
            {
                outputs = (ArrayList<Object>)o;
            }

            try {
                // iterate through site id's, add HUCPrecip input value to each output time series
                for (Object output : outputs) {
                    String id = output.toString();
                    CTimeSeries out = outputSeries.get(id);
                    debug3("Output Site ID: " + out.getTimeSeriesIdentifier().getSite().getId() + " comp: " + comp.getName() +
                            " Setting output for timeslice: " + debugSdf.format(this._timeSliceBaseTime) +
                            " value: " + HUCPrecip);
                    TimedVariable tv = new TimedVariable(_timeSliceBaseTime, HUCPrecip, TO_WRITE);
                    out.addSample(tv);
                }
            } catch (Exception e) {
                warning(e.toString());
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
        for (CTimeSeries v : outputSeries.values()) {
            TimeSeriesIdentifier id = v.getTimeSeriesIdentifier();
            try {
                debug1(comp.getName() + " saving site: " + id.getSiteName() + " timeseries: " + id + " with size: " + v.size());
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
