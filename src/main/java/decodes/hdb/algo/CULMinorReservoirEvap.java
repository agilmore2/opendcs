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
import java.lang.Math;

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
Calculates annual evaporation for minor reservoirs
Time series input is just annual precip (P) at each reservoir
Algorithm extracts the site id and grabs 4 static metadata coefficients from ref_site_coef:
    maximum surface area (SA)
    salvage (S)
    average free water surface evap rate (ER)
    fullness factor (F)
    
evap is calculated as:
    (ER - max(P,S)) * (F*SA) / 12
    
Each metadata coefficient has an effective start date/time, to avoid computing evap before the reservoir existed
The start date/times should be the same on each coefficient
The reservoir's start year will be calculated as the maximum year from all the coefficients (should be the same)
No evap will be written before the start year

 */
//AW:JAVADOC_END
public class CULMinorReservoirEvap
    extends decodes.tsdb.algo.AW_AlgorithmBase
{
//AW:INPUTS
    public double ResPrecip;    //AW:TYPECODE=i
    String _inputNames[] = { "ResPrecip" };
//AW:INPUTS_END

//AW:LOCALVARS
    // Enter any local class variables needed by the algorithm.
    
    String query;
    String status;

    Connection conn = null;
    DBAccess db = null;
    DataObject dbobj = new DataObject();

    double salvage;
    double maxSurfaceArea;
    double fullnessFactor;
    double evapRate;
    int resStartYr = 1900;
    int resEndYr;
    
//AW:LOCALVARS_END

//AW:OUTPUTS
    public NamedVariable evap = new NamedVariable("evap", 0);
    String _outputNames[] = { "evap" };
//AW:OUTPUTS_END

//AW:PROPERTIES
    String _propertyNames[] = {};
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
        
        maxSurfaceArea = getAttrValue("maximum surface area");
        evapRate = getAttrValue("average free water surface evap rate");
        fullnessFactor = getAttrValue("fullness factor");
        salvage = getAttrValue("salvage");
        
        // collect the necessary static metadata for the reservoir
        

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
                    _timeSliceBaseTime + " SDI: " + getSDI("ResPrecip"));

           // beforeTimeslice ensures that comp metadata is correct
           if (_timeSliceBaseTime.getYear() + 1900 < resStartYr)
           {
               warning(comp.getName() + " - " + " Skipping timestep before reservoir start year for site " + getSiteName("ResPrecip","hdb"));
               return;
           }
           else if (resEndYr != 0 && _timeSliceBaseTime.getYear() + 1900 > resEndYr)
           {
               warning(comp.getName() + " - " + " Skipping timestep after reservoir end year for site " + getSiteName("ResPrecip","hdb"));
               return;               
           }
           
           // evap calculation
           double e = (evapRate - Math.max(ResPrecip,salvage)) * (fullnessFactor * maxSurfaceArea) / 12;
           setOutput(evap,e); 

//AW:TIMESLICE_END
    }

    /**
     * This method is called once after iterating all time slices.
     */
    protected void afterTimeSlices()
        throws DbCompException
    {
//AW:AFTER_TIMESLICES
        resStartYr = 1900;
        resEndYr = Integer.MAX_VALUE;
//AW:AFTER_TIMESLICES_END
    }
    
    /** 
     * Make a metadata query to the ref_site_coef table to get a single value for each reservoir
     * Takes an attribute name as input
     */
    private double getAttrValue(String attrName)
            throws DbCompException
    {
        query = "SELECT rsc.coef, EXTRACT(year FROM rsc.effective_start_date_time) syr, NVL(EXTRACT(year FROM rsc.effective_end_date_time),0) eyr FROM\r\n"
                + "ref_site_coef rsc INNER JOIN hdb_attr a\r\n"
                + "ON rsc.attr_id = a.attr_id\r\n"
                + "WHERE \r\n"
                + "rsc.site_id = " + getSiteName("ResPrecip","hdb") + " AND\r\n"
                + "a.attr_name = '" + attrName + "'";
        
        status = db.performQuery(query,dbobj);
        if (status.startsWith("ERROR") || Integer.parseInt(dbobj.get("rowCount").toString()) != 1)
        {
            throw new DbCompException(comp.getName() + " Problem with reservoir metadata for site " + getSiteName("ResPrecip","hdb"));
        }
        else
        {
            // each coefficient for the minor reservoir has an effective start date time (they should all be the same)
            // update the reservoir's start year to be the maximum start year from all the coeffs
            resStartYr = Math.max(resStartYr,Integer.parseInt(dbobj.get("syr").toString()));
            
            // some reservoirs will have effective end date times (they should all be the same, or null). Null will be represented by a 0
            // update the reservoir's end year to be the maximum end year from all the coeffs (0 if null)
            resEndYr = Math.max(resEndYr, Integer.parseInt(dbobj.get("eyr").toString()));
            
            return Double.parseDouble(dbobj.get("coef").toString());            
        }
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
    
}
