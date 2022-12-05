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
Calculates annual evaporation for major reservoirs
Time series inputs
	monthly precip (P)
	monthly surface area at each reservoir (SA)

Algorithm extracts the site id and grabs static metadata coefficients from ref_site_coef:
	salvage (S)(Annual)
	average free water surface evap rate (ER)(Annual)
	monthly evap disagg coefficients (MC)(12 coefficients that apply to all sites)
	
Algorithm also has to extract annual precip for the current calendar year
	annual precip (AP)	

	
For each month, evap is calculated as:
See example workbook for the source of this calculation: COEvap2016-2020.xlsx
	evap = ((ER * MC) - max(AP,S) * P/AP) * SA/12

 */
//AW:JAVADOC_END
public class CULMajorReservoirEvap
	extends decodes.tsdb.algo.AW_AlgorithmBase
{
//AW:INPUTS
	public double ResPrecip;	//AW:TYPECODE=i
	public double SurfaceArea;	//AW:TYPECODE=i
	String _inputNames[] = { "ResPrecip","SurfaceArea"};
//AW:INPUTS_END

//AW:LOCALVARS
	// Enter any local class variables needed by the algorithm.
	
    String query;
    String status;

    Connection conn = null;
    DBAccess db = null;
    DataObject dbobj = new DataObject();

    // single metadata coefficients from ref_site_coef
    double annualSalvage;
    double annualEvapRate;
    
    // monthly array of disagg coefficients
    ArrayList<Object> monthlyCoefs;
    
    // values to be calculated by query
    double annualPrecip;
    
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
        
      
        // collect the necessary static metadata for the reservoir
        annualEvapRate = getAttrValue("average free water surface evap rate");
        annualSalvage = getAttrValue("salvage");
        
        // get monthly coefficients to disaggregate annual evap rate
        // Sorted january to december
        query = "SELECT rsc.coef FROM\r\n"
        		+ "hdb_attr a INNER JOIN ref_site_coef rsc\r\n"
        		+ "ON a.attr_id = rsc.attr_id\r\n"
        		+ "INNER JOIN hdb_site s\r\n"
        		+ "ON s.site_id = rsc.site_id\r\n"
        		+ "WHERE s.site_name = 'UPPER COLORADO RIVER BASIN' AND\r\n"
        		+ "a.attr_name = 'temporal disaggregation, annual to monthly, reservoir (other)'\r\n"
        		+ "ORDER BY rsc.coef_idx ASC";
        
        status = db.performQuery(query,dbobj);
        if (status.startsWith("ERROR") || Integer.parseInt(dbobj.get("rowCount").toString()) != 12)
        {
        	throw new DbCompException(comp.getName() + " Problem with reservoir metadata for site " + getSiteName("ResPrecip","hdb"));
        }
        else
        {
        	monthlyCoefs = (ArrayList<Object>) dbobj.get("coef");
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
	                _timeSliceBaseTime + " SDI: " + getSDI("ResPrecip"));

		   // beforeTimeslice checks metadata is correct
	       // extract month number from time slice base time (1-12) and year
	       int mon = _timeSliceBaseTime.getMonth(); // 0 - 11
	       int yr = _timeSliceBaseTime.getYear() + 1900; // getYear returns yr - 1900    
	       
	       // query for annual precip in the current calendar year
	       query = "SELECT EXTRACT(YEAR FROM start_date_time) yr, SUM(value) sum,COUNT(*) FROM r_month\r\n"
	       		+ "WHERE site_datatype_id = " + getSDI("ResPrecip") + "\r\n"
	       		+ "GROUP BY EXTRACT(YEAR FROM start_date_time)\r\n"
	       		+ "HAVING EXTRACT(YEAR FROM start_date_time) = " + yr + " AND\r\n"
	       		+ "COUNT(*) = 12";
	       
	        status = db.performQuery(query,dbobj);
	        if (status.startsWith("ERROR") || Integer.parseInt(dbobj.get("rowCount").toString()) != 1)
	        {
	        	warning(comp.getName() + " Problem retrieving annual precip for site " + getSiteName("ResPrecip","hdb"));
	        	warning(status);
	        	return;
	        }
	        else
	        {
	        	annualPrecip  = Double.parseDouble(dbobj.get("sum").toString());
	        }
	       
	       
	        
	       // evap calculation
	       // see main description comment for explanation
	       double e = ((annualEvapRate * Double.parseDouble(monthlyCoefs.get(mon).toString())) -
	    		   (Math.max(annualPrecip, annualSalvage) * ResPrecip / annualPrecip)) * SurfaceArea / 12;
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

//AW:AFTER_TIMESLICES_END
	}
	
	/** 
	 * Make a metadata query to the ref_site_coef table to get a single value for each reservoir
	 * Takes an attribute name as input
	 */
	private double getAttrValue(String attrName)
			throws DbCompException
	{
		query = "SELECT rsc.coef FROM\r\n"
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
