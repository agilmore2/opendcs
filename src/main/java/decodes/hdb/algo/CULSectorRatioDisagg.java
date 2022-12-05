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
import java.util.ArrayList;
import java.util.GregorianCalendar;
import java.util.Iterator;
import java.util.TimeZone;

//AW:IMPORTS
//AW:IMPORTS_END

//AW:JAVADOC

/**
Disaggregates data available for a combination of two sectors into the component sectors
Relies on a ratio computed by a separate algorithm
Example: used to compute livestock and stockpond CU by disaggregating the combined source data


 */
//AW:JAVADOC_END
public class CULSectorRatioDisagg
	extends decodes.tsdb.algo.AW_AlgorithmBase
{
//AW:INPUTS
	public double totalInput;	//AW:TYPECODE=i
	public double coefficient;
	String[] _inputNames = { "totalInput" , "coefficient"};
//AW:INPUTS_END

//AW:LOCALVARS
	Connection conn = null;
	
    PropertySpec[] specs =
        {
				new PropertySpec("validation_flag", PropertySpec.STRING,
						"(empty) Always set this validation flag in the output."),
				new PropertySpec("flags", PropertySpec.STRING,
						"(empty) Always set these dataflags in the output."),
				new PropertySpec("estimation_process", PropertySpec.STRING,
						"(CU_Agg_Disagg) Which loading application produces estimates that should be ignored along with CU_FillMissing."),
				new PropertySpec("coeff_year", PropertySpec.INT,
                        "(1985) What year to retrieve coefficients from"),
        };

//AW:LOCALVARS_END

//AW:OUTPUTS
	public NamedVariable sector1 = new NamedVariable("sector1", 0);
	public NamedVariable sector2 = new NamedVariable("sector2", 0);
	String[] _outputNames = { "sector1", "sector2" };
//AW:OUTPUTS_END

//AW:PROPERTIES
	public String estimation_process = "CU_Agg_Disagg";
	public String validation_flag = "";
	public String flags;
	public long coeff_year = 1985;
    String[] _propertyNames = { "estimation_process", "validation_flag", "coeff_year", "flags" };
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
		ParmRef totalRef = getParmRef("totalInput");
		ParmRef coeffRef = getParmRef("coefficient");

		DbKey compID = comp.getId();
		DbKey inputSDI = totalRef.timeSeries.getSDI();
		DbKey coeffSDI = coeffRef.timeSeries.getSDI();

		// Query related variables
	    String status;
	    String query;
	    DataObject dbobj = new DataObject();
		conn = tsdb.getConnection();
		DBAccess db = new DBAccess(conn);

		// Get coefficient
		query = "SELECT value coeff FROM r_year"
				+ " WHERE site_datatype_id = " + coeffSDI.getValue()
				+ " AND extract(year from start_date_time) = " + coeff_year;

		status = this.doQuery(query, dbobj, db);
		if (status.startsWith("ERROR") || (Integer) dbobj.get("rowCount") != 1)
		{
			throw new DbCompException (comp.getName() + "Something wrong with coefficient query: " + query + status );
		}

		double coeff = Double.parseDouble((String) dbobj.get("coeff"));

		// Get years to disagg (years with an annual source data point)
        // May need to get more thorough for this in other sectors
        query = "SELECT EXTRACT(YEAR FROM ry.start_date_time) year, ry.value FROM r_year ry, r_base rb"
				+ " WHERE ry.site_datatype_id = " + inputSDI.getValue()
				+ " AND ry.site_datatype_id = rb.site_datatype_id"
				+ " AND ry.start_date_time = rb.start_date_time"
				+ " AND rb.interval = 'year'"
				+ " AND loading_application_id not in "
				+ "  (select loading_application_id from hdb_loading_application where"
				+ "  loading_application_name in ('CU_FillMissing','"+ estimation_process + "'))"
        		+ " ORDER BY year ASC";
        
        status = this.doQuery(query, dbobj, db);
		if (status.startsWith("ERROR") || (Integer) dbobj.get("rowCount") < 2)
		{
			throw new DbCompException (comp.getName() + ": Value query failed or returned only one row: " + query + status );
		}

        ArrayList<Object> YearstoDisagg = (ArrayList<Object>) dbobj.get("year");
        ArrayList<Object> AnnualSourceData = (ArrayList<Object>) dbobj.get("value");

        // Loop through years to disagg
        TimeZone tz = TimeZone.getTimeZone("MST");
        GregorianCalendar cal = new GregorianCalendar(tz);
        Iterator<Object> itYr = YearstoDisagg.iterator();
        Iterator<Object> itAnnualSource = AnnualSourceData.iterator();

		if (flags != null)
		{
			setHdbDerivationFlag(sector1, flags);
			setHdbDerivationFlag(sector2, flags);
		}
		if (validation_flag.length() > 0) {
			setHdbValidationFlag(sector2, validation_flag.charAt(1));
		}

		while(itYr.hasNext() && itAnnualSource.hasNext()) {
        	int yr = Integer.parseInt(itYr.next().toString());
        	double annualSourceVal = Double.parseDouble(itAnnualSource.next().toString());
			cal.set(yr, 0,1,0,0);

			debug3("Setting output for " + debugSdf.format(cal.getTime()));
        	setOutput(sector1, annualSourceVal * coeff, cal.getTime());
        	setOutput(sector2, annualSourceVal * (1 - coeff), cal.getTime());
        }
//AW:AFTER_TIMESLICES_END
	}

	private String doQuery(String q, DataObject dbobj, DBAccess db)
	{
		String status = db.performQuery(q, dbobj);
		if(status.startsWith("ERROR"))
		{
			System.out.println("Query didn't work"); // improve error handling
		}
		return status;
		
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
