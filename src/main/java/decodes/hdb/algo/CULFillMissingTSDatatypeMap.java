package decodes.hdb.algo;

import decodes.hdb.dbutils.DBAccess;
import decodes.hdb.dbutils.DataObject;
import decodes.hdb.dbutils.HDBAlgoTSUtils;
import decodes.sql.DbKey;
import decodes.tsdb.*;
import decodes.tsdb.algo.AWAlgoType;
import decodes.util.PropertySpec;
import ilex.var.TimedVariable;
import opendcs.dai.TimeSeriesDAI;

import java.sql.Connection;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Pattern;

//AW:IMPORTS
// Place an import statements you need here.
//AW:IMPORTS_END

//AW:JAVADOC

/**
This algorithm fills missing data for RiverWare DMI usage.
Queries the generic map and fills any missing data with specified value. Uses input TS group for datatypes to limit TSID query

 CUL Input parameter should be monthly data at state huc CP level, but will be run as a timed computation.
 Requires that input Timeseries Group specifies datatype ids! Datatype ids will be joined with input data_source map

Timeseries to fill:
select id.ts_id from
 cp_ts_id id, ref_ext_site_data_map map, hdb_ext_data_source source,
 hdb_site_datatype sd where
 source.ext_data_source_name = 'RiverWare Natural Flow Model to CUL' and
 source.ext_data_source_id = map.ext_data_source_id and
 map.hdb_site_datatype_id = id.site_datatype_id and
 sd.site_datatype_id = map.hdb_site_datatype_id and
 sd.datatype_id in (input ts group datatype ids) and
 id.interval = map.hdb_interval_name;

Dates to be filled:
WITH allDates AS
(
select date_time dt from table(dates_between(DATE '" + fillStartYr + "-01-01',trunc(sysdate, 'month'), 'month'))
)
SELECT dt FROM allDates WHERE dt NOT IN
(
SELECT start_date_time FROM r_base
WHERE interval = 'month'
AND site_datatype_id = 29571
)
ORDER BY dt

 */
//AW:JAVADOC_END
public class CULFillMissingTSDatatypeMap
	extends decodes.tsdb.algo.AW_AlgorithmBase
{
	private static final String INPUT = "input";
	//AW:INPUTS
	public double input; //AW:TYPECODE=i
	String[] _inputNames = { INPUT };
//AW:INPUTS_END

//AW:LOCALVARS
	// Enter any local class variables needed by the algorithm.
	String alg_ver = "1.0";
	String query;
	String status;
	int flag;
	Connection conn = null;
	DBAccess db = null;
	DataObject dbobj;
	TimeSeriesDAI dao = null;
	HashMap<String, CTimeSeries> outputSeries = new HashMap<>();
	private static final Pattern mapPattern = Pattern.compile("^[\\w\\h]+$"); //only alphanumeric+_, and spaces allowed


	PropertySpec[] specs = 
		{
				new PropertySpec("fillStartYr", PropertySpec.INT,
					"(1971) First year to fill data"),
				new PropertySpec("fillValue", PropertySpec.NUMBER,
						"(0.0) value to fill timeseries"),
				new PropertySpec("validation_flag", PropertySpec.STRING,
						"(empty) Always set this validation flag in the output."),
				new PropertySpec("flags", PropertySpec.STRING,
						"(empty) Always set these dataflags in the output."),
				new PropertySpec("extDataMap", PropertySpec.STRING,
                        "(RiverWare Natural Flow Model to CUL) Which ext_data_source name in HDB_EXT_DATA_SOURCE to fill missing values with zeros."),

		};
//AW:LOCALVARS_END

//AW:OUTPUTS
	String[] _outputNames = { "" };
//AW:OUTPUTS_END

//AW:PROPERTIES
	public long fillStartYr = 1971;
	public double fillValue = 0.0;
	public String extDataMap = "RiverWare Natural Flow Model to CUL";
	public String validation_flag = "";
	public String flags;
	String[] _propertyNames = { "flags", "validation_flag", "extDataMap", "fillValue", "fillStartYr" };
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
		if ( !mapPattern.matcher( extDataMap ).matches()) {
			throw new DbCompException("Loading application name not valid: " + extDataMap);
		}

		dbobj = new DataObject();
		dbobj.put("ALG_VERSION",alg_ver);
		conn = tsdb.getConnection();
		db = new DBAccess(conn);
		dao = tsdb.makeTimeSeriesDAO();

		ArrayList<DbKey> dids = comp.getGroup().getDataTypeIdList();
		if (dids.isEmpty())
			throw new DbCompException("cannot fill datatype limited timeseries without datatype IDs in ts group!");
		StringBuilder builder = new StringBuilder(); //would use something like stream().map(DbKey::toString).collect(Collectors.toList())).collectors.joining if Java 8, sigh
		for ( DbKey id: dids) {
			builder.append(id.toString()).append(",");
		}
		builder.deleteCharAt(builder.length() - 1); // cut off the last comma

		//get output TS_IDs
		query = "select id.ts_id ts from " +
				" cp_ts_id id, ref_ext_site_data_map map, hdb_ext_data_source source, " +
				" hdb_site_datatype sd where " +
				" source.ext_data_source_name = '" + extDataMap + "' and " +
				" source.ext_data_source_id = map.ext_data_source_id and " +
				" map.hdb_site_datatype_id = id.site_datatype_id and " +
				" map.is_active_y_n = 'Y' and " +
				" sd.site_datatype_id = map.hdb_site_datatype_id and " +
				" sd.datatype_id in ( " + builder + " ) and " +
				" id.interval = map.hdb_interval_name ";

		debug3("Before TimeSlice Query: " + query);
		status = db.performQuery(query,dbobj);

		if (status.startsWith("ERROR") || !HDBAlgoTSUtils.findOutputSeries(dbobj, dao, outputSeries))
		{
			warning(comp.getName() + "-" + alg_ver + " Aborted: see following error message");
			warning(status);
			throw new DbCompException("Error retrieving timeseries for output, cannot continue");
		}
		// If the query succeeds, but finds 0 output tsid's this isn't necessarily an error
		// Warning already handled in the findOutputSeries function
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
		
		flag = VarFlags.TO_WRITE;

        conn = tsdb.getConnection();
        String status;
        DBAccess db = new DBAccess(conn);

		SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss.S",Locale.ENGLISH);
		formatter.setTimeZone(TimeZone.getTimeZone(aggregateTimeZone));

		//Loop on timeseries ids
		for (String tsid: outputSeries.keySet() )
		{
			CTimeSeries out = outputSeries.get(tsid);

			DataObject dbobj = new DataObject();

			query = "WITH allDates AS " +
				"( " +
				"select date_time dt from table(dates_between(DATE '" + fillStartYr + "-01-01',trunc(sysdate, 'month'), 'month')) " +
				") " +
				"SELECT dt FROM allDates WHERE dt NOT IN " +
				"( " +
				"SELECT start_date_time FROM r_" + out.getInterval() + " " +
				"where site_datatype_id = " + out.getSDI() +
				") " +
				"ORDER BY dt ";
        
	        status = db.performQuery(query,dbobj);
    	    debug3(" SQL STRING:" + query + "   DBOBJ: " + dbobj + "STATUS:  " + status);
        
        	if (status.startsWith("ERROR"))
			{
				throw new DbCompException("Failed to query missing data timestamps! " + status);
        	}
			int count = ((Integer) dbobj.get("rowCount"));
			if (count == 0)
				continue;

	        // Occasionally a TS will have exactly one month to fill, which can't be directly cast to arrayList. Handle separately
    	    ArrayList<Object> monthsToFill;
        	if ( count == 1)
        	{
	        	monthsToFill = new ArrayList<>(1);
    	    	monthsToFill.add(dbobj.get("dt"));
        	} else
        	{
        		monthsToFill = (ArrayList<Object>) dbobj.get("dt");
        	}
        	for(Iterator<Object> iter = monthsToFill.iterator(); iter.hasNext();)
			{
				try {
					Date dt = formatter.parse((String) iter.next());
					out.addSample(new TimedVariable(dt, fillValue, flag));
				} catch (ParseException e) {
					// improve error handling
					warning(comp.getName() + "Error setting output: " + e);
				}
			}
			try {
				out.setComputationId(comp.getId());
				dao.saveTimeSeries(out);
			}
			catch (DbIoException | BadTimeSeriesException e) {
				throw new DbCompException(e.toString());
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

	public DbComputation getComp() {
		return comp;
	}

	public TimeSeriesDAI getDao() {
		return dao;
	}

	public HashMap<String, CTimeSeries> getOutputSeries() {
		return outputSeries;
	}
}
