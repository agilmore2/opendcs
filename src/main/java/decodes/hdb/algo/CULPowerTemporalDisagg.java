package decodes.hdb.algo;

import java.sql.Connection;
import java.util.Date;

import ilex.var.NamedVariableList;
import ilex.var.NamedVariable;
import decodes.tsdb.DbAlgorithmExecutive;
import decodes.tsdb.DbCompException;
import decodes.tsdb.DbIoException;
import decodes.tsdb.VarFlags;
import decodes.tsdb.algo.AWAlgoType;
import decodes.tsdb.CTimeSeries;
import decodes.tsdb.ParmRef;
import ilex.var.TimedVariable;
import decodes.tsdb.TimeSeriesIdentifier;
import decodes.tsdb.IntervalIncrement;

//AW:IMPORTS
import java.util.ArrayList;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import decodes.hdb.dbutils.DBAccess;
import decodes.hdb.dbutils.DataObject;
import decodes.sql.DbKey;
import decodes.tsdb.IntervalCodes;
import decodes.tsdb.ParmRef;
import ilex.var.TimedVariable;
import ilex.var.NoConversionException;
//AW:IMPORTS_END

//AW:JAVADOC
/**
Type a javadoc-style comment describing the algorithm class.


 */
//AW:JAVADOC_END
public class CULPowerTemporalDisagg
	extends decodes.tsdb.algo.AW_AlgorithmBase
{
//AW:INPUTS
	public double AnnualInput;	//AW:TYPECODE=i
	String _inputNames[] = { "AnnualInput" };
//AW:INPUTS_END

//AW:LOCALVARS
	ParmRef Annual_iref = null;
	String Annual_iintv = null;
	IntervalIncrement Annual_iintvii = null;
	
	ParmRef oref = null;
	String ointv = null;
	IntervalIncrement ointvii = null;
	
	DbKey compID = null;
	DbKey SDI = null;
	
	Connection conn = null;

//AW:LOCALVARS_END

//AW:OUTPUTS
	public NamedVariable MonthlyOutput = new NamedVariable("MonthlyOutput", 0);
	String _outputNames[] = { "MonthlyOutput" };
//AW:OUTPUTS_END

//AW:PROPERTIES
	String _propertyNames[] = {  };
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
		Annual_iref = getParmRef("AnnualInput");
		Annual_iintv = Annual_iref.compParm.getInterval();
		Annual_iintvii = IntervalCodes.getIntervalCalIncr(Annual_iintv);
		
		oref = getParmRef("MonthlyOutput");
		ointv = oref.compParm.getInterval();
		ointvii = IntervalCodes.getIntervalCalIncr(ointv);
		
		compID = comp.getId();
		SDI = Annual_iref.timeSeries.getSDI();
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
		
		ResultSet MonthlySourceData = null;
		Statement stmt = null;
		double MonthSourceSum = 0;
		ArrayList<Integer> MonthsWithSource = new ArrayList<Integer>();
		
		// Iteration goes from time of THIS input up to (but not including)
		// time of NEXT input.
		Date startT = new Date(_timeSliceBaseTime.getTime());
		aggCal.setTime(startT);
		aggCal.add(Annual_iintvii.getCalConstant(), Annual_iintvii.getCount());
		Date endT = aggCal.getTime();
		aggCal.setTime(startT);
		
		// Is this line okay with the new connection pooling?
		conn = tsdb.getConnection();

		// Get monthly source data
		
//		System.out.println("--------- NEW TIMESLICE ---------");
//		System.out.println(startT);
		
		// Select monthly data with the same sdi within the current year, that doesn't come from this computation
		String q = "SELECT * FROM r_base WHERE"
				+ " site_datatype_id = " + SDI.getValue()
				+ " AND interval = 'month'"
				+ " AND computation_id != " + compID.getValue()
				+ " AND start_date_time >= '" + new java.sql.Date(startT.getTime()) + "'"
				+ " AND end_date_time <= '" + new java.sql.Date(endT.getTime()) + "'";
		
		try {
			stmt = conn.createStatement();
			MonthlySourceData = stmt.executeQuery(q);
			
			while(MonthlySourceData.next())
			{
				MonthSourceSum += MonthlySourceData.getDouble("value");
				MonthsWithSource.add(MonthlySourceData.getDate("start_date_time").getMonth());
			}
		} catch (SQLException e) {
			// improve this
			System.out.println("query didn't work");
		}

		if(MonthsWithSource.size() < 12)
		{
			for(Date t = startT; t.before(endT);
					aggCal.add(ointvii.getCalConstant(), ointvii.getCount()),
					t = aggCal.getTime())
				{
					// Would like to improve this. right now monthswithsource contains integer month numbers, not full dates
					// Struggled with the difference between java.util.date used by opendcs and java.sql.date returned by the query
					if(!MonthsWithSource.contains(t.getMonth()))
					{
						// set monthly output for months that don't already have monthly source data
						// placeholder function until we figure out where to store/how to calc distributions
						setOutput(MonthlyOutput,(AnnualInput - MonthSourceSum) / (12 - MonthsWithSource.size()),t);
					}
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
		setOutputUnitsAbbr("output", getInputUnitsAbbr("input"));
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
}
