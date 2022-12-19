package decodes.hdb.algo;

import decodes.hdb.dbutils.DBAccess;
import decodes.hdb.dbutils.DataObject;
import decodes.tsdb.*;
import decodes.tsdb.algo.AWAlgoType;
import decodes.util.PropertySpec;
import ilex.var.NamedVariable;
import ilex.var.NoConversionException;

import ilex.var.TimedVariable;
import opendcs.dai.TimeSeriesDAI;

import java.util.*;
//AW:IMPORTS_END

//AW:JAVADOC

/**
 Disaggregates from total with coefficient timeseries
 May not obey parameter offset properties correctly

 */
//AW:JAVADOC_END
public class CULTemporalPercentDisagg
    extends decodes.tsdb.algo.AW_AlgorithmBase
{
    private final String TOTAL = "total";
    private final String COEFF = "coeff";
    private final String OUTPUT = "output";
    //AW:INPUTS
    public double total;    //AW:TYPECODE=i
    public double coeff;    //AW:TYPECODE=i

    String[] _inputNames = {TOTAL, COEFF};
//AW:INPUTS_END

//AW:LOCALVARS
    private ParmRef coeffRef;
    private ParmRef inRef;
    private IntervalIncrement inIntvI;
    private IntervalIncrement coeffIntvI;


    PropertySpec[] specs =
        {
        };

//AW:LOCALVARS_END

    //AW:OUTPUTS
    public NamedVariable output = new NamedVariable(OUTPUT, 0);
    String[] _outputNames = { OUTPUT };
//AW:OUTPUTS_END

//AW:PROPERTIES
    String[] _propertyNames = {};
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
        inRef = getParmRef(TOTAL);
        if (inRef == null)
            throw new DbCompException("Cannot determine period of "+ TOTAL);

        String inIntv = inRef.compParm.getInterval();
        inIntvI = IntervalCodes.getIntervalCalIncr(inIntv);
        if (inIntvI == null || inIntvI.getCount() == 0)
            throw new DbCompException(
                    "Cannot DisAggregate from " + TOTAL + " with instantaneous interval.");

        // Get interval of coefficient
        coeffRef = getParmRef(COEFF);
        if (coeffRef == null)
            throw new DbCompException("Cannot determine period of " + COEFF);
        String coeffIntv = coeffRef.compParm.getInterval();
        coeffIntvI = IntervalCodes.getIntervalCalIncr(coeffIntv);
        if (coeffIntvI == null || coeffIntvI.getCount() == 0)
            throw new DbCompException(
                    "Cannot Disaggregate " + COEFF + " with instantaneous interval.");

        TimeSeriesDAI dao = tsdb.makeTimeSeriesDAO();
        //fill coefficient timeseries with all values, not just values at total timeseries times
        try {
            aggCal.setTime(baseTimes.last());
            aggCal.add(inIntvI.getCalConstant(), inIntvI.getCount()); //add interval to get to end of last input period

            dao.fillTimeSeries(coeffRef.timeSeries, baseTimes.first(), aggCal.getTime());
        }
        catch (Exception e)
        {
            throw new DbCompException("Error filling coefficient timeseries: " + e);
        }
        dao.close();
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
        boolean do_setoutput = true;

        if(isTrigger(TOTAL))
        {  //we're triggered by a total, so disagg for all component output intervals

            Date startT = new Date(_timeSliceBaseTime.getTime());
            aggCal.setTime(startT);
            aggCal.add(inIntvI.getCalConstant(), inIntvI.getCount());
            Date endT = aggCal.getTime();
            aggCal.setTime(startT);

            for(Date t = startT; t.before(endT);
                aggCal.add(coeffIntvI.getCalConstant(), coeffIntvI.getCount()),
                        t = aggCal.getTime())
            {
                try
                {
                    double out = coeffRef.timeSeries.findWithin(t, roundSec).getDoubleValue();
                    out = total * out;
                    debug3(comp.getAlgorithmName()+"-"+comp.getName()+" setting output: " +
                            t + " SDI: " + getSDI(OUTPUT) + " value: " + out);
                    setOutput(output, out, t);
                }
                catch(NoConversionException e) {// delete output, computation failed
                    debug3(comp.getAlgorithmName()+" Failed to find coefficient exception:" + e + " At: " +
                            t + " SDI: " + getSDI(OUTPUT) + " Deleting old value");
                    deleteOutput(output, t);
                }
            }
        }
        else
        { // total is not a trigger at this timeslice, so we're triggered by a coefficient
            if (isMissing(total)) {
                TimedVariable tv = inRef.timeSeries.findPrev(_timeSliceBaseTime);
                aggCal.setTime(_timeSliceBaseTime);
                aggCal.add(inIntvI.getCalConstant(), -inIntvI.getCount());
                debug3(comp.getAlgorithmName() + " Finding missing total for: " +
                        _timeSliceBaseTime + " SDI: " + getSDI(TOTAL) + " lookback limit: " + aggCal.getTime() + " result: " + tv);
                if ( tv == null || tv.getTime().before(aggCal.getTime()))
                { // no value or value outside of an interval before current timeslice!
                    do_setoutput = false;
                }
                else try
                {
                        total = tv.getDoubleValue();
                } catch (NoConversionException e) {
                    do_setoutput = false;
                }
                if (!do_setoutput) {
                    debug3(comp.getAlgorithmName() + " Failed to find total At: " +
                            _timeSliceBaseTime + " SDI: " + getSDI(OUTPUT) + " Deleting old value");
                    deleteOutput(output);
                    return;
                }
            }
            double out = total * coeff;
            debug3(comp.getAlgorithmName()+"-"+comp.getName()+" setting output: " +
                    _timeSliceBaseTime + " SDI: " + getSDI(OUTPUT) + " total: " + total + " coeff: " + coeff + " out: " + out);
            setOutput(output, out);
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
