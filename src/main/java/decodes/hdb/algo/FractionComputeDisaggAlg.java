package decodes.hdb.algo;

import decodes.tsdb.*;
import decodes.tsdb.algo.AWAlgoType;
import decodes.util.PropertySpec;
import ilex.var.NamedVariable;
import ilex.var.NoConversionException;
import opendcs.dai.TimeSeriesDAI;

import java.util.Date;

//AW:IMPORTS
// Place an import statements you need here.
//AW:IMPORTS_END

//AW:JAVADOC

/**
 Computes fraction of total from total and same SDI at interval of output
 Output(t) = component(t)/total(t1 through t12) where output and total are parameters, and component timeseries is queried

 */
//AW:JAVADOC_END
public class FractionComputeDisaggAlg
        extends decodes.tsdb.algo.AW_AlgorithmBase
{
    public static final String TOTAL = "total";
    public static final String OUTPUT = "output";
    //AW:INPUTS
    public double total;    //AW:TYPECODE=i
    String[] _inputNames = { TOTAL };
//AW:INPUTS_END

    //AW:LOCALVARS
    // Enter any local class variables needed by the algorithm.
    String alg_ver = "1.0";
    PropertySpec[] specs =
            {};
    CTimeSeries component = null;
    private TimeSeriesDAI dao;
    private IntervalIncrement inIntvI;
    private IntervalIncrement outIntvI;

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
        // Get interval of input
        ParmRef inRef = getParmRef(TOTAL);
        if (inRef == null)
            throw new DbCompException("Cannot determine period of "+ TOTAL);

        String inIntv = inRef.compParm.getInterval();
        inIntvI = IntervalCodes.getIntervalCalIncr(inIntv);
        if (inIntvI == null || inIntvI.getCount() == 0)
            throw new DbCompException(
                    "Cannot DisAggregate from 'input' with instantaneous interval.");

        // Get interval of output
        ParmRef outRef = getParmRef(OUTPUT);
        if (outRef == null)
            throw new DbCompException("Cannot determine period of " + OUTPUT);
        String outIntv = outRef.compParm.getInterval();
        outIntvI = IntervalCodes.getIntervalCalIncr(outIntv);
        if (outIntvI == null || outIntvI.getCount() == 0)
            throw new DbCompException(
                    "Cannot Disaggregate to " + OUTPUT + " with instantaneous interval.");

        dao = tsdb.makeTimeSeriesDAO();

        //get TSID for component values, same SDI as total at interval of output
        component = new CTimeSeries(getSDI(TOTAL), getInterval(OUTPUT), getTableSelector(OUTPUT));
        try {
            aggCal.setTime(baseTimes.last());
            aggCal.add(inIntvI.getCalConstant(), inIntvI.getCount()); //add interval to get to end of last input period

            debug3(comp.getAlgorithmName()+"-"+alg_ver+" filling timeseries for SDI: " + component.getSDI() +
                    " at interval: " + component.getInterval() + " from: " + baseTimes.first() + " to: " + aggCal.getTime());
            dao.fillTimeSeries(component, baseTimes.first(), aggCal.getTime());
        }
        catch (Exception e)
        {
            throw new DbCompException("Error filling component timeseries: " + e);
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
        // Iteration goes from time of THIS input up to (but not including)
        // time of NEXT input.
        Date startT = new Date(_timeSliceBaseTime.getTime());
        aggCal.setTime(startT);
        aggCal.add(inIntvI.getCalConstant(), inIntvI.getCount());
        Date endT = aggCal.getTime();
        aggCal.setTime(startT);

        for(Date t = startT; t.before(endT);
            aggCal.add(outIntvI.getCalConstant(), outIntvI.getCount()),
                    t = aggCal.getTime())
        {
            try
            {
                double out = component.findWithin(t, roundSec).getDoubleValue();
                out = out/total;
                debug3(comp.getAlgorithmName()+"-"+alg_ver+" setting output: " +
                        _timeSliceBaseTime + " SDI: " + getSDI(OUTPUT) + " value: " + out);
                setOutput(output, out, t);
            }
            catch(NoConversionException ignored) {// not an error to not have a value, just don't set anything
                debug3(comp.getAlgorithmName()+"-"+alg_ver+" Failed to find component At: " +
                        _timeSliceBaseTime + " SDI: " + getSDI(OUTPUT) + " Deleting old value");
                deleteOutput(output, t);
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