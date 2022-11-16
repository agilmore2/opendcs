package decodes.hdb.dbutils;

import decodes.sql.DbKey;
import decodes.tsdb.CTimeSeries;
import decodes.tsdb.DbComputation;
import decodes.tsdb.TimeSeriesIdentifier;
import ilex.util.Logger;
import opendcs.dai.TimeSeriesDAI;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

public class HDBAlgoTSUtils {

    /**
     * findOutputSeries processes query results into a timeseries map
     *
     * @param dbobj        - DataObject holding query results
     * @param dao          - database interface
     * @param outputSeries - Map of timeseries to populate
     * @return whether timeseries map populated correctly
     */
    public static boolean findOutputSeries(DataObject dbobj, TimeSeriesDAI dao, HashMap<String, CTimeSeries> outputSeries) {

        // Need to handle multiple TS_IDs!
        int count = Integer.parseInt(dbobj.get("rowCount").toString());
        ArrayList<Object> tsids;

        if (count == 0)
        {
            Logger.instance().warning("Zero output TS_IDs, dbobj: " + dbobj);
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

        try {
            for (Object o : tsids) {
                DbKey id = DbKey.createDbKey(Long.parseLong(o.toString()));
                Logger.instance().debug3("Output Timeseries ID: " + id);
                TimeSeriesIdentifier tsid = dao.getTimeSeriesIdentifier(id);
                outputSeries.putIfAbsent(tsid.getKey().toString(), dao.makeTimeSeries(tsid));
            }
        } catch (Exception e) {
            Logger.instance().warning(e.toString());
            return false;
        }
        return true;
    }
}