package org.apache.drill.exec.physical.config;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.drill.exec.physical.EndpointAffinity;
import org.apache.drill.exec.physical.OperatorCost;
import org.apache.drill.exec.physical.ReadEntry;
import org.apache.drill.exec.physical.base.AbstractScan;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.base.Scan;
import org.apache.drill.exec.physical.base.Size;
import org.apache.drill.exec.proto.CoordinationProtos;

import java.util.List;

public class JSONScanPOP extends AbstractScan<JSONScanPOP.ScanEntry> {
    private final String url;

    @JsonCreator
    public JSONScanPOP(@JsonProperty("url") String url, @JsonProperty("entries") List<ScanEntry> readEntries) {
        super(readEntries);
        this.url = url;
    }

    @Override
    public void applyAssignments(List<CoordinationProtos.DrillbitEndpoint> endpoints) {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public Scan<?> getSpecificScan(int minorFragmentId) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public List<EndpointAffinity> getOperatorAffinity() {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public PhysicalOperator getNewWithChildren(List<PhysicalOperator> children) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    public class ScanEntry implements ReadEntry {

        @Override
        public OperatorCost getCost() {
            return null;  //To change body of implemented methods use File | Settings | File Templates.
        }

        @Override
        public Size getSize() {
            return null;  //To change body of implemented methods use File | Settings | File Templates.
        }
    }
}
