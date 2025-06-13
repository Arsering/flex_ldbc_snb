package graphscope;

import org.ldbcoucil.snb.impls.workloads.graphscope.interactive.GraphScopeInteractiveDb;
import org.ldbcouncil.snb.impls.workloads.interactive.InteractiveTest;

import java.util.HashMap;
import java.util.Map;

public class GraphScopeInteractiveTest extends InteractiveTest {
    public GraphScopeInteractiveTest() {
        super(new GraphScopeInteractiveDb());
    }

    @Override
    protected Map<String, String> getProperties() {
        Map<String, String> properties = new HashMap<>();
        properties.put("url", "39.101.65.134:10000");
        return properties;
    }
}
