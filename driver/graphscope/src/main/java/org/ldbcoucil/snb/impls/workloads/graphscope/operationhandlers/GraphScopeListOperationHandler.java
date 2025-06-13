package org.ldbcoucil.snb.impls.workloads.graphscope.operationhandlers;

import org.ldbcouncil.snb.driver.DbException;
import org.ldbcouncil.snb.driver.Operation;
import org.ldbcouncil.snb.driver.ResultReporter;

import org.ldbcoucil.snb.impls.workloads.graphscope.GraphScopeDbConnectionState;
import org.ldbcouncil.snb.impls.workloads.operationhandlers.ListOperationHandler;

import java.util.List;
import java.util.Map;

import java.io.IOException;

public abstract class GraphScopeListOperationHandler <TOperation extends Operation<List<TOperationResult>>, TOperationResult>
        implements ListOperationHandler<TOperationResult,TOperation, GraphScopeDbConnectionState>
{
    public Map<String, Object> getParameters( TOperation operation )
    {
        return operation.parameterMap();
    }
    public abstract List<TOperationResult> toResult(byte [] bs);

    @Override
    public void executeOperation(TOperation operation, GraphScopeDbConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {
        final byte[] parameters = serialization(operation);
	byte[] bs = null;
        try {
	    bs = dbConnectionState.syncPost(parameters);
	} catch (IOException e) {
            e.printStackTrace();
	    throw new DbException(e);
	}	
	if (bs == null) {
	    throw new DbException("Query response is null");
	}
	List<TOperationResult> results = toResult(bs);
        resultReporter.report( results.size(), results, operation );
    }

    @Override
    public String getQueryString(GraphScopeDbConnectionState state, TOperation operation) {
        return null;
    }
    protected abstract byte [] serialization(TOperation o);
}
