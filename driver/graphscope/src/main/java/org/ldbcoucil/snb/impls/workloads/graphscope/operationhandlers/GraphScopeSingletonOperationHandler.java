package org.ldbcoucil.snb.impls.workloads.graphscope.operationhandlers;

import org.ldbcouncil.snb.driver.DbException;
import org.ldbcouncil.snb.driver.Operation;
import org.ldbcouncil.snb.driver.ResultReporter;

import org.ldbcoucil.snb.impls.workloads.graphscope.GraphScopeDbConnectionState;
import org.ldbcouncil.snb.impls.workloads.operationhandlers.SingletonOperationHandler;


import java.util.Map;
import java.io.IOException;

public abstract class GraphScopeSingletonOperationHandler <TOperation extends Operation<TOperationResult>, TOperationResult>
        implements SingletonOperationHandler<TOperationResult,TOperation, GraphScopeDbConnectionState> {
    public abstract TOperationResult toResult(byte [] bs);

    public Map<String, Object> getParameters( TOperation operation )
    {
        return operation.parameterMap();
    }

    @Override
    public void executeOperation(TOperation operation, GraphScopeDbConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {
        final byte[] parameters = serialization(operation);
	byte[] bs = null;
        try
        {
            bs = dbConnectionState.syncPost(parameters);
        } catch (IOException e) {
            e.printStackTrace();
	    throw new DbException(e);
        }
	if (bs == null) {
	    throw new DbException("Query response is null");
	}
	TOperationResult result = toResult(bs);
        resultReporter.report( 1, result, operation );
    }

    @Override
    public String getQueryString(GraphScopeDbConnectionState state, TOperation operation) {
        return null;
    }
    protected abstract byte [] serialization(TOperation o);
}
