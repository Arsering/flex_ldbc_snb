package org.ldbcoucil.snb.impls.workloads.graphscope.operationhandlers;

import java.io.IOException;

import org.ldbcouncil.snb.driver.DbException;
import org.ldbcouncil.snb.driver.Operation;
import org.ldbcouncil.snb.driver.ResultReporter;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcNoResult;

import org.ldbcoucil.snb.impls.workloads.graphscope.GraphScopeDbConnectionState;
import org.ldbcouncil.snb.impls.workloads.operationhandlers.UpdateOperationHandler;


public abstract class GraphScopeUpdateOperationHandler <TOperation extends Operation<LdbcNoResult>>
        implements UpdateOperationHandler<TOperation, GraphScopeDbConnectionState> {

    @Override
    public void executeOperation(TOperation operation, GraphScopeDbConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {
        final byte[] parameters = serialization(operation);
        try
        {
            dbConnectionState.syncPostWithoutReply(parameters);
        } catch (IOException e) {
            e.printStackTrace();
	    throw new DbException(e);
        }
        resultReporter.report( 0, LdbcNoResult.INSTANCE, operation );
    }

    @Override
    public String getQueryString(GraphScopeDbConnectionState state, TOperation operation) {
        return null;
    }
    protected abstract byte [] serialization(TOperation o);
}
