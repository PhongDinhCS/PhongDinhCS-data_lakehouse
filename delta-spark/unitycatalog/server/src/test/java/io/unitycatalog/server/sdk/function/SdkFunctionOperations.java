package io.unitycatalog.server.sdk.function;

import io.unitycatalog.client.ApiClient;
import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.api.FunctionsApi;
import io.unitycatalog.client.model.CreateFunctionRequest;
import io.unitycatalog.client.model.FunctionInfo;
import io.unitycatalog.server.base.function.FunctionOperations;
import io.unitycatalog.server.utils.TestUtils;

import java.util.List;
import java.util.Objects;

public class SdkFunctionOperations implements FunctionOperations {

    private final FunctionsApi functionsAPI;

    public SdkFunctionOperations(ApiClient apiClient) {
        this.functionsAPI = new FunctionsApi(apiClient);
    }

    @Override
    public FunctionInfo createFunction(CreateFunctionRequest createFunctionRequest) throws ApiException {
        return functionsAPI.createFunction(createFunctionRequest);
    }

    @Override
    public List<FunctionInfo> listFunctions(String catalogName, String schemaName) throws ApiException {
        return TestUtils.toList(Objects.requireNonNull(functionsAPI
                .listFunctions(catalogName, schemaName, 100, null).getFunctions()));
    }

    @Override
    public FunctionInfo getFunction(String functionFullName) throws ApiException {
        return functionsAPI.getFunction(functionFullName);
    }

    @Override
    public void deleteFunction(String functionFullName, boolean force) throws ApiException {
        functionsAPI.deleteFunction(functionFullName);
    }
}