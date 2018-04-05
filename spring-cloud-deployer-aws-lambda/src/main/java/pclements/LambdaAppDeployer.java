package pclements;

import com.amazonaws.services.lambda.AWSLambda;
import com.amazonaws.services.lambda.AWSLambdaClientBuilder;
import com.amazonaws.services.lambda.model.*;
import com.amazonaws.services.lambda.model.Runtime;
import com.amazonaws.util.VersionInfoUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cloud.deployer.spi.app.AppDeployer;
import org.springframework.cloud.deployer.spi.app.AppStatus;
import org.springframework.cloud.deployer.spi.app.DeploymentState;
import org.springframework.cloud.deployer.spi.core.AppDeploymentRequest;
import org.springframework.cloud.deployer.spi.core.RuntimeEnvironmentInfo;
import org.springframework.cloud.deployer.spi.util.RuntimeVersionUtils;

import java.util.HashMap;
import java.util.Map;

public class LambdaAppDeployer implements AppDeployer {

    private Logger logger = LoggerFactory.getLogger(getClass());
    private final String S3_BUCKET_PROPERTY = "s3.bucket";
    private final String S3_KEY_PROPERTY = "s3.key";
    private final String FUNCTION_MEMORY_PROPERTY = "app.function.memory";

    AWSLambda lambdaClient = AWSLambdaClientBuilder.standard().build();

    @Override
    public String deploy(AppDeploymentRequest appDeploymentRequest) {
        String deploymentId = createDeploymentId(appDeploymentRequest);
        logger.info("Starting deployment of app {}", deploymentId);
        // create or update a lambda
        String appName = appDeploymentRequest.getDefinition().getName();
        String appDescription = String.format("%s-%s",appName, deploymentId);

        CreateFunctionRequest createFunctionRequest = new CreateFunctionRequest()
                .withFunctionName(appName)
                .withRuntime(Runtime.Java8)
                .withDescription(appDescription)
                .withMemorySize(getFunctionMemory(appDeploymentRequest.getDeploymentProperties().get(FUNCTION_MEMORY_PROPERTY)))
                .withCode(createFunctionCodeReference(appDeploymentRequest))
                .withEnvironment(createFunctionEnvironment(appDeploymentRequest))
                .withPublish(true)
                .withTags(createFunctionTags(appDeploymentRequest, deploymentId));

        CreateFunctionResult result = lambdaClient.createFunction(createFunctionRequest);
        String appId = String.format("%s:%s", deploymentId, result.getFunctionArn());

        // TODO create/find kinesis source stream ? and destination?
        // Does the dataflow core do this for us during stream creation?
        String kinesisSourceArn = "";

        CreateEventSourceMappingRequest createEventSourceMappingRequest = new CreateEventSourceMappingRequest()
                .withEventSourceArn(kinesisSourceArn)
                .withFunctionName(result.getFunctionArn())
                .withEnabled(true);

        lambdaClient.createEventSourceMapping(createEventSourceMappingRequest);

        // Should we include the ARN and the deployment id in the returned value?
        return appId;
    }

    @Override
    public void undeploy(String appId) {
        // delete (or disable?) a lambda
        String[] ids = appId.split(":");
        String deploymentId = ids[0];
        String functionArn = ids[1];
        LambdaAppInstanceStatus lambdaStatus = new LambdaAppInstanceStatus(lambdaClient, functionArn);
        if(lambdaStatus.getState() == DeploymentState.deployed) {
            undeployFunction(functionArn);
            // TODO delete kinesis stream
        }
    }

    private void undeployFunction(String functionArn) {
        DeleteFunctionRequest request = new DeleteFunctionRequest()
                .withFunctionName(functionArn);
                //.withQualifier(version);
        try {
            DeleteFunctionResult result = lambdaClient.deleteFunction(request);
        } catch(ServiceException|TooManyRequestsException|InvalidParameterValueException e) {
            logger.error("Failed to delete lambda with ARN", functionArn, e);
            throw e;
        } catch(ResourceNotFoundException e){
            logger.error("Tried to delete a lambda that didn't exist with ARN", functionArn, e);
        }
    }

    @Override
    public AppStatus status(String appId) {
        // fetch lambda status
        String[] ids = appId.split(":");
        String deploymentId = ids[0];
        String functionArn = ids[1];

        AppStatus.Builder builder = AppStatus.of(appId);
        return builder.with(new LambdaAppInstanceStatus(lambdaClient, functionArn)).build();
    }

    @Override
    public RuntimeEnvironmentInfo environmentInfo() {
        return new RuntimeEnvironmentInfo.Builder()
                .spiClass(AppDeployer.class)
                .implementationName(this.getClass().getSimpleName())
                .implementationVersion(RuntimeVersionUtils.getVersion(this.getClass()))
                .platformType("AWS Lambda")
                .platformApiVersion(System.getProperty("os.name") + " " + System.getProperty("os.version"))
                .platformClientVersion("aws sdk version:"+VersionInfoUtils.getVersion())
                .platformHostVersion(VersionInfoUtils.getPlatform())
                .build();

    }

    private Map<String,String> createFunctionTags(AppDeploymentRequest appDeploymentRequest, String deploymentId) {
        Map<String, String> tags = new HashMap<>();
        tags.put("deploymentId", deploymentId);
        // TODO? Kinesis source and destination ARNs?
        String kinesisSourceArn = "";
        String kinesisDestinationArn = "";
        tags.put("source", kinesisSourceArn);
        tags.put("destination", kinesisDestinationArn);
        return tags;
    }

    private Environment createFunctionEnvironment(AppDeploymentRequest appDeploymentRequest) {
        //TODO perhaps include information about the destination Kinesis stream so the broker binder for kinesis can be configured to publish there?
        Environment environment = new Environment();
        for(Map.Entry<String, String> entry: appDeploymentRequest.getDefinition().getProperties().entrySet()){
            environment.addVariablesEntry(entry.getKey(), entry.getValue());
        }
        return environment;
    }

    private FunctionCode createFunctionCodeReference(AppDeploymentRequest appDeploymentRequest) {
        FunctionCode code = new FunctionCode();
        code.setS3Bucket(appDeploymentRequest.getDeploymentProperties().get(S3_BUCKET_PROPERTY));
        code.setS3Key(appDeploymentRequest.getDeploymentProperties().get(S3_KEY_PROPERTY));

        if(code.getS3Bucket() == null || code.getS3Key() == null){
            throw new RuntimeException(String.format("%s and %s for function source code required", S3_BUCKET_PROPERTY, S3_KEY_PROPERTY));
        }

        return code;
    }

    private int getFunctionMemory(String memory) {
        // Lambda default value is 128 MB. The value must be a multiple of 64 MB.
        int mb = 128;
        try {
            Integer.parseInt(memory);
        } catch (NumberFormatException e) {
            logger.error("Invalid function memory specified: '{}', defaulting to {}MB", memory, mb, e);
        }
        return mb;
    }

    private String createDeploymentId(AppDeploymentRequest request) {
        String groupId = request.getDeploymentProperties().get(AppDeployer.GROUP_PROPERTY_KEY);
        String deploymentId;
        if (groupId == null) {
            deploymentId = String.format("%s", request.getDefinition().getName());
        }
        else {
            deploymentId = String.format("%s-%s", groupId, request.getDefinition().getName());
        }
        return deploymentId;
    }


}
