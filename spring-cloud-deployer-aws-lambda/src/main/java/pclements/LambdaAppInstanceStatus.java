package pclements;

import com.amazonaws.services.lambda.AWSLambda;
import com.amazonaws.services.lambda.AWSLambdaClient;
import com.amazonaws.services.lambda.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cloud.deployer.spi.app.AppInstanceStatus;
import org.springframework.cloud.deployer.spi.app.DeploymentState;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class LambdaAppInstanceStatus implements AppInstanceStatus {

    private final Logger logger = LoggerFactory.getLogger(LambdaAppInstanceStatus.class);

    private final String arn;
    private DeploymentState deploymentState;
    private GetFunctionResult lambdaInfo = null;

    public LambdaAppInstanceStatus(AWSLambda lambdaClient, String arn) {
        this.arn = arn;
        try {
            this.lambdaInfo = lambdaClient.getFunction(new GetFunctionRequest().withFunctionName(arn));
            deploymentState = DeploymentState.deployed;
            logger.info("Lambda definition found for ARN:", arn);
        } catch (ServiceException|TooManyRequestsException|InvalidParameterValueException e) {
            logger.error("Unable to determine lambda status for ARN:", arn, e);
            deploymentState = DeploymentState.unknown;
        } catch (ResourceNotFoundException e) {
            logger.error("Lambda not found with ARN:", arn, e);
            deploymentState = DeploymentState.undeployed;
        }
    }

    @Override
    public String getId() {
        return arn;
    }

    @Override
    public DeploymentState getState() {
        return deploymentState;
    }

    @Override
    public Map<String, String> getAttributes() {
        Map<String, String> configuration = new HashMap<>();
        configuration.put("arn", lambdaInfo.getConfiguration().getFunctionArn());
        configuration.put("name", lambdaInfo.getConfiguration().getFunctionName());
        configuration.put("memory", lambdaInfo.getConfiguration().getMemorySize().toString());
        configuration.put("description", lambdaInfo.getConfiguration().getDescription());
        configuration.put("codeSha256", lambdaInfo.getConfiguration().getCodeSha256());
        configuration.put("lastModified", lambdaInfo.getConfiguration().getLastModified());
        configuration.put("tags", lambdaInfo.getTags().values().stream().map(Object::toString).collect(Collectors.joining(",")));
        return configuration;
    }

    public GetFunctionResult getLambdaInfo() {
        return this.lambdaInfo;
    }
}
