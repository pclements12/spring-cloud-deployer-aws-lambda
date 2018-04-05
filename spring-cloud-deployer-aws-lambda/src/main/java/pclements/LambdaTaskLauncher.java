package pclements;

import org.springframework.cloud.deployer.spi.core.AppDeploymentRequest;
import org.springframework.cloud.deployer.spi.core.RuntimeEnvironmentInfo;
import org.springframework.cloud.deployer.spi.task.TaskLauncher;
import org.springframework.cloud.deployer.spi.task.TaskStatus;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

public class LambdaTaskLauncher implements TaskLauncher {
    @Override
    public String launch(AppDeploymentRequest appDeploymentRequest) {
        throw new NotImplementedException();
    }

    @Override
    public void cancel(String s) {
        throw new NotImplementedException();
    }

    @Override
    public TaskStatus status(String s) {
        throw new NotImplementedException();
    }

    @Override
    public void cleanup(String s) {
        throw new NotImplementedException();
    }

    @Override
    public void destroy(String s) {
        throw new NotImplementedException();
    }

    @Override
    public RuntimeEnvironmentInfo environmentInfo() {
        return null;
    }
}
