package pclements;


import org.springframework.boot.autoconfigure.AutoConfigureOrder;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cloud.deployer.spi.app.AppDeployer;
import org.springframework.cloud.deployer.spi.task.TaskLauncher;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.Ordered;

import java.util.Properties;

@Configuration
@AutoConfigureOrder(Ordered.HIGHEST_PRECEDENCE)
public class LambdaDeployerAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean(AppDeployer.class)
    public AppDeployer appDeployer() {
        return new LambdaAppDeployer();
    }

    @Bean
    @ConditionalOnMissingBean(TaskLauncher.class)
    public TaskLauncher taskLauncher() {
        return new LambdaTaskLauncher();
    }
}