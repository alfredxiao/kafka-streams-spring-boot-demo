package xiaoyf.demo.kafka.api;

import com.fasterxml.jackson.annotation.JsonInclude;
import demo.model.PreferenceValue;
import io.swagger.v3.core.jackson.ModelResolver;
import org.springframework.boot.autoconfigure.jackson.Jackson2ObjectMapperBuilderCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.converter.json.Jackson2ObjectMapperBuilder;

@Configuration
public class RestConfiguration {

    @Bean
    public ModelResolver modelResolver(Jackson2ObjectMapperBuilder builder) {
        return new ModelResolver(builder
                .mixIn(PreferenceValue.class, AvroMixin.class)
                .build()
        );
    }

    @Bean
    public Jackson2ObjectMapperBuilderCustomizer jsonCustomizer() {
        return builder -> builder
                .serializationInclusion(JsonInclude.Include.NON_NULL)
                .mixIn(PreferenceValue.class, AvroMixin.class);
    }
}
