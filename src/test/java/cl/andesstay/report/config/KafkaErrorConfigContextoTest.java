package cl.andesstay.report.config;

import cl.andesstay.report.web.ManejadorDeErrores;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.kafka.listener.CommonErrorHandler;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * El manejador de errores de Kafka y el de la API REST tienen que poder vivir en el mismo contexto.
 *
 * POR QUE EXISTE ESTE TEST. El @Bean de KafkaErrorConfig se llamaba manejadorDeErrores, el mismo
 * nombre que Spring le da al @RestControllerAdvice web.ManejadorDeErrores. Spring Boot prohibe
 * sobrescribir beans, asi que el servicio no arrancaba: BeanDefinitionOverrideException en el
 * arranque del contenedor. Ningun test lo vio porque ninguno cargaba las dos clases juntas.
 *
 * POR QUE ApplicationContextRunner Y NO @SpringBootTest. Basta con las dos clases en conflicto para
 * reproducir el fallo, y asi el test no necesita MySQL ni un broker. La sobrescritura se prohibe a
 * proposito, igual que la prohibe SpringApplication: si no, el contexto de prueba aceptaria lo que
 * el servicio real rechaza.
 */
class KafkaErrorConfigContextoTest {

    private final ApplicationContextRunner contexto = new ApplicationContextRunner()
            .withAllowBeanDefinitionOverriding(false)
            .withPropertyValues("spring.kafka.bootstrap-servers=localhost:9092")
            .withUserConfiguration(ManejadorDeErrores.class, KafkaErrorConfig.class);

    @Test
    @DisplayName("El manejador de Kafka y el @RestControllerAdvice arrancan juntos")
    void arrancanJuntos() {
        contexto.run(ctx -> {
            assertThat(ctx).hasNotFailed();
            assertThat(ctx).hasSingleBean(ManejadorDeErrores.class);
            assertThat(ctx).hasSingleBean(CommonErrorHandler.class);
        });
    }
}
