package cl.andesstay.report.config;

import cl.andesstay.report.messaging.events.ReservationEvent;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;

import java.util.HashMap;
import java.util.Map;

/**
 * El cliente consumidor de reservations.events, tipado y declarado en codigo.
 *
 * EL TIPO ES PARTE DEL CONTRATO. Antes el consumidor recibia un Map&lt;String, Object&gt; y leia
 * cada campo con un cast: un nombre mal escrito se convertia en null y el KPI correspondiente
 * salia en cero sin que nadie se enterara. Declarar ReservationEvent hace que un mensaje con otra
 * forma falle al deserializar, que es visible, en vez de agregar a medias, que no lo es.
 *
 * LOS PAQUETES DE CONFIANZA se limitan a cl.andesstay. Estaban en "*", que desactiva la proteccion
 * de JsonDeserializer y permite instanciar cualquier clase del classpath a partir de un mensaje.
 *
 * SE IGNORA LA CABECERA DE TIPO porque el productor no la manda: cada repositorio tiene su copia
 * del record y los paquetes no coinciden. El consumidor declara el tipo que espera.
 *
 * ErrorHandlingDeserializer ENVUELVE AL DE JSON. Sin el, un mensaje con JSON invalido revienta
 * dentro del poll, antes de que el manejador de errores pueda verlo, y el contenedor entra en un
 * bucle de reintento del que no sale. Con el, el fallo llega al manejador como cualquier otro y
 * termina en el tema de descarte.
 *
 * CONCURRENCIA 3 porque el tema tiene tres particiones. Una sola instancia las atiende las tres;
 * si se levantan tres instancias, el grupo reparte una por instancia. Es el mecanismo de balanceo
 * de Kafka y el numero sale de la topologia, no de una estimacion.
 *
 * LA CONFIRMACION ES MANUAL: el offset avanza cuando el evento esta en la base, no cuando se
 * entrego al metodo.
 *
 * ESTE CONSUMIDOR NO ES LA TOPOLOGIA. Kafka Streams lee el mismo tema con su propio grupo y
 * mantiene los KPIs en vivo; este guarda cada evento en MySQL para el historico. Dos grupos
 * distintos sobre el mismo tema, que es justo lo que Kafka permite y una cola no: cada uno lee
 * todos los mensajes sin quitarselos al otro.
 */
@Configuration
public class KafkaConsumerConfig {

    private static final String PAQUETES_DE_CONFIANZA = "cl.andesstay.*";
    private static final int PARTICIONES_DEL_TEMA = 3;

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${andesstay.kafka.groups.report}")
    private String grupoDeReporteria;

    @Bean
    public ConsumerFactory<String, ReservationEvent> eventosConsumerFactory() {
        Map<String, Object> propiedades = new HashMap<>();

        propiedades.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        propiedades.put(ConsumerConfig.GROUP_ID_CONFIG, grupoDeReporteria);
        propiedades.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        propiedades.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        // Un lote pequeño mantiene corta la distancia entre lo que se procesa y lo que se
        // confirma: tras un reequilibrio se repite menos trabajo.
        propiedades.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 50);

        propiedades.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        propiedades.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        propiedades.put(ErrorHandlingDeserializer.KEY_DESERIALIZER_CLASS, StringDeserializer.class);
        propiedades.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, JsonDeserializer.class);

        propiedades.put(JsonDeserializer.VALUE_DEFAULT_TYPE, ReservationEvent.class.getName());
        propiedades.put(JsonDeserializer.TRUSTED_PACKAGES, PAQUETES_DE_CONFIANZA);
        propiedades.put(JsonDeserializer.USE_TYPE_INFO_HEADERS, false);

        return new DefaultKafkaConsumerFactory<>(propiedades);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, ReservationEvent>
            eventosKafkaListenerContainerFactory(CommonErrorHandler manejadorDeErrores) {

        var factory = new ConcurrentKafkaListenerContainerFactory<String, ReservationEvent>();
        factory.setConsumerFactory(eventosConsumerFactory());
        factory.setConcurrency(PARTICIONES_DEL_TEMA);
        factory.setCommonErrorHandler(manejadorDeErrores);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);
        return factory;
    }
}
