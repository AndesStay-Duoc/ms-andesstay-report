# Registro de cambios

El formato sigue [Keep a Changelog](https://keepachangelog.com/es-ES/1.1.0/) y el versionado es
[semántico](https://semver.org/lang/es/). La versión del artefacto no es la de la API: esta
publica `/api/v1` y seguirá haciéndolo mientras no haya un cambio incompatible de contrato.

## [1.1.0]

### Añadido
- Topología de Kafka Streams (`KpiTopology`) que mantiene los tres KPIs del caso al día a medida
  que llegan los eventos, en lugar de recalcularlos en cada consulta.
- `GET /api/v1/report/kpis/live`, que lee esos KPIs del estado de Streams con consultas
  interactivas. `GET /api/v1/report/kpis` no cambia y sigue sirviendo el histórico desde MySQL.
- **Tiempo de ciclo**, el KPI que el caso pedía y no existía en ninguna parte. Se mide de la
  creación al cierre de cada reserva con los instantes reales de los eventos.
- `InstanteDelEvento`, extractor que agrupa por el momento en que ocurrió el hecho y no por el que
  trae el registro de Kafka: releer el tema reconstruye exactamente las mismas ventanas.
- `IngestaDeEventosService`, con la lógica separada de la mensajería y probable sin broker.
- `KafkaErrorConfig`: reintento con espera creciente de 1 s, 2 s y 4 s, y descarte hacia
  `reservations.events.DLT`.
- Pruebas de la topología con `TopologyTestDriver` y del servicio de ingesta con Mockito.
- Flujo de integración continua que compila y prueba en cada cambio.

### Cambiado
- Las consultas de KPIs pasan al vocabulario del contrato (`reservation.created`,
  `reservation.in_stay`, `reservation.checked_out`). Desaparece el parche que aceptaba
  `EN_ESTADIA` y `EN_ESTADÍA` a la vez, que existía porque el tipo del evento era el nombre de un
  enum del dominio y ese nombre cambió.

### Corregido
- El consumidor envolvía todo en un `try` y, al fallar, registraba el error sin confirmar el
  offset: eso no reintentaba, bloqueaba la partición indefinidamente.
- `OCCURRED_AT` guardaba el momento del consumo, de modo que una relectura del tema amontonaba
  toda la serie de «reservas por hora» en la hora de la relectura.

### Seguridad
- `spring.json.trusted.packages` pasa de `*` a `cl.andesstay.*`.

## [1.0.0]
- Primera versión: tabla de agregaciones y endpoints de KPIs.
