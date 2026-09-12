# ms-andesstay-report

Microservicio de **reportería** del sistema AndesStay. Consume el tópico Kafka
`reservations.events`, agrega los datos y expone el panel de operaciones en modo
**solo lectura**.

Alimenta el tablero que la red necesita para ver su operación en tiempo real, sin bloquear ni
acoplarse al servicio de reservas.

## Responsabilidad

- Consumir `reservations.events` desde Kafka.
- Mantener las agregaciones en el esquema Oracle `REPORT`.
- Exponer los KPIs de operación.

## KPIs

| Indicador | Qué mide |
|---|---|
| Reservas por hora | Volumen de reservas creadas y confirmadas por franja horaria |
| Tiempo de ciclo | Cuánto demora una reserva desde `CREADA` hasta `CHECKOUT` |
| Ocupación activa | Unidades en estado `EN_ESTADIA` en este momento |
| Unidades más demandadas | Ranking de unidades por reservas en el rango |

## Endpoints

| Método | Ruta | Rol |
|---|---|---|
| `GET` | `/api/report/kpis?range=last24h` | Admin |
| `GET` | `/api/report/top-units?range=last7d` | Admin |

Se accede siempre a través del BFF, detrás del API Gateway. Ver
[`rutas-gateway.md`](https://github.com/AndesStay-Duoc/infra/blob/develop/docs/contracts/rutas-gateway.md).

## Stack

Java 21 · Spring Boot 3.5 · Spring Kafka · Spring Data JPA · Oracle · Spring Security como
resource server.

## Variables de entorno

| Variable | Descripción |
|---|---|
| `DB_URL` | JDBC del esquema `REPORT`, por ejemplo `jdbc:oracle:thin:@//host:1521/FREEPDB1` |
| `DB_USER` | Usuario del esquema `REPORT` |
| `DB_PASSWORD` | Contraseña del esquema |
| `KAFKA_BOOTSTRAP_SERVERS` | Lista de brokers |
| `JWT_ISSUER_STAFF` | Issuer del tenant corporativo |
| `JWT_AUDIENCE_STAFF` | Audience esperada del token corporativo |

Se configuran en un archivo `.env` que **no se versiona**. Ver `.env.example`.

## Cómo levantarlo

```bash
./mvnw spring-boot:run -Dspring-boot.run.profiles=dev
```

Requiere Oracle y Kafka en marcha. Los compose están en el repositorio
[`infra`](https://github.com/AndesStay-Duoc/infra).

## Contratos

Los esquemas de eventos y el envelope común son canónicos y viven en
[`infra/docs/contracts/`](https://github.com/AndesStay-Duoc/infra/tree/develop/docs/contracts).
Todo consumidor descarta duplicados por `eventId`.

## Cómo contribuir

Ver [`CONTRIBUTING.md`](CONTRIBUTING.md).
