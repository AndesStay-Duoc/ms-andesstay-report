package cl.andesstay.report.messaging.events;

/**
 * Quien origino la accion. Campo obligatorio del envelope comun; ver
 * infra/docs/contracts/events/envelope.md.
 *
 * Es una copia del record que publica ms-andesstay-reservations. No se comparte una libreria
 * porque el proyecto es un polirepo sin registro de artefactos; lo que mantiene a las dos copias
 * compatibles es el contrato escrito y el hecho de que el productor no manda la cabecera con el
 * nombre de la clase: el consumidor declara el tipo que espera y los paquetes no tienen que
 * coincidir.
 */
public record Actor(String userId, String role, String tenant) {
}
