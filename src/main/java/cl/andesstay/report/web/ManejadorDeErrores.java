package cl.andesstay.report.web;

import jakarta.persistence.EntityNotFoundException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.util.Map;

/**
 * Traduce las excepciones a los codigos que fija infra/docs/contracts/estados.md.
 *
 * Este servicio es de solo lectura, asi que el repertorio es corto: no encontrado, rol
 * insuficiente y peticion mal formada. Sin este manejador, los tres salian como 500 con una traza
 * de Spring, y el frontend no podia distinguir "no existe" de "el backend se cayo".
 *
 * Deliberadamente NO se captura Exception a secas: un fallo inesperado tiene que seguir siendo un
 * 500 con su traza en el log, o se esconderian defectos reales.
 */
@RestControllerAdvice
public class ManejadorDeErrores {

    @ExceptionHandler(EntityNotFoundException.class)
    public ResponseEntity<Map<String, Object>> noEncontrado(EntityNotFoundException e) {
        return ResponseEntity.status(HttpStatus.NOT_FOUND)
                .body(Map.of("error", "NO_ENCONTRADO", "detalle", e.getMessage()));
    }

    @ExceptionHandler(AccessDeniedException.class)
    public ResponseEntity<Map<String, Object>> sinPermiso(AccessDeniedException e) {
        return ResponseEntity.status(HttpStatus.FORBIDDEN)
                .body(Map.of("error", "ROL_INSUFICIENTE"));
    }

    @ExceptionHandler(IllegalArgumentException.class)
    public ResponseEntity<Map<String, Object>> peticionInvalida(IllegalArgumentException e) {
        return ResponseEntity.badRequest()
                .body(Map.of("error", "PETICION_INVALIDA", "detalle", String.valueOf(e.getMessage())));
    }
}
