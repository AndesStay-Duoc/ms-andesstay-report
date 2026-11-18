package cl.andesstay.report.controller;

import cl.andesstay.report.ApiPaths;

import cl.andesstay.report.repository.ReservationStatRepository;
import cl.andesstay.report.service.KpiEnVivoService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.*;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping(ApiPaths.REPORT)
@RequiredArgsConstructor
public class ReportController {

    private final ReservationStatRepository repository;
    private final KpiEnVivoService kpisEnVivo;

    /**
     * GET /api/report/kpis?range=last24h
     * Retorna: reservas por hora, ocupación activa.
     */
    @GetMapping("/kpis")
    @PreAuthorize("hasRole('Admin')")
    public ResponseEntity<Map<String, Object>> getKpis(
            @RequestParam(defaultValue = "last24h") String range) {

        LocalDateTime from = resolveFrom(range);

        List<Object[]> perHour = repository.reservationsPerHour(from);
        long active = repository.activeReservations();

        // Formatea la lista de reservas por hora
        List<Map<String, Object>> hourlyData = perHour.stream().map(row -> {
            Map<String, Object> entry = new HashMap<>();
            entry.put("hour", row[0]);
            entry.put("total", row[1]);
            return entry;
        }).toList();

        Map<String, Object> response = new HashMap<>();
        response.put("range", range);
        response.put("from", from.toString());
        response.put("activeReservations", active);
        response.put("reservationsPerHour", hourlyData);

        return ResponseEntity.ok(response);
    }

    /**
     * GET /api/report/top-units?range=last7d
     * Unidades más demandadas en el período.
     */
    @GetMapping("/top-units")
    @PreAuthorize("hasRole('Admin')")
    public ResponseEntity<List<Map<String, Object>>> getTopUnits(
            @RequestParam(defaultValue = "last7d") String range) {

        LocalDateTime from = resolveFrom(range);
        List<Object[]> rows = repository.topUnits(from);

        List<Map<String, Object>> result = rows.stream().map(row -> {
            Map<String, Object> entry = new HashMap<>();
            entry.put("unitId", row[0]);
            entry.put("totalReservations", row[1]);
            return entry;
        }).toList();

        return ResponseEntity.ok(result);
    }

    /**
     * GET /api/v1/report/kpis/live?range=last24h
     *
     * Los mismos tres KPIs del caso, pero leidos del estado que Kafka Streams mantiene al dia en
     * lugar de consultados a MySQL. La diferencia no es cosmetica: /kpis recorre el historico cada
     * vez que alguien abre el panel y su coste crece con los datos; esto lee un valor ya calculado
     * y responde igual de rapido con un evento o con un millon. Es lo que permite que la pantalla
     * se refresque sola sin castigar a la base.
     *
     * Añade ademas el tiempo de ciclo, que el caso pide y hasta ahora no existia en ningun sitio:
     * sale de restar el instante de creacion al de cierre de cada reserva, con los instantes
     * reales de los eventos.
     *
     * Si la topologia no esta corriendo —porque el servicio se desplego sin clúster de Kafka— se
     * responde disponible=false en vez de ceros, que serian indistinguibles de "no ha pasado nada".
     */
    @GetMapping("/kpis/live")
    @PreAuthorize("hasRole('Admin')")
    public ResponseEntity<Map<String, Object>> getKpisEnVivo(
            @RequestParam(defaultValue = "last24h") String range) {

        Map<String, Object> respuesta = new LinkedHashMap<>();
        respuesta.put("range", range);
        respuesta.put("fuente", "kafka-streams");
        respuesta.put("disponible", kpisEnVivo.disponible());

        if (!kpisEnVivo.disponible()) {
            respuesta.put("detalle", "La topología de Kafka Streams no está corriendo.");
            return ResponseEntity.ok(respuesta);
        }

        respuesta.put("reservasConocidas", kpisEnVivo.reservasEnEstado());
        respuesta.put("activeReservations", kpisEnVivo.ocupacionActiva());
        respuesta.put("tiempoDeCicloMedioMinutos", kpisEnVivo.tiempoDeCicloMedioEnMinutos());
        respuesta.put("reservationsPerHour", kpisEnVivo.reservasPorHora(resolverVentana(range)));

        return ResponseEntity.ok(respuesta);
    }

    /** El mismo vocabulario de rangos que /kpis, expresado como duración. */
    private Duration resolverVentana(String range) {
        return switch (range) {
            case "last1h"  -> Duration.ofHours(1);
            case "last7d"  -> Duration.ofDays(7);
            case "last30d" -> Duration.ofDays(30);
            default        -> Duration.ofHours(24);
        };
    }

    private LocalDateTime resolveFrom(String range) {
        return switch (range) {
            case "last1h"  -> LocalDateTime.now().minusHours(1);
            case "last24h" -> LocalDateTime.now().minusHours(24);
            case "last7d"  -> LocalDateTime.now().minusDays(7);
            case "last30d" -> LocalDateTime.now().minusDays(30);
            default        -> LocalDateTime.now().minusHours(24);
        };
    }
}
