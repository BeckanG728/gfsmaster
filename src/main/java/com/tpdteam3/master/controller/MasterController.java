package com.tpdteam3.master.controller;

import com.tpdteam3.master.model.FileMetadata;
import com.tpdteam3.master.service.MasterService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

@RestController
@RequestMapping("/api/master")
@CrossOrigin(origins = "*")
public class MasterController {

    @Autowired
    private MasterService masterService;

    /**
     * Endpoint para planificar la subida de un archivo
     */
    @PostMapping("/upload")
    public ResponseEntity<Map<String, Object>> planUpload(@RequestBody Map<String, Object> request) {
        try {
            String imagenId = (String) request.get("imagenId");
            Number sizeNumber = (Number) request.get("size");
            long size = sizeNumber.longValue();

            FileMetadata metadata = masterService.planUpload(imagenId, size);

            Map<String, Object> response = new HashMap<>();
            response.put("status", "success");
            response.put("imagenId", metadata.getImagenId());
            response.put("chunks", metadata.getChunks());

            return ResponseEntity.ok(response);
        } catch (Exception e) {
            e.printStackTrace();
            Map<String, Object> error = new HashMap<>();
            error.put("status", "error");
            error.put("message", e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(error);
        }
    }

    /**
     * Endpoint para obtener metadatos de un archivo
     */
    @GetMapping("/metadata")
    public ResponseEntity<Map<String, Object>> getMetadata(@RequestParam String imagenId) {
        try {
            FileMetadata metadata = masterService.getMetadata(imagenId);

            Map<String, Object> response = new HashMap<>();
            response.put("status", "success");
            response.put("imagenId", metadata.getImagenId());
            response.put("size", metadata.getSize());
            response.put("chunks", metadata.getChunks());
            response.put("timestamp", metadata.getTimestamp());

            return ResponseEntity.ok(response);
        } catch (RuntimeException e) {
            Map<String, Object> error = new HashMap<>();
            error.put("status", "error");
            error.put("message", e.getMessage());
            return ResponseEntity.status(HttpStatus.NOT_FOUND).body(error);
        }
    }

    /**
     * Endpoint para eliminar un archivo
     */
    @DeleteMapping("/delete")
    public ResponseEntity<Map<String, String>> deleteFile(@RequestParam String imagenId) {
        try {
            masterService.deleteFile(imagenId);

            Map<String, String> response = new HashMap<>();
            response.put("status", "success");
            response.put("message", "Archivo eliminado correctamente");

            return ResponseEntity.ok(response);
        } catch (Exception e) {
            Map<String, String> error = new HashMap<>();
            error.put("status", "error");
            error.put("message", e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(error);
        }
    }

    /**
     * Endpoint para listar todos los archivos
     */
    @GetMapping("/files")
    public ResponseEntity<Collection<FileMetadata>> listFiles() {
        return ResponseEntity.ok(masterService.listFiles());
    }

    /**
     * Endpoint para obtener estadísticas del sistema
     */
    @GetMapping("/stats")
    public ResponseEntity<Map<String, Object>> getStats() {
        return ResponseEntity.ok(masterService.getStats());
    }

    /**
     * Endpoint de health check
     */
    @GetMapping("/health")
    public ResponseEntity<Map<String, String>> health() {
        Map<String, String> response = new HashMap<>();
        response.put("status", "UP");
        response.put("service", "Master Service");
        return ResponseEntity.ok(response);
    }
}