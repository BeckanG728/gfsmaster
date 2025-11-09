package com.tpdteam3.master.service;

import com.tpdteam3.master.model.FileMetadata;
import com.tpdteam3.master.model.FileMetadata.ChunkMetadata;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class MasterService {

    // Almacena metadatos de archivos en memoria
    private final Map<String, FileMetadata> fileMetadataStore = new ConcurrentHashMap<>();

    // Lista de chunkservers disponibles CON context-path
    private final List<String> chunkservers = new ArrayList<>();
    private int nextChunkserverIndex = 0;

    public MasterService() {
        // ✅ CORRECCIÓN: Registrar chunkservers CON el context-path /chunkserver
        chunkservers.add("http://localhost:9001/chunkserver");
        chunkservers.add("http://localhost:9002/chunkserver");
        chunkservers.add("http://localhost:9003/chunkserver");

        System.out.println("✅ Master Service iniciado con " + chunkservers.size() + " chunkservers:");
        chunkservers.forEach(cs -> System.out.println("   - " + cs));
    }

    /**
     * Planifica dónde se almacenarán los fragmentos de un archivo
     */
    public FileMetadata planUpload(String imagenId, long fileSize) {
        FileMetadata metadata = new FileMetadata(imagenId, fileSize);

        // Calcular número de fragmentos necesarios (512KB por fragmento)
        int chunkSize = 512 * 1024;
        int numChunks = (int) Math.ceil((double) fileSize / chunkSize);

        System.out.println("📋 Planificando upload para imagen: " + imagenId);
        System.out.println("   Tamaño: " + fileSize + " bytes");
        System.out.println("   Fragmentos necesarios: " + numChunks);

        // Asignar cada fragmento a un chunkserver (round-robin simplificado)
        for (int i = 0; i < numChunks; i++) {
            String chunkserver = getNextChunkserver();
            ChunkMetadata chunk = new ChunkMetadata(i, chunkserver, chunkserver);
            metadata.getChunks().add(chunk);
            System.out.println("   Fragmento " + i + " → " + chunkserver);
        }

        // Guardar metadatos
        fileMetadataStore.put(imagenId, metadata);

        return metadata;
    }

    /**
     * Obtiene metadatos de un archivo
     */
    public FileMetadata getMetadata(String imagenId) {
        FileMetadata metadata = fileMetadataStore.get(imagenId);
        if (metadata == null) {
            throw new RuntimeException("Archivo no encontrado: " + imagenId);
        }
        return metadata;
    }

    /**
     * Elimina metadatos de un archivo
     */
    public void deleteFile(String imagenId) {
        fileMetadataStore.remove(imagenId);
        System.out.println("🗑️ Metadatos eliminados para: " + imagenId);
    }

    /**
     * Obtiene el siguiente chunkserver disponible (round-robin)
     */
    private String getNextChunkserver() {
        String chunkserver = chunkservers.get(nextChunkserverIndex);
        nextChunkserverIndex = (nextChunkserverIndex + 1) % chunkservers.size();
        return chunkserver;
    }

    /**
     * Lista todos los archivos registrados
     */
    public Collection<FileMetadata> listFiles() {
        return fileMetadataStore.values();
    }

    /**
     * Registra un nuevo chunkserver (para extensibilidad)
     */
    public void registerChunkserver(String url) {
        if (!chunkservers.contains(url)) {
            chunkservers.add(url);
            System.out.println("✅ Nuevo chunkserver registrado: " + url);
        }
    }

    /**
     * Obtiene estadísticas del sistema
     */
    public Map<String, Object> getStats() {
        Map<String, Object> stats = new HashMap<>();
        stats.put("totalFiles", fileMetadataStore.size());
        stats.put("totalChunkservers", chunkservers.size());
        stats.put("chunkservers", chunkservers);

        long totalSize = fileMetadataStore.values().stream()
                .mapToLong(FileMetadata::getSize)
                .sum();
        stats.put("totalStorageUsed", totalSize);

        return stats;
    }
}