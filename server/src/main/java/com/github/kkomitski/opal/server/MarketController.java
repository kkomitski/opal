package com.github.kkomitski.opal.server;

import java.io.IOException;
import java.nio.file.Files;

import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class MarketController {

    @GetMapping("/markets")
    public ResponseEntity<String> getMarkets() {
        try {
            Resource resource = new ClassPathResource("markets.xml");
            String content = new String(Files.readAllBytes(resource.getFile().toPath()));
            return ResponseEntity.ok()
                    .contentType(MediaType.APPLICATION_XML)
                    .body(content);
        } catch (IOException e) {
            return ResponseEntity.status(500).body("Error reading markets.xml");
        }
    }
}