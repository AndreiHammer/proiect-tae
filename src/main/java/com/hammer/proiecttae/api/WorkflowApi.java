package com.hammer.proiecttae.api;

import com.hammer.proiecttae.service.WorkflowService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Profile;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@Slf4j
@RestController
@RequiredArgsConstructor
@RequestMapping("api/workflow")
@Profile("invoice-producer")
public class WorkflowApi {

    private final WorkflowService workflowService;

    @GetMapping("/start")
    public ResponseEntity<String> startWorkflow() {
        log.info("GET /api/workflow/start received – initiating workflow");
        String result = workflowService.startWorkflow();
        log.info(result);
        return ResponseEntity.ok(result);
    }
}
