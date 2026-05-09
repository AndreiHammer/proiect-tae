package com.hammer.proiecttae.service;

import com.hammer.proiecttae.model.Invoice;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Service;

@Slf4j
@RequiredArgsConstructor
@Service
@Profile("invoice-producer")
public class WorkflowService {

    private final InvoiceProducerService invoiceProducerService;

    public String startWorkflow() {
        Invoice invoice = Invoice.sample();

        log.info("┌─────────────────────────────────────────────────────────────┐");
        log.info("│              XSLT-over-Kafka Workflow Started                │");
        log.info("├─────────────────────────────────────────────────────────────┤");
        log.info("  Invoice ID  : {}", invoice.getInvoiceId());
        log.info("  Vendor      : {}", invoice.getVendor());
        log.info("  Customer    : {}", invoice.getCustomer());
        log.info("  Amount      : {} {}", invoice.getAmount(), invoice.getCurrency());
        log.info("  Status      : {}", invoice.getStatus());
        log.info("└─────────────────────────────────────────────────────────────┘");

        invoiceProducerService.sendInvoice(invoice);

        return String.format(
                "Workflow started. Invoice [%s] sent to 'invoices' topic. " +
                        "Check logs for XSLT transformation result on 'payments' topic.",
                invoice.getInvoiceId()
        );
    }
}
