package com.hammer.proiecttae.model;

import jakarta.xml.bind.annotation.XmlAccessType;
import jakarta.xml.bind.annotation.XmlAccessorType;
import jakarta.xml.bind.annotation.XmlElement;
import jakarta.xml.bind.annotation.XmlRootElement;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.time.LocalDate;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@XmlRootElement(name = "Invoice")
@XmlAccessorType(XmlAccessType.FIELD)
public class Invoice {

    @XmlElement(name = "invoiceId", required = true)
    private String invoiceId;

    @XmlElement(name = "issueDate", required = true)
    private String issueDate;

    @XmlElement(name = "vendor", required = true)
    private String vendor;

    @XmlElement(name = "customer", required = true)
    private String customer;

    @XmlElement(name = "amount", required = true)
    private BigDecimal amount;

    @XmlElement(name = "currency", required = true)
    private String currency;

    @XmlElement(name = "description")
    private String description;

    @XmlElement(name = "status", required = true)
    private String status;

    public static Invoice sample() {
        return Invoice.builder()
                .invoiceId("INV-" + System.currentTimeMillis())
                .issueDate(LocalDate.now().toString())
                .vendor("ACME Corp")
                .customer("John Doe")
                .amount(new BigDecimal("1500.00"))
                .currency("EUR")
                .description("Software license Q2 2024")
                .status("PENDING")
                .build();
    }

}
