package com.hammer.proiecttae.model;

import jakarta.xml.bind.annotation.XmlAccessType;
import jakarta.xml.bind.annotation.XmlAccessorType;
import jakarta.xml.bind.annotation.XmlElement;
import jakarta.xml.bind.annotation.XmlRootElement;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@XmlRootElement(name = "Notification")
@XmlAccessorType(XmlAccessType.FIELD)
public class Notification {

    @XmlElement(name = "notificationId", required = true)
    private String notificationId;

    @XmlElement(name = "notifiedAt", required = true)
    private String notifiedAt;

    @XmlElement(name = "recipient", required = true)
    private String recipient;

    @XmlElement(name = "subject", required = true)
    private String subject;

    @XmlElement(name = "amount", required = true)
    private String amount;

    @XmlElement(name = "currency", required = true)
    private String currency;

    @XmlElement(name = "status", required = true)
    private String status;

    @XmlElement(name = "sourcePaymentId", required = true)
    private String sourcePaymentId;
}
