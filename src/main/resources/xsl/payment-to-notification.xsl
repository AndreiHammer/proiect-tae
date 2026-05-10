<?xml version="1.0" encoding="UTF-8"?>
<!--
  payment-to-notification.xsl
  ─────────────────────────────────────────────────────────────────────────────
  Transforms a <Payment> XML message (consumed from the 'payments' Kafka topic)
  into a <Notification> XML message produced to the 'notifications' topic.

  Transformation rules:
    - notificationId  → "NOTIF-" + paymentId
    - notifiedAt      → paymentDate (when the payment was processed)
    - recipient       → payer (the customer is notified of their payment)
    - subject         → "Payment confirmation for " + reference
    - amount          → netAmount
    - currency        → currency (unchanged)
    - status          → paymentStatus
    - sourcePaymentId → paymentId (traceability back to the payment)
  ─────────────────────────────────────────────────────────────────────────────
-->
<xsl:stylesheet version="1.0"
                xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
    <xsl:output method="xml" indent="yes" encoding="UTF-8"/>

    <xsl:template match="/Payment">
        <Notification>
            <!-- Derive notification ID from payment ID -->
            <notificationId>
                <xsl:value-of select="concat('NOTIF-', paymentId)"/>
            </notificationId>

            <!-- Notification timestamp follows the payment processing date -->
            <notifiedAt>
                <xsl:value-of select="paymentDate"/>
            </notifiedAt>

            <!-- The payer (customer) is the notification recipient -->
            <recipient>
                <xsl:value-of select="payer"/>
            </recipient>

            <!-- Subject is a human-readable confirmation line -->
            <subject>
                <xsl:value-of select="concat('Payment confirmation for ', reference)"/>
            </subject>

            <!-- Monetary details pass through unchanged -->
            <amount>
                <xsl:value-of select="netAmount"/>
            </amount>

            <currency>
                <xsl:value-of select="currency"/>
            </currency>

            <!-- Mirror the payment status into the notification -->
            <status>
                <xsl:value-of select="paymentStatus"/>
            </status>

            <!-- Preserve payment ID for traceability -->
            <sourcePaymentId>
                <xsl:value-of select="paymentId"/>
            </sourcePaymentId>

        </Notification>
    </xsl:template>

</xsl:stylesheet>