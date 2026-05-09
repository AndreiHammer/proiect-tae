<?xml version="1.0" encoding="UTF-8"?>
<!--
  invoice-to-payment.xsl
  ─────────────────────────────────────────────────────────────────────────────
  Transforms an <Invoice> XML message (consumed from the 'invoices' Kafka topic)
  into a <Payment> XML message that is then produced to the 'payments' topic.

  Transformation rules:
    - paymentId   → "PAY-" + invoiceId
    - paymentDate → today (issueDate passed through)
    - payee       → vendor
    - payer       → customer
    - netAmount   → amount
    - currency    → currency (unchanged)
    - reference   → description
    - paymentStatus → "APPROVED" (business rule: all invoices auto-approved)
  ─────────────────────────────────────────────────────────────────────────────
-->
<xsl:stylesheet version="1.0"
                xmlns:xsl="http://www.w3.org/1999/XSL/Transform">

    <xsl:output method="xml" indent="yes" encoding="UTF-8"/>

    <!-- Root template: match the Invoice element -->
    <xsl:template match="/Invoice">
        <Payment>
            <!-- Derive a payment ID from the invoice ID -->
            <paymentId>
                <xsl:value-of select="concat('PAY-', invoiceId)"/>
            </paymentId>

            <!-- Payment date follows the invoice issue date -->
            <paymentDate>
                <xsl:value-of select="issueDate"/>
            </paymentDate>

            <!-- The vendor receives the payment -->
            <payee>
                <xsl:value-of select="vendor"/>
            </payee>

            <!-- The customer is the payer -->
            <payer>
                <xsl:value-of select="customer"/>
            </payer>

            <!-- Monetary details pass through unchanged -->
            <netAmount>
                <xsl:value-of select="amount"/>
            </netAmount>

            <currency>
                <xsl:value-of select="currency"/>
            </currency>

            <!-- Description becomes the payment reference -->
            <reference>
                <xsl:value-of select="description"/>
            </reference>

            <!-- Business rule: all forwarded invoices are auto-approved -->
            <paymentStatus>APPROVED</paymentStatus>

            <!-- Preserve original invoice ID for traceability -->
            <sourceInvoiceId>
                <xsl:value-of select="invoiceId"/>
            </sourceInvoiceId>
        </Payment>
    </xsl:template>

</xsl:stylesheet>
