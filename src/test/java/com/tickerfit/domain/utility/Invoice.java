package com.tickerfit.domain.utility;

import jakarta.persistence.*;
import org.hibernate.annotations.GenericGenerator;
import org.hibernate.annotations.Parameter;

@Entity
public class Invoice implements EntityGroupingIdentifier {
    @Id
    @GenericGenerator(
            name = "Invoice_ID_Generator",
            strategy = "com.tickerfit.domain.utility.PrefixedEntitySequenceIdGenerator",
            parameters = {
                    @Parameter(name = PrefixedEntitySequenceIdGenerator.TABLE_PARAM, value = "MySequenceTable"),
                    @Parameter(name = PrefixedEntitySequenceIdGenerator.NUMBER_FORMAT_PARAM, value = "%05d")})
    @GeneratedValue(strategy = GenerationType.TABLE, generator = "Invoice_ID_Generator")
    private String id;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    @Override
    public String getEntityReferenceGroupPrefix() {
        return "INV";
    }
}
