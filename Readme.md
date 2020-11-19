## Sample Usage


# Annotate your Entities ID Field with...
~~~~
@Id
@GeneratedValue(strategy = GenerationType.TABLE, generator = "MyEntity_ID_Generator")
@GenericGenerator(
    name = "MyEntity_ID_Generator",
    strategy = "com.tickerfit.domain.utility.PrefixedEntitySequenceIdGenerator",
    parameters = {
        @Parameter(name = PrefixedEntitySequenceIdGenerator.TABLE_PARAM, value = "MySequencesTable"),
        @Parameter(name = PrefixedEntitySequenceIdGenerator.NUMBER_FORMAT_PARAM, value = "%05d")})
private String referenceId;
~~~~

# Implement the Identifier interface on your Entity
~~~~
public class MyPersonEntity implements EntityGroupingIdentifier {
...

@Override
public String getEntityReferenceGroupPrefix() {

    // e.g. gender=="MALE"?"MAN":"WOMAN"
    // Entities will now have ids like MAN-00001,MAN-00002... and WOMAN-00001,WOMAN-00002
    
    // Note: if multiple different entity classes return the same EntityReferenceGroupPrefix(s) 
    // they will also share the same id generation index and so id's generated for any given 
    // entity class may not be contiguous in this case.
    return someInstanceSpecificGroupingValue; 
}
~~~~

