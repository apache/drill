package org.elasticsearch.hadoop.serialization.builder;

// Optional interface used by readers interested in knowing when a document hit starts and ends
// and also when metadata is being parsed vs the document core

// extended to provide event info about certain special events such as handling geo data
public interface ValueParsingCallback {

	void beginDoc();

	void beginLeadMetadata();

	void endLeadMetadata();

	void beginSource();
	
	void endSource();

	void beginTrailMetadata();
	
	void endTrailMetadata();
	
	void endDoc();

    // Geo fields are special
    // add hooks for them so the parser is aware
    void beginGeoField();

    void endGeoField();
}
