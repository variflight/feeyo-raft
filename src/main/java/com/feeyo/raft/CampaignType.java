package com.feeyo.raft;

import java.util.Arrays;

// Possible values for CampaignType
public enum CampaignType {

	/**
     * campaignPreElection represents the first phase of a normal election when
     * Config.PreVote is true.
     */
	CAMPAIGN_PRE_ELECTION( "CampaignPreElection".getBytes() ), 
	
	/**
     * campaignElection represents a normal (time-based) election (the second phase
     * of the election when Config.PreVote is true).
     */
	CAMPAIGN_ELECTION( "CampaignElection".getBytes() ), 
	
	 /**
     * campaignTransfer represents the type of leader transfer
     */
	CAMPAIGN_TRANSFER( "CampaignTransfer".getBytes() );
	
	//
	public boolean isCampaignPreElection() {
		return this == CAMPAIGN_PRE_ELECTION;
	}

	public boolean isCampaignElection() {
		return this == CAMPAIGN_ELECTION;
	}

	public boolean isCampaignTransfer() {
		return this == CAMPAIGN_TRANSFER;
	}
	
    private byte[] value;

    private CampaignType(byte[] value){
        this.value = value;
    }

	public byte[] getValue() {
		return value;
	}
	
	public static CampaignType valuesOf(byte[] value) {
		CampaignType[] types = values();
        for (CampaignType type : types) {
            if ( Arrays.equals(type.value, value) ) {
                return type;
            }
        }
        return null;
    }

}