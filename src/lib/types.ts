export type SecurityStatus = "Top Secret" | "Restricted" | "Open" | "Internal";

export interface CampaignBucket {
  id: string;
  name: string;
  role: string;
  securityStatus: SecurityStatus;
  /**
   * Arbitrary CSS color value used to tint the bucket card
   * (e.g. "#22c55e", "rgb(59,130,246)", "hsl(210 100% 56%)").
   */
  themeColor: string;
}


