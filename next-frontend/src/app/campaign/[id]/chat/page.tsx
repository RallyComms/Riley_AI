import { redirect } from "next/navigation";

export default async function LegacyCampaignChatRedirect({
  params,
}: {
  params: { id: string };
}) {
  redirect(`/campaign/${params.id}/conversations`);
}
