import { PageLayout } from "@/components/PageLayout";
import { WelcomeBanner } from "@/components/WelcomeBanner";
import { QuickActions } from "@/components/QuickActions";
import { Summary } from "@/components/Summary";
import { InProgress } from "@/components/InProgress";

export function Dashboard() {
  return (
    <PageLayout>
      <div className="p-6 space-y-6">
        <WelcomeBanner />
        <QuickActions />
        <Summary />
        <InProgress />
      </div>
    </PageLayout>
  );
}
