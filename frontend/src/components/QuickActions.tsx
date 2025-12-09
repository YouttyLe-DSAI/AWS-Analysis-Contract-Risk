
import { Link } from "react-router-dom";
import { Zap, Search, Plus, FileText } from "lucide-react";

const actions = [
  {
    icon: Zap,
    title: "Quick Review",
    description: "Scan PDF/Docx instantly",
    path: "/deep-analysis",
  },
  {
    icon: Search,
    title: "Deep Analysis",
    description: "Detailed risk report",
    path: "/deep-analysis",
  },
  {
    icon: Plus,
    title: "Create Contract",
    description: "Use AI templates",
    path: "/templates",
  },
  {
    icon: FileText,
    title: "Templates Library",
    description: "Browse standard forms",
    path: "/library",
  },
];

export function QuickActions() {
  return (
    <div>
      <h2 className="text-xl font-bold mb-4">Quick Actions</h2>
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
        {actions.map((action, index) => (
          <Link to={action.path} key={index} className="group block hover:no-underline">
            <div className="p-6 bg-card border rounded-lg h-full text-left group-hover:border-indigo-600 group-hover:bg-muted/30 transition-colors duration-200">
              <div className="mb-4 p-2 bg-indigo-100 rounded-lg inline-block">
                <action.icon className="w-6 h-6 text-indigo-600" />
              </div>
              <h3 className="font-semibold text-card-foreground text-lg mb-1">{action.title}</h3>
              <p className="text-sm text-muted-foreground">{action.description}</p>
            </div>
          </Link>
        ))}
      </div>
    </div>
  );
}
