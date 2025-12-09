
import { AlertCircle, Shield, CheckCircle, Sparkles } from "lucide-react";
import { cn } from "@/lib/utils";

// Define the type for a single clause, matching the source
interface Clause {
  id: string;
  title: string;
  content: string;
  risk: 'safe' | 'caution' | 'danger';
  suggestion?: string;
}

interface DeepAnalysisViewProps {
  clauses: Clause[];
}

const riskConfig = {
  danger: {
    icon: AlertCircle,
    className: "text-destructive border-destructive/40",
    borderClassName: "border-destructive/40",
    title: "Rủi ro cao"
  },
  caution: {
    icon: Shield,
    className: "text-yellow-600 border-yellow-500/40",
    borderClassName: "border-yellow-500/40",
    title: "Cần chú ý"
  },
  safe: {
    icon: CheckCircle,
    className: "text-green-600 border-green-500/40",
    borderClassName: "border-green-500/40",
    title: "An toàn"
  },
};

// This function will generate a short quote from the content.
const generateQuote = (content: string) => {
    const words = content.split(' ');
    if (words.length > 15) {
        return `"${words.slice(0, 15).join(' ')}..."`;
    }
    return `"${content}"`;
}

export function DeepAnalysisView({ clauses }: DeepAnalysisViewProps) {

  if (!clauses || clauses.length === 0) {
    return (
        <div className="text-center py-10">
            <h3 className="mt-2 text-sm font-semibold text-gray-900">Không có dữ liệu</h3>
            <p className="mt-1 text-sm text-gray-500">
              Không có dữ liệu hợp đồng để phân tích.
            </p>
        </div>
    )
  }

  return (
    <div className="space-y-3">
      {clauses.map((item) => {
        const config = riskConfig[item.risk as keyof typeof riskConfig];
        const Icon = config.icon;

        return (
          <div key={item.id} className={cn("bg-card border rounded-lg overflow-hidden", config.borderClassName)}>
            <div className={cn("p-2.5 border-b flex items-center gap-2 font-semibold", config.className, config.borderClassName)}>
              <Icon className="w-4 h-4 flex-shrink-0" />
              <h3 className="flex-1 text-card-foreground text-sm">{item.title}</h3>
            </div>

            <div className="p-3 space-y-3">
              <div>
                <p className="text-[11px] text-muted-foreground mb-1.5">Trích dẫn từ hợp đồng:</p>
                <blockquote className="border-l-4 pl-3 italic text-card-foreground/80 text-xs">
                  {generateQuote(item.content)}
                </blockquote>
              </div>
              
              <div className={cn("p-2.5 rounded-md", {
                'bg-primary/5': item.risk !== 'safe',
                'bg-green-500/5': item.risk === 'safe',
              })}>
                <div className={cn("flex items-center gap-2", {
                  'text-primary': item.risk !== 'safe',
                  'text-green-700': item.risk === 'safe',
                })}>
                  <Sparkles className="w-3.5 h-3.5 flex-shrink-0" />
                  <h4 className="font-semibold text-xs">{item.risk === 'safe' ? 'Nhận xét' : 'Kiến nghị'}</h4>
                </div>
                <p className={cn("mt-1.5 pl-5 text-xs", {
                  'text-primary/90': item.risk !== 'safe',
                  'text-green-800/90': item.risk === 'safe',
                })}>{item.suggestion || "Không có đề xuất."}</p>
              </div>
            </div>
          </div>
        );
      })}
    </div>
  );
}
