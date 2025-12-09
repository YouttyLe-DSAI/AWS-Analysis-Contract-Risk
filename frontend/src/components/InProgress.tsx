
import { FileText, CheckCircle, ShieldAlert, FileWarning, ShieldCheck } from "lucide-react";
import { Button } from "@/components/ui/button";
import { useAuth } from "@/contexts/AuthContext";
import { useNavigate } from "react-router-dom";
import { UserInspection } from "@/contexts/AuthContext";
import { formatDistanceToNow } from 'date-fns';
import { vi } from 'date-fns/locale';
import { cn } from '@/lib/utils';

const getStatusIcon = (score: number) => {
    if (score < 0) return { Icon: FileText, color: "text-gray-500", bgColor: "bg-gray-100" };
    if (score >= 80) return { Icon: CheckCircle, color: "text-green-600", bgColor: "bg-green-50" };
    if (score >= 60) return { Icon: FileWarning, color: "text-yellow-600", bgColor: "bg-yellow-50" };
    return { Icon: ShieldAlert, color: "text-red-600", bgColor: "bg-red-50" };
};

const getScoreDisplay = (score: number) => {
    if (score < 0) {
      return <span className="font-medium text-slate-500 flex items-center gap-2"><ShieldAlert size={16}/>Chưa phân tích</span>;
    }
    const scoreColor = score >= 80 ? 'text-green-600' : score >= 60 ? 'text-amber-600' : 'text-red-600';
    return <span className={cn('font-bold flex items-center gap-2', scoreColor)}><ShieldCheck size={16}/>{score}%</span>;
};

export function InProgress() {
  const { user } = useAuth();
  const navigate = useNavigate();

  const recentDocuments = user?.inspections
    ?.sort((a, b) => new Date(b.createdAt).getTime() - new Date(a.createdAt).getTime())
    .slice(0, 5) || [];

  const handleRowClick = (inspection: UserInspection) => {
    if (inspection.analysisData) {
        navigate('/deep-analysis', { 
            state: { 
              analysisData: inspection.analysisData, 
              fileName: inspection.name 
            } 
        });
    }
  }

  return (
    <div>
      <div className="flex justify-between items-center mb-4">
        <h2 className="text-xl font-bold text-gray-800">Đánh giá gần đây</h2>
        <Button variant="link" className="text-indigo-600 hover:underline" onClick={() => navigate("/inspections")}>
          Xem tất cả
        </Button>
      </div>
      <div className="bg-white border border-slate-200/80 rounded-2xl shadow-sm overflow-hidden">
        {recentDocuments.length === 0 ? (
          <div className="text-center p-8 text-gray-500">
            <FileText className="w-12 h-12 mx-auto mb-4 text-gray-300" />
            <p className="font-semibold">Không có đánh giá nào</p>
            <p className="text-sm">Bắt đầu bằng cách tải lên một hợp đồng để phân tích.</p>
            <Button className="mt-4" onClick={() => navigate("/deep-analysis")} size="sm">Phân tích ngay</Button>
          </div>
        ) : (
          <table className="w-full text-left text-sm">
              <thead className="bg-slate-50 border-b border-slate-200/80">
                  <tr>
                      <th className="px-6 py-3 font-semibold text-slate-600">Tên hợp đồng</th>
                      <th className="px-6 py-3 font-semibold text-slate-600 w-40">Điểm rủi ro</th>
                      <th className="px-6 py-3 font-semibold text-slate-600 w-40">Lần cập nhật cuối</th>
                  </tr>
              </thead>
              <tbody className="divide-y divide-slate-100">
                  {recentDocuments.map((doc: UserInspection) => {
                      const status = getStatusIcon(doc.score);
                      const StatusIcon = status.Icon;
                      return (
                          <tr key={doc.id} onClick={() => handleRowClick(doc)} className="hover:bg-slate-50/50 transition-colors group cursor-pointer">
                              <td className="px-6 py-3">
                                  <div className="flex items-center gap-3">
                                      <div className={cn("w-8 h-8 rounded-lg flex items-center justify-center shrink-0", status.bgColor)}>
                                          <StatusIcon size={16} className={status.color} />
                                      </div>
                                      <span className="font-medium text-slate-800 group-hover:text-indigo-600">{doc.name}</span>
                                  </div>
                              </td>
                              <td className="px-6 py-3 text-slate-500">{getScoreDisplay(doc.score)}</td>
                              <td className="px-6 py-3 text-slate-500">{formatDistanceToNow(new Date(doc.createdAt), { addSuffix: true, locale: vi })}</td>
                          </tr>
                      )
                  })}
              </tbody>
          </table>
        )}
      </div>
    </div>
  );
}
