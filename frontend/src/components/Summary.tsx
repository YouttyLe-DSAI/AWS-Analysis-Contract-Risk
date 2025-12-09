
import { File, CheckCircle2, AlertTriangle, LucideProps } from "lucide-react";
import React from 'react';
import { useAuth } from "@/contexts/AuthContext";

export function Summary() {
  const { user } = useAuth();

  const totalContracts = user?.inspections?.length || 0;
  const reviewsCompleted = user?.inspections?.filter(i => i.score > -1).length || 0;
  const highRiskFound = user?.inspections?.filter(i => i.score >= 0 && i.score < 60).length || 0;

  const summaryData = [
    {
      title: "Tổng số hợp đồng",
      value: totalContracts,
      Icon: (props: LucideProps) => <File {...props} />,
      iconClassName: "text-gray-500"
    },
    {
      title: "Đã hoàn thành đánh giá",
      value: reviewsCompleted,
      Icon: (props: LucideProps) => <CheckCircle2 {...props} />,
      iconClassName: "text-green-500"
    },
    {
      title: "Rủi ro cao được tìm thấy",
      value: highRiskFound,
      Icon: (props: LucideProps) => <AlertTriangle {...props} />,
      iconClassName: "text-red-500"
    },
  ];

  return (
    <div>
      <h2 className="text-xl font-bold text-gray-800 mb-4">Tóm tắt</h2>
      <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
        {summaryData.map((item, index) => {
            const Icon = item.Icon;
            return (
              <div key={index} className="bg-white p-6 rounded-2xl border border-slate-200/80 shadow-sm flex items-center gap-4">
                <div className={`w-12 h-12 flex items-center justify-center bg-gray-100 rounded-full mr-4 ${item.iconClassName}`}>
                    <Icon className="w-6 h-6" />
                </div>
                <div>
                  <p className="text-2xl font-bold text-gray-800">{item.value}</p>
                  <p className="text-sm text-gray-500">{item.title}</p>
                </div>
              </div>
            )
        })}
      </div>
    </div>
  );
}
