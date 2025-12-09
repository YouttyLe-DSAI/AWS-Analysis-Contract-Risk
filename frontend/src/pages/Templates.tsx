import React, { useState } from 'react';
import { useNavigate } from 'react-router-dom';
import { 
  FileText, Plus, Search, MoreHorizontal, Edit, Trash2
} from 'lucide-react';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu";
import { useAuth } from '@/contexts/AuthContext';
import { useToast } from "@/components/ui/use-toast";
import { formatDistanceToNow } from 'date-fns';
import { vi } from 'date-fns/locale';

export const Templates = () => {
  const navigate = useNavigate();
  const { user, updateUserProfile } = useAuth(); 
  const { toast } = useToast();
  
  const [searchTerm, setSearchTerm] = useState("");

  const handleDelete = async (draftId: string) => {
    if (!user || !window.confirm("Bạn có chắc muốn xóa bản nháp này?")) return;

    const updatedTemplates = user.templates.filter(t => t.id !== draftId);
    const { error } = await updateUserProfile({ templates: updatedTemplates });

    if (!error) {
      toast({ title: "Đã xóa bản nháp!" });
    } else {
      toast({ title: "Lỗi", description: "Không thể xóa bản nháp.", variant: "destructive" });
    }
  };

  const filteredTemplates = user?.templates.filter(template =>
    template.name.toLowerCase().includes(searchTerm.toLowerCase())
  ) || [];

  return (
    <>
      <div className="p-6 max-w-[1400px] mx-auto font-sans text-slate-700">
        
        <div className="flex justify-between items-center mb-8">
          <div>
              <h1 className="text-2xl font-bold text-gray-800">Các bản nháp của tôi</h1>
              <p className="text-gray-500 text-sm mt-1">Quản lý các hợp đồng bạn đang soạn thảo.</p>
          </div>
          <Button onClick={() => navigate('/library')} className="bg-indigo-600 hover:bg-indigo-700 text-white gap-2">
              <Plus size={16} /> Tạo hợp đồng mới
          </Button>
        </div>

        <div className="flex items-center gap-4 mb-6 bg-white p-2 rounded-xl border border-slate-200/80 shadow-sm">
          <div className="relative flex-1">
              <Search className="absolute left-3.5 top-1/2 -translate-y-1/2 text-slate-400 w-4 h-4" />
              <Input 
                  placeholder="Tìm kiếm bản nháp..." 
                  className="pl-9 border-none shadow-none focus-visible:ring-0 bg-transparent h-10" 
                  value={searchTerm}
                  onChange={(e) => setSearchTerm(e.target.value)}
              />
          </div>
        </div>

        <div className="bg-white border border-slate-200/80 rounded-2xl shadow-sm overflow-hidden">
          <table className="w-full text-left text-sm">
              <thead className="bg-slate-50 border-b border-slate-200/80">
                  <tr>
                      <th className="px-6 py-4 font-semibold text-slate-600">Tên bản nháp</th>
                      <th className="px-6 py-4 font-semibold text-slate-600 w-48">Lần cuối lưu</th>
                      <th className="px-6 py-4 font-semibold text-slate-600 w-24 text-right">Hành động</th>
                  </tr>
              </thead>
              {filteredTemplates.length > 0 && (
                <tbody className="divide-y divide-slate-100">
                    {filteredTemplates.map((draft) => (
                        <tr key={draft.id} className="hover:bg-slate-50/50 transition-colors group">
                            <td className="px-6 py-4">
                                <div className="flex items-center gap-3">
                                    <div className="w-8 h-8 bg-indigo-50 text-indigo-600 rounded-lg flex items-center justify-center shrink-0">
                                        <FileText size={16} />
                                    </div>
                                    <span className="font-medium text-slate-800 cursor-pointer hover:text-indigo-600" onClick={() => navigate(`/templates/edit/${draft.id}`)}>
                                        {draft.name}
                                    </span>
                                </div>
                            </td>
                            <td className="px-6 py-4 text-slate-500">
                                {formatDistanceToNow(new Date(draft.lastSaved), { addSuffix: true, locale: vi })}
                            </td>
                            <td className="px-6 py-4 text-right">
                                <DropdownMenu>
                                    <DropdownMenuTrigger asChild>
                                        <Button variant="ghost" size="icon" className="text-slate-400 hover:text-slate-600 data-[state=open]:bg-slate-100">
                                            <MoreHorizontal size={18} />
                                        </Button>
                                    </DropdownMenuTrigger>
                                    <DropdownMenuContent align="end">
                                        <DropdownMenuItem onClick={() => navigate(`/templates/edit/${draft.id}`)}>
                                            <Edit className="mr-2 h-4 w-4" /> Chỉnh sửa
                                        </DropdownMenuItem>
                                        
                                        {/* 👇 ĐÃ XÓA MỤC "LƯU VÀO INSPECTIONS" TẠI ĐÂY */}

                                        <DropdownMenuItem onClick={() => handleDelete(draft.id)} className="text-red-500 focus:text-red-500">
                                            <Trash2 className="mr-2 h-4 w-4" /> Xóa
                                        </DropdownMenuItem>
                                    </DropdownMenuContent>
                                </DropdownMenu>
                            </td>
                        </tr>
                    ))}
                </tbody>
              )}
          </table>
          {filteredTemplates.length === 0 && (
              <div className="p-16 text-center text-slate-500">
                <div className="mx-auto w-16 h-16 bg-slate-100 rounded-full flex items-center justify-center mb-4">
                  <FileText size={32} className="text-slate-400"/>
                </div>
                <h3 className="font-semibold text-lg">Chưa có bản nháp nào</h3>
                <p className="text-sm mt-1 mb-4">Hãy bắt đầu bằng cách tạo một hợp đồng mới từ thư viện.</p>
                <Button onClick={() => navigate('/library')} className="bg-indigo-600 hover:bg-indigo-700 text-white gap-2">
                  Đi tới thư viện
                </Button>
              </div>
          )}
        </div>
      </div>
    </>
  );
};

export default Templates;