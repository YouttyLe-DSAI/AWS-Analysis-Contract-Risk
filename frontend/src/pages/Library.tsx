import React, { useState, useMemo } from 'react';
import { useNavigate } from 'react-router-dom';
import { 
  LayoutGrid, List, Search, FileText, Briefcase, Home, Users, Sparkles, Loader2 
} from 'lucide-react';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';
import { cn } from "@/lib/utils";
import { useAuth, UserDraft } from '@/contexts/AuthContext';
import { templateLibrary } from '@/data/template-library';
import { generateContractService } from '@/services/contractService';
import { useToast } from "@/components/ui/use-toast";
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
  DialogDescription,
  DialogFooter,
} from "@/components/ui/dialog";

const getInputsForTemplate = (templateId: string) => {
  // 1. Nhóm Hợp đồng THUÊ
  if (templateId.includes('thue')) {
    return [
      { key: "party_a", label: "Bên Cho Thuê (Bên A)", placeholder: "Họ tên, CCCD/MST..." },
      { key: "party_b", label: "Bên Thuê (Bên B)", placeholder: "Họ tên, CCCD/MST..." },
      { key: "address", label: "Địa chỉ tài sản thuê", placeholder: "Số nhà, đường, phường, quận..." },
      { key: "price", label: "Giá thuê (VNĐ/tháng)", placeholder: "Ví dụ: 10.000.000" },
      { key: "duration", label: "Thời hạn thuê", placeholder: "Ví dụ: 12 tháng" },
      { key: "deposit", label: "Tiền đặt cọc", placeholder: "Ví dụ: 20.000.000" }
    ];
  }

  // 2. Nhóm Hợp đồng MUA BÁN / ĐẶT CỌC
  if (templateId.includes('mua_ban') || templateId.includes('dat_coc')) {
    return [
      { key: "party_a", label: "Bên Bán (Bên A)", placeholder: "Họ tên, CCCD/MST..." },
      { key: "party_b", label: "Bên Mua (Bên B)", placeholder: "Họ tên, CCCD/MST..." },
      { key: "asset_desc", label: "Mô tả tài sản", placeholder: "Diện tích, số tờ, số thửa..." },
      { key: "total_price", label: "Tổng giá trị (VNĐ)", placeholder: "Ví dụ: 2.500.000.000" },
      { key: "deposit_amount", label: "Số tiền đặt cọc (nếu có)", placeholder: "Ví dụ: 100.000.000" }
    ];
  }

  // 3. Nhóm BIÊN BẢN / THANH LÝ
  if (templateId.includes('bien_ban') || templateId.includes('thanh_ly')) {
    return [
      { key: "party_a", label: "Đại diện Bên A", placeholder: "Họ tên..." },
      { key: "party_b", label: "Đại diện Bên B", placeholder: "Họ tên..." },
      { key: "contract_ref", label: "Căn cứ hợp đồng số", placeholder: "Số hợp đồng gốc..." },
      { key: "reason", label: "Lý do thanh lý", placeholder: "Hết hạn hợp đồng / Thỏa thuận..." }
    ];
  }

  // 4. Mặc định
  return [
    { key: "party_a", label: "Bên A", placeholder: "Tên cá nhân/tổ chức..." },
    { key: "party_b", label: "Bên B", placeholder: "Tên cá nhân/tổ chức..." },
    { key: "content_req", label: "Nội dung chính", placeholder: "Mô tả nội dung..." }
  ];
};

export const Library = () => {
  const navigate = useNavigate();
  const { user, updateUserProfile } = useAuth();
  const { toast } = useToast();
  
  const [layout, setLayout] = useState('grid');
  const [searchTerm, setSearchTerm] = useState('');
  const [activeCategory, setActiveCategory] = useState('all');
  
  const [selectedTemplate, setSelectedTemplate] = useState<any>(null);
  const [formData, setFormData] = useState<Record<string, string>>({});
  const [isGenerating, setIsGenerating] = useState(false);

  const categories = useMemo(() => {
    const allCats = templateLibrary.map(t => t.category);
    const uniqueCats = [...new Set(allCats)];
    return [{ id: 'all', name: 'Tất cả' }, ...uniqueCats.map(cat => ({ id: cat, name: cat }))];
  }, []);

  const filteredTemplates = templateLibrary.filter(template => {
    const matchesCategory = activeCategory === 'all' || template.category === activeCategory;
    const matchesSearch = template.name.toLowerCase().includes(searchTerm.toLowerCase());
    return matchesCategory && matchesSearch;
  });

  const getCategoryIcon = (categoryName: string) => {
    switch(categoryName) {
        case 'Kinh doanh': return Briefcase;
        case 'Bất động sản': return Home;
        case 'Nhân sự': return Users;
        default: return FileText;
    }
  }

  const handleInputChange = (key: string, value: string) => {
    setFormData(prev => ({ ...prev, [key]: value }));
  };

  const handleGenerateContract = async () => {
    if (!selectedTemplate || !user) return;
    setIsGenerating(true);

    try {
      const response = await generateContractService(selectedTemplate.id, {
        ...formData,
        notes_for_lawyer_ai: `Hãy soạn thảo ${selectedTemplate.name} dựa trên thông tin được cung cấp.`
      });
      
      const contentHtml = response.data?.contentHtml || selectedTemplate.content;
      const docName = `${selectedTemplate.name} - ${new Date().toLocaleDateString('vi-VN')}`;

      const newDraft: UserDraft = {
        id: `draft_${Date.now()}`,
        originalTemplateId: selectedTemplate.id,
        name: docName,
        content: contentHtml,
        lastSaved: new Date().toISOString(),
      };

      const updatedTemplates = [newDraft, ...user.templates];
      await updateUserProfile({ templates: updatedTemplates });

      toast({ title: "Thành công!", description: "Đang chuyển hướng đến trình soạn thảo..." });
      
      setTimeout(() => {
        setIsGenerating(false);
        setSelectedTemplate(null);
        navigate(`/templates/edit/${newDraft.id}`);
      }, 1000);

    } catch (error) {
      console.error("Generate Error:", error);
      toast({ title: "Lỗi", description: "Không thể tạo hợp đồng lúc này.", variant: "destructive" });
      setIsGenerating(false);
    }
  };

  const currentInputs = selectedTemplate ? getInputsForTemplate(selectedTemplate.id) : [];

  return (
    <div className="p-6 max-w-[1400px] mx-auto font-sans">
      <div className="text-center mb-8">
        <h1 className="text-4xl font-bold text-gray-800">Thư viện Mẫu Hợp đồng</h1>
        <p className="text-lg text-gray-500 mt-2">Chọn mẫu và AI sẽ giúp bạn soạn thảo chi tiết.</p>
      </div>

      <div className="flex flex-wrap items-center justify-between gap-4 mb-6">
        <div className="relative w-full md:w-80">
          <Search className="absolute left-3 top-1/2 -translate-y-1/2 text-gray-400 w-5 h-5" />
          <Input 
            placeholder="Tìm kiếm mẫu..." 
            className="pl-10 h-11" 
            value={searchTerm}
            onChange={(e) => setSearchTerm(e.target.value)}
          />
        </div>
        <div className="flex items-center bg-gray-100 p-1 rounded-lg">
          <Button variant={layout === 'grid' ? 'secondary' : 'ghost'} size="icon" onClick={() => setLayout('grid')}><LayoutGrid /></Button>
          <Button variant={layout === 'list' ? 'secondary' : 'ghost'} size="icon" onClick={() => setLayout('list')}><List /></Button>
        </div>
      </div>
      
      <div className="flex flex-col md:flex-row gap-8">
        <div className="w-full md:w-56 space-y-2">
            {categories.map(cat => (
              <button 
                  key={cat.id}
                  onClick={() => setActiveCategory(cat.id)}
                  className={cn(
                      "w-full flex items-center gap-3 px-3 py-2.5 rounded-lg text-sm font-medium transition-colors",
                      activeCategory === cat.id ? "bg-[#4f46e5]/10 text-[#4f46e5]" : "hover:bg-gray-100 text-gray-600"
                  )}
              >
                  {cat.id === 'all' ? <LayoutGrid size={18} /> : React.createElement(getCategoryIcon(cat.name), { size: 18 })}
                  {cat.name}
              </button>
            ))}
        </div>

        <div className="flex-1">
          <div className={cn("grid gap-5", layout === 'grid' ? "grid-cols-1 sm:grid-cols-2 lg:grid-cols-3" : "grid-cols-1")}>
            {filteredTemplates.map((template) => {
              const Icon = getCategoryIcon(template.category);
              return (
                <div key={template.id} className={cn("bg-white border rounded-lg overflow-hidden transition-all hover:shadow-md flex flex-col", layout === 'list' && 'flex-row')}>
                  <div className={cn("p-5 border-b flex-grow", layout === 'list' && 'border-b-0 border-r w-2/3')}>
                     <div className='flex items-start gap-4'>
                        <div className='w-12 h-12 bg-[#4f46e5]/10 text-[#4f46e5] rounded-lg flex items-center justify-center shrink-0'><Icon size={24} /></div>
                        <div><h3 className="text-lg font-semibold text-gray-800 truncate">{template.name}</h3><p className="text-sm text-gray-500 mt-1">{template.category}</p></div>
                     </div>
                     <p className="text-sm text-gray-600 mt-4 h-12 line-clamp-2">{template.description}</p>
                  </div>
                   <div className={cn("p-4 bg-gray-50/70 border-t mt-auto", layout === 'list' && 'border-t-0 pl-0 w-1/3 flex items-center')}>
                     <Button onClick={() => { setSelectedTemplate(template); setFormData({}); }} className="w-full bg-[#4f46e5] hover:bg-[#4338ca] text-white">Sử dụng mẫu</Button>
                   </div>
                </div>
              );
            })}
          </div>
        </div>
      </div>

      <Dialog open={!!selectedTemplate} onOpenChange={(isOpen) => !isOpen && !isGenerating && setSelectedTemplate(null)}>
        <DialogContent className="max-w-xl">
          <DialogHeader>
            <DialogTitle className="text-2xl flex items-center gap-2">
               <Sparkles className="text-indigo-600 w-5 h-5"/> 
               {selectedTemplate?.name}
            </DialogTitle>
            <DialogDescription>
               Nhập thông tin bên dưới để AI tự động điền vào hợp đồng.
            </DialogDescription>
          </DialogHeader>

          <div className="py-4 space-y-4 max-h-[60vh] overflow-y-auto px-1">
             {currentInputs.map((field) => (
                <div key={field.key} className="space-y-1.5">
                   <Label className="text-gray-700">{field.label}</Label>
                   <Input 
                      placeholder={field.placeholder}
                      value={formData[field.key] || ''}
                      onChange={(e) => handleInputChange(field.key, e.target.value)}
                      disabled={isGenerating}
                   />
                </div>
             ))}
          </div>

          <DialogFooter className="gap-2">
            <Button variant="outline" onClick={() => setSelectedTemplate(null)} disabled={isGenerating}>Hủy</Button>
            <Button onClick={handleGenerateContract} disabled={isGenerating} className="bg-[#4f46e5] hover:bg-[#4338ca] min-w-[140px]">
               {isGenerating ? (
                  <><Loader2 className="mr-2 h-4 w-4 animate-spin"/> Đang xử lý...</>
               ) : (
                  <><Sparkles className="mr-2 h-4 w-4"/> Tạo Hợp Đồng</>
               )}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </div>
  );
};