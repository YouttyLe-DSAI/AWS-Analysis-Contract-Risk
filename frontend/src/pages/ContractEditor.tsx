import React, { useState, useEffect, useCallback, useRef } from 'react';
import { useParams, useNavigate } from 'react-router-dom';
import { useAuth, UserDraft, UserInspection } from '@/contexts/AuthContext';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { File as FileIcon, Save, Trash2, X, Wand2, Check, Download, Loader2 } from 'lucide-react';
import { useToast } from "@/components/ui/use-toast";
import { chatContractService, uploadContractService, aiAssistService } from '@/services/contractService';
import { cn } from '@/lib/utils';
import {
  AlertDialog,
  AlertDialogCancel,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogTitle,
} from "@/components/ui/alert-dialog";
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu";

const wait = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));
const RichTextEditor = ({ value, onChange }: { value: string, onChange: (value: string) => void }) => {
  const editorRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    if (editorRef.current && editorRef.current.innerHTML !== value) {
      editorRef.current.innerHTML = value;
    }
  }, [value]);

  return (
    <div className="w-full flex-1 min-h-0 border rounded-lg bg-white p-4 overflow-y-auto shadow-sm">
      <div 
        ref={editorRef}
        contentEditable 
        suppressContentEditableWarning
        className="outline-none prose max-w-none prose-p:my-2 prose-headings:my-4"
        onInput={(e) => onChange(e.currentTarget.innerHTML)}
        onBlur={(e) => onChange(e.currentTarget.innerHTML)}
      />
    </div>
  );
};

const AiPanel = ({ onGenerate, onClose }: { onGenerate: (text: string) => void, onClose: () => void }) => {
    const [prompt, setPrompt] = useState('');
    const [isLoading, setIsLoading] = useState(false); 

    const handleGenerate = async () => {
      if (!prompt.trim()) return;
      setIsLoading(true);

      try {
        const response = await aiAssistService( 
           `Hãy đóng vai luật sư, soạn thảo một điều khoản hợp đồng về: "${prompt}". 
            Yêu cầu: Viết ngắn gọn, dùng thẻ HTML (<p>, <ul>, <li>, <b>) để định dạng.`);
        
        const aiText = response.answer || "<p>AI không phản hồi.</p>";
        
        onGenerate(aiText); 
        setPrompt('');
      } catch (e) {
        console.error(e);
      } finally {
        setIsLoading(false);
      }
    };

    return (
      <div className="absolute top-0 right-0 h-full w-[400px] bg-white border-l shadow-2xl z-20 flex flex-col p-4 transition-transform duration-300 ease-in-out">
        <div className="flex items-center justify-between border-b pb-3 mb-4">
          <h2 className="font-semibold text-lg flex items-center gap-2"><Wand2 size={20}/> Trợ lý AI</h2>
          <Button variant="ghost" size="icon" onClick={onClose}><X size={20}/></Button>
        </div>
        <div className="flex-1 flex flex-col">
          <p className="text-sm text-slate-600 mb-4">Nhập yêu cầu để AI viết thêm điều khoản cho bạn.</p>
          <textarea 
            value={prompt} 
            onChange={(e) => setPrompt(e.target.value)} 
            placeholder="Ví dụ: Viết điều khoản phạt nếu thanh toán chậm..."
            className="w-full flex-1 p-3 border rounded-md resize-none focus:ring-2 focus:ring-indigo-500 outline-none text-sm" 
            disabled={isLoading}
          />
          <Button 
            onClick={handleGenerate} 
            disabled={isLoading || !prompt.trim()}
            className="mt-4 bg-indigo-600 hover:bg-indigo-700"
          >
            {isLoading ? (
               <><Loader2 size={16} className="mr-2 animate-spin"/> Đang viết...</>
            ) : (
               <><Wand2 size={16} className="mr-2"/> Tạo nội dung</>
            )}
          </Button>
        </div>
      </div>
    );
}

export const ContractEditor = () => {
  const { draftId } = useParams<{ draftId: string }>();
  const navigate = useNavigate();
  const { user, updateUserProfile, refreshUser, loading } = useAuth();
  const { toast } = useToast();

  const [draft, setDraft] = useState<UserDraft | null>(null);
  const [content, setContent] = useState('');
  const [draftName, setDraftName] = useState('');
  const [isSaveAlertOpen, setIsSaveAlertOpen] = useState(false);
  const [isReady, setIsReady] = useState(false);
  const [isAiPanelOpen, setAiPanelOpen] = useState(false);
  const [saveStatus, setSaveStatus] = useState<'idle' | 'saving' | 'saved'>('idle');
  const [isProcessing, setIsProcessing] = useState(false);

  useEffect(() => {
    if (loading) return;
    const currentDraft = user?.templates.find(t => t.id === draftId);
    if (currentDraft) {
      setDraft(currentDraft);
      setContent(currentDraft.content);
      setDraftName(currentDraft.name);
      setIsReady(true);
    } else {
      navigate('/templates', { replace: true });
    }
  }, [draftId, user?.templates, navigate, loading]);

  const handleSaveDraft = useCallback(async () => {
    if (!user || !draft) return;
    setSaveStatus('saving');
    const updatedDraft: UserDraft = { ...draft, name: draftName, content, lastSaved: new Date().toISOString() };
    const updatedTemplates = user.templates.map(t => t.id === draft.id ? updatedDraft : t);
    const { error } = await updateUserProfile({ templates: updatedTemplates });
    if (!error) {
      setSaveStatus('saved');
      setTimeout(() => setSaveStatus('idle'), 2000);
    } else {
      setSaveStatus('idle');
      toast({ title: "Lỗi", description: "Không thể lưu bản nháp.", variant: 'destructive' });
    }
  }, [user, draft, draftName, content, updateUserProfile, toast]);

  const cleanText = (str: string) => {
    if (!str) return '';
    let result = str;
    try {
        if (/[\u00C0-\u00FF]/.test(result)) { 
             const decoded = decodeURIComponent(escape(result));
             result = decoded;
        }
    } catch (e) {}
    return result.normalize('NFC');
  };

  const handleSaveToInspections = async (deleteDraft: boolean) => {
    if (!user || !draft) return;

    setIsProcessing(true); // Bật loading
    toast({ title: "Đang xử lý...", description: "Đang tạo file PDF và lưu vào hệ thống..." });

    try {
        const name = cleanText(draftName) || 'document';
        const htmlContent = cleanText(content);

        // 1. IMPORT THƯ VIỆN
        const { default: jsPDF } = await import('jspdf');
        const { default: html2canvas } = await import('html2canvas');

        // 2. TẠO GIAO DIỆN TẠM ĐỂ CHỤP ẢNH (Render HTML)
        const tempDiv = document.createElement('div');
        tempDiv.innerHTML = htmlContent;
        
        // CSS giả lập trang giấy in A4
        Object.assign(tempDiv.style, {
            width: '800px',
            padding: '40px',
            background: 'white',
            fontSize: '13pt',
            lineHeight: '1.6',
            color: '#000000',
            fontFamily: "'Times New Roman', serif", 
            position: 'absolute',
            left: '-9999px',
            top: '0'
        });
        document.body.appendChild(tempDiv);
        await wait(100);

        // 3. CHỤP ẢNH DIV
        const canvas = await html2canvas(tempDiv, { 
            scale: 2, 
            useCORS: true, 
            logging: false
        });
        document.body.removeChild(tempDiv);

        // 4. TẠO PDF TỪ ẢNH
        const imgData = canvas.toDataURL('image/jpeg', 0.95);
        const pdf = new jsPDF('p', 'pt', 'a4');
        const pdfWidth = pdf.internal.pageSize.getWidth();
        const pdfHeight = pdf.internal.pageSize.getHeight();
        const ratio = pdfWidth / canvas.width;
        const scaledHeight = canvas.height * ratio;

        let heightLeft = scaledHeight;
        let position = 0;

        pdf.addImage(imgData, 'JPEG', 0, position, pdfWidth, scaledHeight);
        heightLeft -= pdfHeight;

        while (heightLeft > 0) {
            position = heightLeft - scaledHeight;
            pdf.addPage();
            pdf.addImage(imgData, 'JPEG', 0, position, pdfWidth, scaledHeight);
            heightLeft -= pdfHeight;
        }

        // 5. CHUYỂN PDF THÀNH FILE ĐỂ UPLOAD
        const pdfBlob = pdf.output('blob');
        
        // Tạo tên file an toàn (bỏ ký tự đặc biệt)
        const safeName = name.replace(/[^a-zA-Z0-9\u00C0-\u1EF9 ]/g, "_");
        const fileName = `${safeName}.pdf`;

        const pdfFile = new File([pdfBlob], fileName, { type: 'application/pdf' });

        // 6. GỌI API UPLOAD (Backend sẽ lưu S3 và tạo Session trong DB -> F5 không mất)
        await uploadContractService(pdfFile);

        // 7. XÓA BẢN NHÁP 
        if (deleteDraft) {
            const updatedTemplates = user.templates.filter(t => t.id !== draft.id);
            await updateUserProfile({ templates: updatedTemplates });
        }

        // 8. CẬP NHẬT DỮ LIỆU USER ĐỂ ĐỒNG BỘ VỚI BACKEND
        await refreshUser();
        
        toast({ 
            title: "Thành công!", 
            description: `Đã lưu "${fileName}" vào mục Inspections.` 
        });
        
        setIsSaveAlertOpen(false);
        navigate('/inspections');

    } catch (error) {
        console.error("Save PDF Error:", error);
        toast({ 
            title: "Lỗi hệ thống", 
            description: "Không thể tạo file PDF hoặc upload thất bại.", 
            variant: "destructive" 
        });
    } finally {
        setIsProcessing(false);
    }
  }

  const handleDeleteDraft = async () => {
      if (!user || !draft || !window.confirm(`Bạn có chắc muốn xóa vĩnh viễn bản nháp "${draftName}"?`)) return;

      const updatedTemplates = user.templates.filter(t => t.id !== draft.id);
      const { error } = await updateUserProfile({ templates: updatedTemplates });

      if (!error) {
        toast({ title: "Đã xóa bản nháp!" });
        navigate('/templates', { replace: true });
      } else {
        toast({ title: "Lỗi", description: "Không thể xóa bản nháp.", variant: "destructive" });
      }
  }
  
  const handleExport = async (format: 'pdf' | 'docx' | 'html' | 'txt') => {
    const name = cleanText(draftName) || 'document';
    const htmlContent = cleanText(content);

    switch (format) {
      case 'pdf': {
        toast({ title: "Đang xuất PDF...", description: "Đang xử lý font Tiếng Việt..." });
        try {
            const { default: jsPDF } = await import('jspdf');
            const { default: html2canvas } = await import('html2canvas');

            const tempDiv = document.createElement('div');
            tempDiv.innerHTML = htmlContent;
            
            Object.assign(tempDiv.style, {
                width: '800px',
                padding: '40px',
                background: 'white',
                fontSize: '13pt',
                lineHeight: '1.6',
                color: '#000000',
                fontFamily: "'Times New Roman', serif", 
                fontVariantLigatures: 'none',
                position: 'absolute',
                left: '-9999px',
                top: '0'
            });
            document.body.appendChild(tempDiv);

            const canvas = await html2canvas(tempDiv, { 
                scale: 2, 
                useCORS: true, 
                logging: false
            });
            
            document.body.removeChild(tempDiv);

            const imgData = canvas.toDataURL('image/jpeg', 0.95);
            const pdf = new jsPDF('p', 'pt', 'a4');
            const pdfWidth = pdf.internal.pageSize.getWidth();
            const pdfHeight = pdf.internal.pageSize.getHeight();
            const imgWidth = canvas.width;
            const imgHeight = canvas.height;
            const ratio = pdfWidth / imgWidth;
            const scaledHeight = imgHeight * ratio;

            let position = 0;
            let heightLeft = scaledHeight;

            pdf.addImage(imgData, 'JPEG', 0, position, pdfWidth, scaledHeight);
            heightLeft -= pdfHeight;

            while (heightLeft > 0) {
                position = heightLeft - scaledHeight;
                pdf.addPage();
                pdf.addImage(imgData, 'JPEG', 0, position, pdfWidth, scaledHeight);
                heightLeft -= pdfHeight;
            }

            pdf.save(`${name}.pdf`);
            toast({ title: "Đã xuất thành công!", description: `Tệp ${name}.pdf đã được tải xuống.` });
        } catch(e) {
            console.error(e);
            toast({ title: "Lỗi", description: "Không thể xuất ra PDF.", variant: "destructive" });
        } 
        break;
      }

      case 'docx': {
        toast({ title: "Đang xuất Word...", description: "Vui lòng đợi trong giây lát." });
        try {
            const header = `
                <html xmlns:o='urn:schemas-microsoft-com:office:office' 
                      xmlns:w='urn:schemas-microsoft-com:office:word' 
                      xmlns='http://www.w3.org/TR/REC-html40'>
                <head>
                    <meta charset='utf-8'>
                    <title>${name}</title>
                    <style>
                        body, p, div, span, td, th, li { 
                            font-family: 'Times New Roman', serif; 
                            font-size: 13pt; 
                            line-height: 1.5; 
                            color: #000000;
                            mso-ascii-font-family: 'Times New Roman';
                            mso-hansi-font-family: 'Times New Roman';
                            mso-bidi-font-family: 'Times New Roman';
                        }
                        p { margin-bottom: 12pt; }
                        table { border-collapse: collapse; width: 100%; margin-bottom: 12pt; }
                        td, th { border: 1px solid black; padding: 5pt; }
                    </style>
                </head><body>`;
            
            const footer = "</body></html>";
            const sourceHTML = header + htmlContent + footer;
            
            const blob = new Blob(['\ufeff', sourceHTML], { 
                type: 'application/msword;charset=utf-8' 
            });
            
            const url = URL.createObjectURL(blob);
            const a = document.createElement('a');
            a.href = url;
            a.download = `${name}.doc`;
            document.body.appendChild(a);
            a.click();
            document.body.removeChild(a);
            URL.revokeObjectURL(url);
            
            toast({ title: "Đã xuất thành công!", description: `Tệp ${name}.doc đã được tải xuống.` });
        } catch (e) {
            console.error(e);
            toast({ title: "Lỗi", description: "Không thể xuất file Word.", variant: "destructive" });
        }
        break;
      }

      case 'html': {
        const fullHtml = `<!DOCTYPE html><html><head><meta charset="UTF-8"><title>${name}</title></head><body>${htmlContent}</body></html>`;
        const blob = new Blob(['\ufeff', fullHtml], { type: 'text/html;charset=utf-8' });
        const url = URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = url;
        a.download = `${name}.html`;
        document.body.appendChild(a);
        a.click();
        document.body.removeChild(a);
        URL.revokeObjectURL(url);
        toast({ title: "Đã xuất thành công!", description: `Tệp ${name}.html đã được tải xuống.` });
        break;
      }
      
      case 'txt': {
        const tempDiv = document.createElement('div');
        tempDiv.innerHTML = htmlContent;
        const textContent = tempDiv.textContent || tempDiv.innerText || '';
        const blob = new Blob(['\ufeff', textContent], { type: 'text/plain;charset=utf-8' });
        const url = URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = url;
        a.download = `${name}.txt`;
        document.body.appendChild(a);
        a.click();
        document.body.removeChild(a);
        URL.revokeObjectURL(url);
        toast({ title: "Đã xuất thành công!", description: `Tệp ${name}.txt đã được tải xuống.` });
        break;
      }
    }
  };

  const handleAiGenerate = (text: string) => {
    setContent(currentContent => `${currentContent}${text}`);
    setAiPanelOpen(false);
    toast({ title: "Đã thêm nội dung AI!" });
  }

  if (!isReady) return <div className="flex h-screen w-full items-center justify-center">Đang tải...</div>;

  return (
    <div className="h-screen bg-gray-100 flex flex-col relative overflow-hidden">
      <header className="bg-white border-b p-3 flex items-center justify-between shrink-0 z-10">
        <div className="flex items-center gap-3 w-1/2">
          <Button variant="outline" size="icon" onClick={() => navigate('/templates')} className="h-8 w-8"><X size={16}/></Button>
          <FileIcon className="text-indigo-600 shrink-0" />
          <Input value={draftName} onChange={(e) => setDraftName(e.target.value)} className="font-semibold text-lg border-none focus-visible:ring-0 shadow-none p-0 h-auto truncate" />
        </div>
        <div className="flex items-center gap-2">
          <div className="flex items-center gap-2 w-28 pr-2">
            {saveStatus === 'saving' && <span className="text-sm text-slate-500 animate-pulse">Đang lưu...</span>}
            {saveStatus === 'saved' && <span className="text-sm text-green-600 flex items-center gap-1"><Check size={16}/> Đã lưu!</span>}
          </div>
          <Button variant="ghost" size="sm" onClick={handleDeleteDraft} className="text-red-600 hover:text-red-700"><Trash2 className="w-4 h-4 mr-2"/> Xóa</Button>
          <Button variant="outline" size="sm" onClick={handleSaveDraft} disabled={saveStatus === 'saving'}><Save className="w-4 h-4 mr-2"/> Lưu nháp</Button>
          <DropdownMenu>
            <DropdownMenuTrigger asChild>
              <Button variant="outline" size="sm"><Download className="w-4 h-4 mr-2" />Xuất</Button>
            </DropdownMenuTrigger>
            <DropdownMenuContent align="end">
              <DropdownMenuItem onClick={() => handleExport('pdf')}>PDF (.pdf)</DropdownMenuItem>
              <DropdownMenuItem onClick={() => handleExport('docx')}>Word (.docx)</DropdownMenuItem>
              <DropdownMenuItem onClick={() => handleExport('html')}>HTML (.html)</DropdownMenuItem>
              <DropdownMenuItem onClick={() => handleExport('txt')}>Plain Text (.txt)</DropdownMenuItem>
            </DropdownMenuContent>
          </DropdownMenu>
          <Button size="sm" className="bg-indigo-600 hover:bg-indigo-700" onClick={() => setIsSaveAlertOpen(true)}>Hoàn tất & Lưu</Button>
        </div>
      </header>

      <main className={cn("flex-1 p-4 flex flex-col min-h-0 transition-all duration-300 ease-in-out", isAiPanelOpen ? "mr-[400px]" : "mr-0")}>
        <div className="h-full w-full flex flex-col min-h-0 max-w-4xl mx-auto">
          <h2 className="text-sm font-semibold mb-2 text-gray-600 px-1 shrink-0">NỘI DUNG HỢP ĐỒNG</h2>
          <RichTextEditor value={content} onChange={setContent} />
        </div>
      </main>

      {isAiPanelOpen && <AiPanel onGenerate={handleAiGenerate} onClose={() => setAiPanelOpen(false)} />}
      {!isAiPanelOpen && (
        <Button onClick={() => setAiPanelOpen(true)} className="absolute bottom-6 right-6 rounded-full h-12 px-5 shadow-lg bg-indigo-600 hover:bg-indigo-700 text-white flex items-center gap-2 font-semibold">
          <Wand2 size={18} />
          Mở Trợ lý AI
        </Button>
      )}

      <AlertDialog open={isSaveAlertOpen} onOpenChange={setIsSaveAlertOpen}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>Hoàn tất và lưu vào Inspections?</AlertDialogTitle>
            <AlertDialogDescription>Chọn một trong hai tùy chọn bên dưới:</AlertDialogDescription>
          </AlertDialogHeader>
          <div className="grid gap-2 mt-4">
            <div className={`p-4 border rounded-lg hover:bg-gray-50 cursor-pointer ${isProcessing ? 'opacity-50 pointer-events-none' : ''}`} onClick={() => handleSaveToInspections(true)}>
                <div className="flex items-center justify-between">
                    <h4 className="font-semibold">Chuyển vào Inspections</h4>
                    {isProcessing && <Loader2 className="animate-spin h-4 w-4 text-indigo-600"/>}
                </div>
                <p className="text-sm text-gray-600">Lưu tài liệu và xóa bản nháp này.</p>
            </div>
            
            <div className={`p-4 border rounded-lg hover:bg-gray-50 cursor-pointer ${isProcessing ? 'opacity-50 pointer-events-none' : ''}`} onClick={() => handleSaveToInspections(false)}>
                <div className="flex items-center justify-between">
                    <h4 className="font-semibold">Lưu một bản sao</h4>
                    {isProcessing && <Loader2 className="animate-spin h-4 w-4 text-indigo-600"/>}
                </div>
                <p className="text-sm text-gray-600">Giữ lại bản nháp này, tạo bản PDF và tải lên phân tích.</p>
            </div>
          </div>
          <AlertDialogFooter className="mt-4">
            <AlertDialogCancel disabled={isProcessing}>Hủy</AlertDialogCancel>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>
    </div>
  );
};