import React, { useState, useRef, useEffect } from 'react';
import { useNavigate, useLocation } from 'react-router-dom';
import {
  CloudUpload, Loader2, FileText, RotateCcw,
  CheckCircle, Bot, AlertTriangle, Shield, TrendingUp, DraftingCompass, MessageSquare, HardDriveDownload
} from 'lucide-react';
import { Button } from '@/components/ui/button';
import { cn } from '@/lib/utils';
import { motion, AnimatePresence } from 'framer-motion';
import { DeepAnalysisView } from '@/components/DeepAnalysisView';
import {
  AlertDialog,
  AlertDialogAction,
  AlertDialogCancel,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogTitle,
} from "@/components/ui/alert-dialog";
import { useAuth, UserInspection, ContractAnalysisData } from '@/contexts/AuthContext';
import { useToast } from '@/components/ui/use-toast';
import DocViewer, { DocViewerRenderers } from "@cyntler/react-doc-viewer";
import { uploadContractService, chatContractService } from '@/services/contractService';
import { renderAsync } from 'docx-preview';

// ---------------- DOCX VIEWER (docx-preview) ----------------

const DocxViewer = ({ fileUrl }: { fileUrl: string }) => {
  const containerRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    if (!fileUrl || !containerRef.current) return;

    fetch(fileUrl)
      .then((res) => res.blob())
      .then((blob) => {
        if (containerRef.current) {
          renderAsync(blob, containerRef.current, containerRef.current, {
            inWrapper: true,
            ignoreWidth: false,
            className: "docx-viewer-content"
          });
        }
      })
      .catch((err) => console.error("Lỗi tải DOCX:", err));
  }, [fileUrl]);

  return (
    <div
      ref={containerRef}
      className="w-full h-full overflow-y-auto bg-gray-100 p-4"
    />
  );
};


const detectExt = (
  fileType?: string,
  fileName?: string,
  fileUrl?: string
): string => {
  if (fileType) return fileType.toLowerCase();

  if (fileName) {
    const parts = fileName.split(".");
    if (parts.length > 1) {
      const extFromName = parts.pop()!.toLowerCase();
      if (extFromName !== fileName.toLowerCase()) return extFromName;
    }
  }

  if (fileUrl) {
    try {
      const urlObj = new URL(fileUrl);
      const lastSegment = urlObj.pathname.split("/").pop() || "";
      const dotIdx = lastSegment.lastIndexOf(".");
      if (dotIdx !== -1) {
        return lastSegment.slice(dotIdx + 1).toLowerCase();
      }
    } catch {
    }
  }

  return "";
};

// ---------------- PDF / DOCX VIEWER WRAPPER ----------------

const PDFViewerSection = React.memo(
  ({ fileUrl, fileName, fileType, analysisResult }: any) => {
    const lowerUrl = (fileUrl || "").toLowerCase();
    const lowerType = (fileType || "").toLowerCase();

    const isPdf =
      lowerType === "pdf" ||
      lowerUrl.includes(".pdf");

    const isDocx =
      lowerType === "docx" ||
      lowerType === "doc" ||
      lowerUrl.includes(".docx") ||
      lowerUrl.includes(".doc");

    if (fileUrl && (isPdf || isDocx)) {
      return (
        <div className="bg-gray-100/50 flex flex-col h-full relative overflow-hidden">
          <div className="w-full h-full absolute inset-0 doc-viewer-wrapper">
            {isPdf && (
              <DocViewer
                documents={[{ uri: fileUrl, fileType: "pdf", fileName }]}
                pluginRenderers={DocViewerRenderers}
                style={{ height: "100%", width: "100%" }}
                config={{
                  header: {
                    disableHeader: true,
                    disableFileName: true,
                    retainURLParams: true,
                  },
                  pdfVerticalScrollByDefault: true,
                  pdfZoom: { defaultZoom: 0.8, zoomJump: 0.1 },
                }}
                prefetchMethod="GET"
                requestHeaders={{}}
              />
            )}

            {isDocx && <DocxViewer fileUrl={fileUrl} />}
          </div>
        </div>
      );
    }

    if (analysisResult?.contract?.fullContent) {
      return (
        <div className="bg-gray-100/50 flex flex-col h-full relative overflow-hidden">
          <div className="overflow-y-auto p-8 custom-scrollbar w-full h-full bg-white">
            <div
              className="w-full max-w-4xl mx-auto pb-20 font-serif text-justify leading-relaxed text-base space-y-6 text-gray-800"
              dangerouslySetInnerHTML={{ __html: analysisResult.contract.fullContent }}
            />
          </div>
        </div>
      );
    }

    if (fileUrl) {
      return (
        <div className="bg-gray-100/50 flex flex-col h-full relative overflow-hidden">
          <div className="flex items-center justify-center h-full text-gray-500">
            <div className="text-center space-y-2">
              <p className="text-sm">
                Loại tệp này hiện chưa được hỗ trợ xem trực tiếp.
              </p>
              <a
                href={fileUrl}
                target="_blank"
                rel="noreferrer"
                className="text-xs text-indigo-600 underline"
              >
                Tải tệp về thiết bị
              </a>
            </div>
          </div>
        </div>
      );
    }

    return (
      <div className="bg-gray-100/50 flex flex-col h-full relative overflow-hidden">
        <div className="flex items-center justify-center h-full text-gray-400">
          <div className="text-center">
            <Loader2 className="w-8 h-8 animate-spin mx-auto mb-2" />
            Đang tải tài liệu...
          </div>
        </div>
      </div>
    );
  },
  (prev, next) => prev.fileUrl === next.fileUrl
);

const STORAGE_KEYS = {
  ANALYSIS: 'agreeme_active_analysis',
  FILENAME: 'agreeme_active_filename',
  SESSION: 'agreeme_active_session',
  CHAT: 'agreeme_active_chat',
  FILE_URL: 'agreeme_active_file_url'
};


const mapBackendToFrontend = (backendResult: any, fileName: string): ContractAnalysisData => {
  let score = 50;
  const riskLevel = backendResult.overall_risk_level || "MEDIUM";
  if (riskLevel === "LOW") score = 85;
  if (riskLevel === "MEDIUM") score = 60;
  if (riskLevel === "HIGH") score = 30;
  if (riskLevel === "CRITICAL") score = 10;

  const statusMap: Record<string, string> = {
    "LOW": "An toàn",
    "MEDIUM": "Cần lưu ý",
    "HIGH": "Rủi ro cao",
    "CRITICAL": "Nguy hiểm"
  };

  const risksCount = { danger: 0, caution: 0, safe: 0 };

  const clauses = (backendResult.risk_items || []).map((item: any, index: number) => {
    let riskType: 'safe' | 'caution' | 'danger' = 'safe';

    if (item.severity === 'CRITICAL' || item.severity === 'HIGH') {
      riskType = 'danger';
      risksCount.danger++;
    } else if (item.severity === 'MEDIUM') {
      riskType = 'caution';
      risksCount.caution++;
    } else {
      risksCount.safe++;
    }

    return {
      id: item.id || `clause-${index}`,
      title: item.title || `Vấn đề #${index + 1}`,
      content: item.clause_excerpt || "Không trích dẫn cụ thể",
      risk: riskType,
      suggestion: item.recommendation || item.description
    };
  });

  return {
    contract: {
      title: fileName,
      clauses,
      fullContent: backendResult.full_text_html || ""
    },
    summary: {
      score,
      status: statusMap[riskLevel] || riskLevel,
      description: backendResult.summary || "Đã hoàn tất phân tích.",
      risks: [
        { level: "danger", count: risksCount.danger },
        { level: "caution", count: risksCount.caution },
        { level: "safe", count: risksCount.safe },
      ]
    }
  };
};

type SidebarView = 'summary' | 'deep-analysis' | 'chat';

// ========================= MAIN COMPONENT =========================

const DeepAnalysis = () => {
  const navigate = useNavigate();
  const location = useLocation();
  const { user, updateUserProfile } = useAuth();
  const { toast } = useToast();
  const [analysisResult, setAnalysisResult] = useState<ContractAnalysisData | null>(null);
  const [fileName, setFileName] = useState<string>("");
  const [fileUrl, setFileUrl] = useState<string>("");
  const [fileType, setFileType] = useState<string>("");

  const [isAnalyzing, setIsAnalyzing] = useState(false);
  const [showResult, setShowResult] = useState(false);
  const [saveState, setSaveState] = useState<'idle' | 'saving' | 'saved'>('idle');
  const [hasUnsavedChanges, setHasUnsavedChanges] = useState(false);
  const [isConfirmOpen, setIsConfirmOpen] = useState(false);
  const [isDragging, setIsDragging] = useState(false);
  const [activeView, setActiveView] = useState<SidebarView>('summary');
  const fileInputRef = useRef<HTMLInputElement>(null);
  const [sessionId, setSessionId] = useState<string | null>(null);
  const [chatMessages, setChatMessages] = useState<{ from: string, text: string }[]>([
    { from: "bot", text: "Chào bạn, tôi là trợ lý AI. Tôi có thể giúp gì về hợp đồng này?" }
  ]);
  const [inputMessage, setInputMessage] = useState("");
  const [isChatLoading, setIsChatLoading] = useState(false);

  // -------- Upload & phân tích file mới --------

  const processFile = async (file: File) => {
    setFileName(file.name);
    setIsAnalyzing(true);
    setHasUnsavedChanges(false);
    setSaveState('idle');

    try {
      const data = await uploadContractService(file);
      console.log("🔥 DỮ LIỆU TỪ BACKEND:", data);

      setSessionId(data.session_id);

      if (data.file_url) {
        setFileUrl(data.file_url);
        localStorage.setItem(STORAGE_KEYS.FILE_URL, data.file_url);
      }

      const ext = file.name.split('.').pop()?.toLowerCase();
      setFileType(ext || "");

      const mappedResult = mapBackendToFrontend(data.result, file.name);

      setAnalysisResult(mappedResult);
      setShowResult(true);
      setHasUnsavedChanges(true);
      setActiveView('summary');

      const initialChat = [{ from: "bot", text: "Chào bạn, tôi là trợ lý AI. Tôi có thể giúp gì về hợp đồng này?" }];
      setChatMessages(initialChat);

      localStorage.setItem(STORAGE_KEYS.ANALYSIS, JSON.stringify(mappedResult));
      localStorage.setItem(STORAGE_KEYS.FILENAME, file.name);
      localStorage.setItem(STORAGE_KEYS.SESSION, data.session_id);
      localStorage.setItem(STORAGE_KEYS.CHAT, JSON.stringify(initialChat));

    } catch (error: any) {
      console.error("Analysis Error:", error);
      toast({
        title: "Lỗi phân tích",
        description: "Có lỗi xảy ra khi kết nối với AI Server.",
        variant: "destructive"
      });
      setFileName("");
    } finally {
      setIsAnalyzing(false);
    }
  };

  // -------- Restore khi mở từ Inspections hoặc refresh --------

  useEffect(() => {
  if (location.state?.analysisData) {
    let {
      analysisData,
      fileName: navFileName,
      sessionId: savedSessionId,
      fileUrl: navFileUrl,
      fileType: navFileType,
    } = location.state as any;

    if (
      !analysisData.contract &&
      (analysisData.risk_items || analysisData.risks || analysisData.summary)
    ) {
      analysisData = mapBackendToFrontend(analysisData, navFileName || "Tài liệu");
    }

    setAnalysisResult(analysisData);
    setFileName(navFileName);
    if (savedSessionId) setSessionId(savedSessionId);
    setShowResult(true);
    setHasUnsavedChanges(true);
    setSaveState('idle');

    if (navFileUrl) {
      setFileUrl(navFileUrl);
      localStorage.setItem(STORAGE_KEYS.FILE_URL, navFileUrl);
    } else {
      setFileUrl("");
    }

    if (navFileType) {
      setFileType(navFileType);
    } else if (navFileName) {
      const ext = navFileName.split('.').pop()?.toLowerCase();
      if (ext) setFileType(ext);
    }

  } else {
    const savedAnalysis = localStorage.getItem(STORAGE_KEYS.ANALYSIS);
    const savedName = localStorage.getItem(STORAGE_KEYS.FILENAME);
    const savedSession = localStorage.getItem(STORAGE_KEYS.SESSION);
    const savedChat = localStorage.getItem(STORAGE_KEYS.CHAT);
    const savedUrl = localStorage.getItem(STORAGE_KEYS.FILE_URL);

    if (savedAnalysis && savedName) {
      try {
        setAnalysisResult(JSON.parse(savedAnalysis));
        setFileName(savedName);
        setShowResult(true);
        setHasUnsavedChanges(true);
        setSaveState('idle');
        if (savedSession) setSessionId(savedSession);
        if (savedChat) setChatMessages(JSON.parse(savedChat));
        if (savedUrl) setFileUrl(savedUrl);

        const ext = savedName.split('.').pop()?.toLowerCase();
        if (ext) setFileType(ext);
      } catch (e) {
        console.error(e);
      }
    }
  }
}, [location.state]);



  // -------- Sync chat vào localStorage --------

  useEffect(() => {
    if (showResult && chatMessages.length > 0) {
      localStorage.setItem(STORAGE_KEYS.CHAT, JSON.stringify(chatMessages));
    }
  }, [chatMessages, showResult]);

  // -------- Chat với AI --------

  const handleSendMessage = async () => {
    if (!inputMessage.trim() || !sessionId) return;
    const userMsg = inputMessage;
    const newHistory = [...chatMessages, { from: 'user', text: userMsg }];
    setChatMessages(newHistory);
    setInputMessage("");
    setIsChatLoading(true);
    try {
      const data = await chatContractService(sessionId, userMsg);
      setChatMessages(prev => [...prev, { from: 'bot', text: data.answer }]);
    } catch (error) {
      setChatMessages(prev => [...prev, { from: 'bot', text: "Xin lỗi, tôi đang gặp sự cố kết nối." }]);
    } finally {
      setIsChatLoading(false);
    }
  };

  // -------- Upload / Drag & Drop --------

  const handleFileChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    if (e.target.files && e.target.files[0]) processFile(e.target.files[0]);
  };
  const handleDragOver = (e: React.DragEvent<HTMLDivElement>) => { e.preventDefault(); setIsDragging(true); };
  const handleDragLeave = (e: React.DragEvent<HTMLDivElement>) => { e.preventDefault(); setIsDragging(false); };
  const handleDrop = (e: React.DragEvent<HTMLDivElement>) => {
    e.preventDefault();
    setIsDragging(false);
    if (e.dataTransfer.files && e.dataTransfer.files[0]) processFile(e.dataTransfer.files[0]);
  };

  // -------- Lưu Inspection --------

  const handleSave = async () => {
    if (!user || !fileName || !analysisResult) return;
    setSaveState('saving');
    const newInspection: UserInspection = {
      id: `insp_${Date.now()}`,
      name: fileName,
      content: analysisResult.contract.fullContent,
      score: analysisResult.summary.score,
      createdAt: new Date().toISOString(),
      analysisData: analysisResult,
    };
    const updatedInspections = [...user.inspections, newInspection];
    const { error } = await updateUserProfile({ inspections: updatedInspections });
    if (!error) {
      setSaveState('saved');
      toast({ title: "Lưu thành công!", description: `"${fileName}" đã được chuyển vào mục Hợp đồng đã hoàn tất.` });
      setHasUnsavedChanges(false);
      setTimeout(() => navigate('/inspections'), 1000);
    } else {
      setSaveState('idle');
      toast({ title: "Lỗi", description: "Không thể lưu phân tích.", variant: "destructive" });
    }
  };

  // -------- Reset màn hình --------

  const handleReset = () => {
    setFileName("");
    setShowResult(false);
    setAnalysisResult(null);
    setFileUrl("");
    setFileType("");
    setChatMessages([{ from: "bot", text: "Chào bạn, tôi có thể giúp gì cho bạn?" }]);
    setSessionId(null);
    if (fileInputRef.current) fileInputRef.current.value = "";
    setHasUnsavedChanges(false);
    setSaveState('idle');

    localStorage.removeItem(STORAGE_KEYS.ANALYSIS);
    localStorage.removeItem(STORAGE_KEYS.FILENAME);
    localStorage.removeItem(STORAGE_KEYS.SESSION);
    localStorage.removeItem(STORAGE_KEYS.CHAT);
    localStorage.removeItem(STORAGE_KEYS.FILE_URL);
  };

  const handleResetClick = () => { hasUnsavedChanges ? setIsConfirmOpen(true) : handleReset(); };
  const confirmAction = (shouldSave: boolean) => {
    if (shouldSave) { handleSave(); } else { handleReset(); }
    setIsConfirmOpen(false);
  };

  // -------- UI phụ trợ: icon, summary, chat --------

  const renderRiskIcon = (level: string, className = "w-4 h-4") => {
    const RENDER_MAP: { [key: string]: React.ReactNode } = {
      danger: <AlertTriangle className={cn("text-red-500", className)} />,
      caution: <Shield className={cn("text-yellow-500", className)} />,
      safe: <CheckCircle className={cn("text-green-500", className)} />
    };
    return RENDER_MAP[level] || RENDER_MAP['safe'];
  };

  const ScoreRing = ({ score }: { score: number }) => {
    const radius = 36; const circumference = 2 * Math.PI * radius;
    const strokeDashoffset = circumference - (score / 100) * circumference;
    return (
      <div className="relative w-20 h-20 flex items-center justify-center">
        <svg className="transform -rotate-90 w-20 h-20">
          <circle cx="40" cy="40" r={radius} stroke="currentColor" strokeWidth="6" fill="transparent" className="text-white/20" />
          <circle cx="40" cy="40" r={radius} stroke="currentColor" strokeWidth="6" fill="transparent" strokeDasharray={circumference} strokeDashoffset={strokeDashoffset} strokeLinecap="round" className="text-white transition-all duration-1000 ease-out" />
        </svg>
        <div className="absolute inset-0 flex flex-col items-center justify-center text-white">
          <span className="text-xl font-bold leading-none">{score}</span>
          <span className="text-[9px] opacity-80 font-medium">/100</span>
        </div>
      </div>
    );
  };

  const renderSummaryContent = () => {
    if (!analysisResult?.summary) return null;
    return (
      <div className='space-y-4'>
        <div className="bg-gradient-to-br from-[#4f46e5] to-[#4338ca] rounded-2xl p-5 text-white shadow-lg flex items-center justify-between">
          <div className="flex flex-col gap-1">
            <span className="text-[10px] uppercase tracking-widest font-semibold opacity-70">Điểm hợp đồng</span>
            <h2 className="text-2xl font-bold tracking-tight">{analysisResult.summary.status || "Chưa xác định"}</h2>
            <p className="text-xs opacity-90 font-medium max-w-[150px]">
              {analysisResult.summary.description || "..."}
            </p>
          </div>
          <div className="flex-shrink-0">
            <ScoreRing score={analysisResult.summary.score || 0} />
          </div>
        </div>

        <div className='p-4 bg-card rounded-xl border shadow-sm'>
          <div className='flex items-center justify-around text-center'>
            {(analysisResult.summary.risks || []).map((risk: any) => (
              <div key={risk.level} className='flex flex-col items-center gap-1'>
                <div className={cn('text-2xl font-bold', {
                  'text-red-500': risk.level === 'danger',
                  'text-yellow-500': risk.level === 'caution',
                  'text-green-500': risk.level === 'safe'
                })}>
                  {risk.count || 0}
                </div>
                <div className='text-[10px] text-gray-500 font-medium uppercase tracking-wide'>
                  {risk.level} Risk
                </div>
              </div>
            ))}
          </div>
          <div className="w-full bg-gray-100 h-px my-4"></div>
          <div className='space-y-2'>
            {(analysisResult?.contract?.clauses || []).map((clause: any) => (
              <a
                href={`#${clause.id}`}
                key={clause.id}
                className='flex items-center gap-2.5 p-2 rounded-lg hover:bg-slate-100 cursor-pointer transition-colors border border-transparent hover:border-slate-200'
              >
                {renderRiskIcon(clause.risk)}
                <span className='text-xs text-gray-700 font-medium flex-1 truncate'>{clause.title}</span>
              </a>
            ))}
          </div>
        </div>
      </div>
    );
  };

  const renderChatContent = () => (
    <div className="bg-card border rounded-xl p-3 flex flex-col h-full shadow-sm">
      <div className="flex-1 space-y-3 pr-2 -mr-2 overflow-y-auto custom-scrollbar-sm mb-3">
        {chatMessages.map((msg, index) => (
          <div key={index} className={cn('flex items-end gap-2', { 'justify-end': msg.from === 'user' })}>
            {msg.from === 'bot' && (
              <div className="w-6 h-6 rounded-full bg-[#eef2ff] flex items-center justify-center flex-shrink-0">
                <Bot className="w-4 h-4 text-[#4f46e5]" />
              </div>
            )}
            <div className={cn('max-w-[85%] p-2.5 rounded-xl', {
              'bg-gray-100': msg.from === 'bot',
              'bg-[#4f46e5] text-primary-foreground': msg.from === 'user'
            })}>
              <p className="text-xs whitespace-pre-wrap">{msg.text}</p>
            </div>
          </div>
        ))}
        {isChatLoading && (
          <div className="flex items-end gap-2">
            <div className="w-6 h-6 rounded-full bg-[#eef2ff] flex items-center justify-center flex-shrink-0">
              <Bot className="w-4 h-4 text-[#4f46e5]" />
            </div>
            <div className="bg-gray-100 p-2.5 rounded-xl">
              <Loader2 className="h-4 w-4 animate-spin text-gray-500" />
            </div>
          </div>
        )}
      </div>
      <div className="relative mt-auto pt-2 border-t border-dashed">
        <input
          value={inputMessage}
          onChange={(e) => setInputMessage(e.target.value)}
          onKeyDown={(e) => e.key === 'Enter' && handleSendMessage()}
          placeholder="Hỏi AI về hợp đồng này..."
          className="w-full pr-9 h-9 border rounded-lg bg-gray-50 pl-3 text-xs focus:ring-1 focus:ring-[#4f46e5]/50 outline-none transition-all"
          disabled={isChatLoading || !sessionId}
        />
        <Button
          onClick={handleSendMessage}
          disabled={isChatLoading || !sessionId}
          variant="ghost"
          size="icon"
          className="absolute right-0.5 top-1/2 -translate-y-1/2 h-8 w-8 text-[#4f46e5]/80 hover:text-[#4f46e5] mt-1"
        >
          <MessageSquare size={16} />
        </Button>
      </div>
    </div>
  );

  const TABS: { id: SidebarView; label: string; icon: React.ElementType; }[] = [
    { id: 'summary', label: 'Tổng quan', icon: TrendingUp },
    { id: 'deep-analysis', label: 'Phân tích sâu', icon: DraftingCompass },
    { id: 'chat', label: 'Trò chuyện', icon: Bot }
  ];

  // ---------------- RENDER ----------------

  return (
    <div
      className="flex flex-col flex-1 min-h-0 bg-slate-50 h-screen overflow-hidden"
      onDragOver={handleDragOver}
      onDragLeave={handleDragLeave}
      onDrop={handleDrop}
    >
      {!showResult ? (
        <div className="flex-1 flex items-center justify-center p-8">
          <input
            type="file"
            ref={fileInputRef}
            onChange={handleFileChange}
            className="hidden"
            accept=".doc,.docx,.pdf,.txt"
          />
          <div
            className={cn(
              "w-full max-w-2xl h-80 flex flex-col items-center justify-center border-2 border-dashed rounded-3xl text-center transition-all duration-300",
              isDragging ? 'border-indigo-500 bg-indigo-50' : 'border-gray-300 bg-white'
            )}
          >
            {isAnalyzing ? (
              <>
                <Loader2 className="h-12 w-12 animate-spin text-indigo-500" />
                <p className="mt-4 font-semibold text-gray-700">Đang phân tích tài liệu...</p>
                <p className="text-sm text-gray-500">{fileName}</p>
              </>
            ) : (
              <>
                <CloudUpload className="h-12 w-12 text-gray-400" />
                <p className="mt-4 font-semibold text-gray-700">
                  Kéo & thả hợp đồng của bạn vào đây
                </p>
                <p className="text-sm text-gray-500">hoặc</p>
                <Button
                  onClick={() => fileInputRef.current?.click()}
                  variant="link"
                  className="text-indigo-600"
                >
                  Tải tệp lên
                </Button>
                <p className="text-xs text-gray-400 mt-2">Hỗ trợ: PDF, DOCX</p>
              </>
            )}
          </div>
        </div>
      ) : (
        <>
          <div className="h-14 bg-white border-b flex items-center justify-between px-5 flex-shrink-0 z-10 shadow-sm">
            <div className="flex items-center gap-3 min-w-0">
              <FileText size={18} className='text-indigo-600' />
              <h3 className="font-semibold text-gray-800 text-sm truncate">{fileName}</h3>
            </div>
            <div className="flex items-center gap-2">
              <Button
                onClick={handleResetClick}
                variant="ghost"
                className="h-8 text-gray-600 hover:text-red-600 hover:bg-red-50 gap-1.5 text-xs px-3"
              >
                <RotateCcw size={13} /> Tải tệp mới
              </Button>
              <div className="h-5 w-px bg-gray-200"></div>
              <Button
                onClick={handleSave}
                className={cn(
                  "h-8 gap-2 px-4 text-xs font-semibold rounded-md transition-all w-32",
                  saveState === 'saved'
                    ? "bg-green-500 hover:bg-green-600 cursor-not-allowed"
                    : saveState === 'saving'
                      ? "bg-indigo-400 cursor-not-allowed"
                      : "bg-indigo-600 hover:bg-indigo-700"
                )}
                disabled={saveState === 'saving' || saveState === 'saved'}
              >
                {saveState === 'saving' && (
                  <>
                    <Loader2 size={14} className="animate-spin" /> Đang lưu...
                  </>
                )}
                {saveState === 'saved' && (
                  <>
                    <CheckCircle size={14} /> Đã lưu
                  </>
                )}
                {saveState === 'idle' && (
                  <>
                    <HardDriveDownload size={13} /> Lưu & Hoàn tất
                  </>
                )}
              </Button>
            </div>
          </div>

          <div className="grid md:grid-cols-[1fr_400px] flex-1 min-h-0 overflow-hidden relative">
            <PDFViewerSection
              fileUrl={fileUrl}
              fileName={fileName}
              fileType={fileType}
              analysisResult={analysisResult}
            />

            <div className="border-l flex flex-col bg-white h-full min-h-0">
              <div className="flex items-center border-b p-1.5 flex-shrink-0 bg-white">
                {TABS.map(tab => (
                  <button
                    key={tab.id}
                    onClick={() => setActiveView(tab.id)}
                    className={cn(
                      "flex-1 py-2.5 text-xs font-bold rounded-md flex items-center justify-center gap-2 transition-colors",
                      {
                        'bg-[#eef2ff] text-[#4f46e5]': activeView === tab.id,
                        'text-muted-foreground hover:text-foreground hover:bg-muted/50': activeView !== tab.id
                      }
                    )}
                  >
                    <tab.icon
                      className={cn(
                        "w-4 h-4",
                        activeView === tab.id ? "text-[#4f46e5]" : "text-gray-400"
                      )}
                    />
                    {tab.label}
                  </button>
                ))}
              </div>
              <div className="flex-1 min-h-0 relative overflow-hidden bg-slate-50">
                <AnimatePresence mode="wait">
                  <motion.div
                    key={activeView}
                    initial={{ opacity: 0, y: 10 }}
                    animate={{ opacity: 1, y: 0 }}
                    exit={{ opacity: 0, y: -10 }}
                    transition={{ duration: 0.2 }}
                    className="h-full w-full"
                  >
                    {analysisResult && (
                      <>
                        {activeView === 'summary' && (
                          <div className="h-full overflow-y-auto p-4 custom-scrollbar">
                            {renderSummaryContent()}
                          </div>
                        )}
                        {activeView === 'deep-analysis' && (
                          <div className="h-full overflow-y-auto p-4 custom-scrollbar">
                            <DeepAnalysisView clauses={analysisResult?.contract?.clauses || []} />
                          </div>
                        )}
                      </>
                    )}
                    {activeView === 'chat' && (
                      <div className="h-full w-full p-4 overflow-hidden">
                        {renderChatContent()}
                      </div>
                    )}
                  </motion.div>
                </AnimatePresence>
              </div>
              <div className="p-2 bg-white border-t flex-shrink-0 z-10">
                <div className="text-center text-[10px] text-muted-foreground/80 select-none">
                  Agreeme có thể mắc lỗi. Hãy kiểm tra lại thông tin quan trọng.
                </div>
              </div>
            </div>
          </div>
        </>
      )}

      <AlertDialog open={isConfirmOpen} onOpenChange={setIsConfirmOpen}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>Bắt đầu phân tích mới?</AlertDialogTitle>
            <AlertDialogDescription>
              Bạn có thay đổi chưa được lưu. Bạn có muốn lưu bản phân tích hiện tại trước khi
              bắt đầu một bản mới không?
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel onClick={() => confirmAction(false)}>
              Bỏ qua & Bắt đầu mới
            </AlertDialogCancel>
            <AlertDialogAction onClick={() => confirmAction(true)}>
              Lưu & Tiếp tục
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>
    </div>
  );
};

export default DeepAnalysis;
