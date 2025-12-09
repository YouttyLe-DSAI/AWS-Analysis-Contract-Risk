import { Request, Response } from 'express';
import path from 'path';
import { uploadToS3, getDownloadUrl } from '../services/s3Service';
import { 
  createChatSession, 
  saveChatMessage, 
  getSessionById,
  updateSessionWithAnalysis,
  getChatHistory,
  deleteSession 
} from '../services/dynamoService';
import { invokeChatLambda, sendChatToBedrock } from '../services/awsService';
import { searchLegalDocs } from '../services/ragService';
import { getUserInspections, getUserDrafts, updateUserDrafts } from '../services/dynamoService';
import { generateLegalText } from '../services/awsService'; 

const MAX_FILE_SIZE_BYTES = 4.5 * 1024 * 1024; 

const sanitizeBedrockDocumentName = (filename: string): string => {
  const base = filename.replace(/\.[^.]+$/, '');
  let safe = base.replace(/[^A-Za-z0-9\-\(\)\[\]\s]/g, ' ');
  safe = safe.replace(/\s+/g, ' ').trim();
  if (!safe) safe = 'Contract Document';
  return safe;
};

// ==========================================================
// 1. UPLOAD CONTRACT -> GỌI RAG -> GỌI PYTHON LAMBDA
// ==========================================================
export const uploadContract = async (req: Request, res: Response) => {
  try {
    const file = (req as any).file;
    const user = (req as any).user;

    // Validation
    if (!file) return res.status(400).json({ error: "No file uploaded" });
    if (!user || !user.id) return res.status(401).json({ error: "User not authenticated" });

    if (file.size > MAX_FILE_SIZE_BYTES) {
      return res.status(400).json({ error: "File too large" });
    }

    const userId = user.id;

    // A. Upload S3
    const s3Key = await uploadToS3(
      file.buffer,
      file.originalname,
      file.mimetype,
      'user-document',
      userId
    );

    // B. Tạo Session
    const displayName = path.parse(file.originalname).name;     
    const sessionId = await createChatSession(userId, displayName, s3Key);
    // C. Chuẩn bị file cho Lambda
    const fileBytesBase64 = file.buffer.toString('base64');
    const fileFormat = path.extname(file.originalname).replace('.', '').toLowerCase() || 'pdf';
    const fileNameForBedrock = sanitizeBedrockDocumentName(file.originalname);
    
    // D. GỌI RAG ĐỂ LẤY CONTEXT LUẬT
    const ragQuery = file.originalname || 'hop_dong';
    const ragContext = await searchLegalDocs(ragQuery, 5);
    console.log('[RAG] Context length:', ragContext ? ragContext.length : 0);

    // E. Payload cho AI Lambda Python
    const payload = {
      language: 'vi',
      file_bytes_base64: fileBytesBase64,
      file_format: fileFormat,
      file_name: fileNameForBedrock,
      context_rag: ragContext, 

      // Metadata
      session_id: sessionId,
      s3_key: s3Key,
      user_id: userId
    };

    console.log(`[Upload] Invoking AI Lambda for file: ${file.originalname}`);

    // F. Gọi AI Review Lambda (Python)
    const lambdaRaw = await invokeChatLambda(
      process.env.LAMBDA_REVIEW_ARN!,
      payload
    );

    if (!lambdaRaw || (lambdaRaw.statusCode && lambdaRaw.statusCode !== 200)) {
      console.error("AI Analysis Failed:", lambdaRaw);
      return res.status(500).json({ 
        error: "AI analysis failed",
        details: lambdaRaw?.body || lambdaRaw 
      });
    }

    // Parse Body
    const bodyObj = typeof lambdaRaw.body === 'string'
        ? JSON.parse(lambdaRaw.body)
        : lambdaRaw.body || lambdaRaw;

    const analysis = bodyObj.analysis;

    // G. Lưu kết quả vào DB
    await updateSessionWithAnalysis(sessionId, analysis);

    // H. Lưu tin nhắn chào mừng
    const overallRisk = analysis?.overall_risk_level ?? 'UNKNOWN';
    
    // Format tin nhắn chào mừng
    const riskDisplay = overallRisk === 'LOW' ? '🟢 THẤP' : 
                        overallRisk === 'MEDIUM' ? '🟡 TRUNG BÌNH' : 
                        overallRisk === 'HIGH' ? '🔴 CAO' : overallRisk;

    await saveChatMessage(
      sessionId,
      'assistant',
      `✅ **Đã phân tích xong hợp đồng: ${file.originalname}**\n\n` +
      `📊 Mức độ rủi ro tổng quan: **${riskDisplay}**\n` +
      `_Bạn có thể hỏi chi tiết về các điều khoản bên dưới._`
    );
    const fileUrl = await getDownloadUrl(s3Key);
    return res.json({
      message: "Analysis complete",
      session_id: sessionId,
      status: "ANALYZED",
      result: analysis,
      file_url: fileUrl,
      file_type: fileFormat
    });

  } catch (error) {
    console.error("Upload/Analysis Error:", error);
    return res.status(500).json({ error: "Processing failed", details: String(error) });
  }
};

// ==========================================================
// 2. CHAT QA -> CÓ NHỚ LỊCH SỬ
// ==========================================================
export const chatWithContract = async (req: Request, res: Response) => {
  try {
    const { sessionId, message } = req.body;

    if (!sessionId || !message) {
      return res.status(400).json({ error: "sessionId và message là bắt buộc" });
    }

    // 1. Lưu câu hỏi của User vào DB trước
    await saveChatMessage(sessionId, "user", message);

    // 2. Lấy Session từ DB
    const session = await getSessionById(sessionId);
    if (!session) {
      return res.status(404).json({ error: "Session not found" });
    }

    // 3. Chuẩn bị Context Analysis cho AI (JSON kết quả phân tích rủi ro)
    const s: any = session;
    const analysisData = s.analysis || s.analysis_json || {
        summary: s.summary || "Chưa có dữ liệu",
        risks: s.risks || []
    };
    const analysisContextString = JSON.stringify(analysisData, null, 2);

    // 4. LẤY LỊCH SỬ CHAT ĐỂ AI CÓ "TRÍ NHỚ"
    const historyItems = await getChatHistory(sessionId) || [];
    
    // Lấy 6 tin nhắn gần nhất (để tránh quá limit token và tập trung vào ngữ cảnh gần)
    const recentHistory = historyItems.slice(-6); 

    let conversationHistory = "";
    if (recentHistory.length > 0) {
        conversationHistory = recentHistory.map((item: any) => {
            const roleName = item.role === 'user' ? 'Người dùng' : 'AI Assistant';
            return `${roleName}: ${item.content}`;
        }).join('\n');
    }

    // 5. Ghép Prompt thông minh: Lịch sử + Câu hỏi mới
    const fullMessageToAI = `
    === LỊCH SỬ HỘI THOẠI TRƯỚC ĐÓ (Để tham khảo ngữ cảnh) ===
    ${conversationHistory}
    ==========================================================
    
    CÂU HỎI MỚI CỦA NGƯỜI DÙNG:
    "${message}"
    
    (Hãy trả lời câu hỏi mới dựa trên phân tích hợp đồng và lịch sử hội thoại trên. Nếu câu hỏi dùng từ thay thế như "nó", "điều đó", hãy hiểu dựa theo lịch sử.)
    `;

    console.log(`[Chat] Sending message with history to Bedrock for session ${sessionId}...`);

    // 6. Gọi Bedrock (Gửi prompt đã ghép lịch sử)
    const aiAnswer = await sendChatToBedrock(fullMessageToAI, analysisContextString);

    // 7. Lưu câu trả lời của AI
    await saveChatMessage(sessionId, "assistant", aiAnswer);

    // 8. Trả về Frontend
    return res.json({ answer: aiAnswer });

  } catch (error) {
    console.error("Chat Error:", error);
    return res.status(500).json({ error: "Chat failed" });
  }
};


// ==========================================================
// 3. GENERATE CONTRACT -> TRẢ VỀ LINK DOWNLOAD
// ==========================================================
export const generateContractAPI = async (req: Request, res: Response) => {
  try {
    const user = (req as any).user;
    const { template_id, contract_info } = req.body;

    if (!template_id || !contract_info) {
      return res.status(400).json({ error: "Thiếu template_id hoặc contract_info" });
    }

    console.log(`[Generate] User ${user.id} requesting template ${template_id}`);

    const payload = {
      template_id: template_id,
      contract_info: contract_info,
      language: "vi",
      user_id: user.id
    };

    const lambdaArn = process.env.LAMBDA_GENERATE_ARN || process.env.LAMBDA_TEMPLATE_ARN; 
    
    // Gọi hàm invoke
    const result = await invokeChatLambda(lambdaArn!, payload);

    if (!result || (result.statusCode && result.statusCode !== 200)) {
        return res.status(500).json({ error: "Lỗi sinh hợp đồng từ AI", details: result });
    }

    const bodyObj = typeof result.body === 'string' ? JSON.parse(result.body) : result.body;
    
    const contractHtml = bodyObj.contract_html || "";
    
    if (!contractHtml) {
        return res.status(500).json({ error: "AI không trả về nội dung hợp đồng" });
    }

    // Tạo file .DOC (Fake Word từ HTML)
    const docContent = `
      <html xmlns:o='urn:schemas-microsoft-com:office:office' xmlns:w='urn:schemas-microsoft-com:office:word' xmlns='http://www.w3.org/TR/REC-html40'>
      <head><meta charset='utf-8'><title>Contract</title></head>
      <body>${contractHtml}</body></html>
    `;
    const docBuffer = Buffer.from(docContent, 'utf-8');
    
    // Đặt tên file
    const contractTitle = bodyObj.template_title || "Hop_dong_mau";
    const safeTitle = contractTitle.replace(/[^a-zA-Z0-9\u00C0-\u1EF9 ]/g, "_");
    const fileName = `${safeTitle}_${Date.now()}.doc`; 

    // Upload S3
    const s3Key = await uploadToS3(
        docBuffer,
        fileName,
        'application/msword',     
        'generated-monthly-user', 
        user.id                   
    );

    // LƯU VÀO DB ĐỂ HIỆN TRONG LỊCH SỬ (TAB INSPECTIONS)
    const sessionId = await createChatSession(
        user.id,
        contractTitle,
        s3Key
    );
    console.log(`[Generate] Saved to ChatSessions history with ID: ${sessionId}`);
    // -------------------------------------------------------------------------

    // Tạo Presigned URL
    const downloadUrl = await getDownloadUrl(s3Key);

    return res.json({
      success: true,
      data: {
        sessionId: sessionId,
        template_title: contractTitle,
        final_doc_path: s3Key,
        downloadUrl: downloadUrl,
        contentHtml: contractHtml,
        message: "Hợp đồng đã được tạo thành công."
      }
    });

  } catch (error) {
    console.error("Generate Error:", error);
    return res.status(500).json({ error: "Internal Server Error" });
  }
};

// Lấy danh sách inspections + drafts cho user, kèm URL mới cho mỗi file
export const getUserDashboard = async (req: Request, res: Response) => {
  try {
    const user = (req as any).user;
    if (!user) {
      return res.status(401).json({ error: "Unauthorized" });
    }

    const [rawInspections, drafts] = await Promise.all([
      getUserInspections(user.id),
      getUserDrafts(user.id),
    ]);

    const inspections = await Promise.all(
      (rawInspections || []).map(async (item: any) => {
        const s3Key =
          item.s3Key ||
          item.s3_key ||
          item.fileKey ||
          item.originalS3Key ||
          item.original_s3_key;

        let fileUrl: string | undefined;

        if (s3Key) {
          try {
            fileUrl = await getDownloadUrl(s3Key);
          } catch (err) {
            console.error("Generate presigned URL error for inspection:", item.id, err);
          }
        }

        // --- 2. Đoán loại file (pdf/docx/...) ---
        const rawType =
          item.fileType ||
          item.file_type ||
          item.extension ||
          item.originalExtension;

        let fileType = rawType as string | undefined;

        if (!fileType && item.name) {
          const ext = path.extname(item.name).replace(".", "").toLowerCase();
          if (ext) fileType = ext;
        }

        // --- 3. Trả về object đã “bổ sung” field cho FE ---
        return {
          ...item,
          fileUrl,  
          fileType, 
        };
      })
    );

    return res.json({
      success: true,
      inspections,
      drafts: drafts || [],
    });
  } catch (error) {
    console.error("Dashboard Error:", error);
    return res.status(500).json({ error: "Failed to load dashboard" });
  }
};


export const saveUserDrafts = async (req: Request, res: Response) => {
    try {
        const user = (req as any).user;
        const { templates } = req.body; 

        await updateUserDrafts(user.id, templates);
        res.json({ success: true });
    } catch (error) {
        console.error("Save Draft Error:", error);
        res.status(500).json({ error: "Failed to save drafts" });
    }
};

export const deleteContract = async (req: Request, res: Response) => {
    try {
        const { id } = req.params;
        const user = (req as any).user;

        if (!id) {
            return res.status(400).json({ error: "Thiếu Session ID" });
        }

        await deleteSession(id, user.id);

        res.json({ success: true, message: "Đã xóa hợp đồng thành công" });
    } catch (error) {
        console.error("Delete Error:", error);
        res.status(500).json({ error: "Không thể xóa hợp đồng" });
    }
};

// 4. AI WRITER ASSIST (API MỚI CHO EDITOR)
// ==========================================================
export const aiWriterAssist = async (req: Request, res: Response) => {
  try {
    const { prompt } = req.body;
    
    if (!prompt) {
      return res.status(400).json({ error: "Thiếu nội dung prompt" });
    }

    const content = await generateLegalText(prompt);

    return res.json({ answer: content });

  } catch (error) {
    console.error("AI Writer Error:", error);
    return res.status(500).json({ error: "Failed to generate text" });
  }
};