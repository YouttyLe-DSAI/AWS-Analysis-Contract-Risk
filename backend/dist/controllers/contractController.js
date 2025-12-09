"use strict";
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    function adopt(value) { return value instanceof P ? value : new P(function (resolve) { resolve(value); }); }
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : adopt(result.value).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.aiWriterAssist = exports.deleteContract = exports.saveUserDrafts = exports.getUserDashboard = exports.generateContractAPI = exports.chatWithContract = exports.uploadContract = void 0;
const path_1 = __importDefault(require("path"));
const s3Service_1 = require("../services/s3Service");
const dynamoService_1 = require("../services/dynamoService");
const awsService_1 = require("../services/awsService");
const ragService_1 = require("../services/ragService");
const dynamoService_2 = require("../services/dynamoService");
const awsService_2 = require("../services/awsService");
const MAX_FILE_SIZE_BYTES = 4.5 * 1024 * 1024; // ~4.5MB
// Helper: Làm sạch tên file cho Bedrock
const sanitizeBedrockDocumentName = (filename) => {
    const base = filename.replace(/\.[^.]+$/, '');
    let safe = base.replace(/[^A-Za-z0-9\-\(\)\[\]\s]/g, ' ');
    safe = safe.replace(/\s+/g, ' ').trim();
    if (!safe)
        safe = 'Contract Document';
    return safe;
};
// ==========================================================
// 1. UPLOAD CONTRACT -> GỌI RAG -> GỌI PYTHON LAMBDA
// ==========================================================
const uploadContract = (req, res) => __awaiter(void 0, void 0, void 0, function* () {
    var _a;
    try {
        const file = req.file;
        const user = req.user;
        // Validation
        if (!file)
            return res.status(400).json({ error: "No file uploaded" });
        if (!user || !user.id)
            return res.status(401).json({ error: "User not authenticated" });
        if (file.size > MAX_FILE_SIZE_BYTES) {
            return res.status(400).json({ error: "File too large" });
        }
        const userId = user.id;
        // A. Upload S3
        const s3Key = yield (0, s3Service_1.uploadToS3)(file.buffer, file.originalname, file.mimetype, 'user-document', userId);
        // B. Tạo Session
        //const sessionId = await createChatSession(userId, file.originalname, s3Key);
        const displayName = path_1.default.parse(file.originalname).name;
        const sessionId = yield (0, dynamoService_1.createChatSession)(userId, displayName, s3Key);
        // C. Chuẩn bị file cho Lambda
        const fileBytesBase64 = file.buffer.toString('base64');
        const fileFormat = path_1.default.extname(file.originalname).replace('.', '').toLowerCase() || 'pdf';
        const fileNameForBedrock = sanitizeBedrockDocumentName(file.originalname);
        // D. GỌI RAG ĐỂ LẤY CONTEXT LUẬT
        const ragQuery = file.originalname || 'hop_dong';
        const ragContext = yield (0, ragService_1.searchLegalDocs)(ragQuery, 5);
        console.log('[RAG] Context length:', ragContext ? ragContext.length : 0);
        // E. Payload cho AI Lambda Python
        const payload = {
            language: 'vi',
            file_bytes_base64: fileBytesBase64,
            file_format: fileFormat,
            file_name: fileNameForBedrock,
            context_rag: ragContext, // Gửi luật tham khảo sang Python
            // Metadata
            session_id: sessionId,
            s3_key: s3Key,
            user_id: userId
        };
        console.log(`[Upload] Invoking AI Lambda for file: ${file.originalname}`);
        // F. Gọi AI Review Lambda (Python)
        const lambdaRaw = yield (0, awsService_1.invokeChatLambda)(process.env.LAMBDA_REVIEW_ARN, payload);
        if (!lambdaRaw || (lambdaRaw.statusCode && lambdaRaw.statusCode !== 200)) {
            console.error("AI Analysis Failed:", lambdaRaw);
            return res.status(500).json({
                error: "AI analysis failed",
                details: (lambdaRaw === null || lambdaRaw === void 0 ? void 0 : lambdaRaw.body) || lambdaRaw
            });
        }
        // Parse Body
        const bodyObj = typeof lambdaRaw.body === 'string'
            ? JSON.parse(lambdaRaw.body)
            : lambdaRaw.body || lambdaRaw;
        const analysis = bodyObj.analysis;
        // G. Lưu kết quả vào DB
        yield (0, dynamoService_1.updateSessionWithAnalysis)(sessionId, analysis);
        // H. Lưu tin nhắn chào mừng
        const overallRisk = (_a = analysis === null || analysis === void 0 ? void 0 : analysis.overall_risk_level) !== null && _a !== void 0 ? _a : 'UNKNOWN';
        // Format tin nhắn chào mừng đẹp hơn
        const riskDisplay = overallRisk === 'LOW' ? '🟢 THẤP' :
            overallRisk === 'MEDIUM' ? '🟡 TRUNG BÌNH' :
                overallRisk === 'HIGH' ? '🔴 CAO' : overallRisk;
        yield (0, dynamoService_1.saveChatMessage)(sessionId, 'assistant', `✅ **Đã phân tích xong hợp đồng: ${file.originalname}**\n\n` +
            `📊 Mức độ rủi ro tổng quan: **${riskDisplay}**\n` +
            `_Bạn có thể hỏi chi tiết về các điều khoản bên dưới._`);
        const fileUrl = yield (0, s3Service_1.getDownloadUrl)(s3Key);
        return res.json({
            message: "Analysis complete",
            session_id: sessionId,
            status: "ANALYZED",
            result: analysis,
            file_url: fileUrl,
            file_type: fileFormat
        });
    }
    catch (error) {
        console.error("Upload/Analysis Error:", error);
        return res.status(500).json({ error: "Processing failed", details: String(error) });
    }
});
exports.uploadContract = uploadContract;
// ==========================================================
// 2. CHAT QA -> CÓ NHỚ LỊCH SỬ
// ==========================================================
const chatWithContract = (req, res) => __awaiter(void 0, void 0, void 0, function* () {
    try {
        const { sessionId, message } = req.body;
        if (!sessionId || !message) {
            return res.status(400).json({ error: "sessionId và message là bắt buộc" });
        }
        // 1. Lưu câu hỏi của User vào DB trước
        yield (0, dynamoService_1.saveChatMessage)(sessionId, "user", message);
        // 2. Lấy Session từ DB
        const session = yield (0, dynamoService_1.getSessionById)(sessionId);
        if (!session) {
            return res.status(404).json({ error: "Session not found" });
        }
        // 3. Chuẩn bị Context Analysis cho AI (JSON kết quả phân tích rủi ro)
        const s = session;
        const analysisData = s.analysis || s.analysis_json || {
            summary: s.summary || "Chưa có dữ liệu",
            risks: s.risks || []
        };
        const analysisContextString = JSON.stringify(analysisData, null, 2);
        // 4. LẤY LỊCH SỬ CHAT ĐỂ AI CÓ "TRÍ NHỚ"
        const historyItems = (yield (0, dynamoService_1.getChatHistory)(sessionId)) || [];
        // Lấy 6 tin nhắn gần nhất (để tránh quá limit token và tập trung vào ngữ cảnh gần)
        const recentHistory = historyItems.slice(-6);
        let conversationHistory = "";
        if (recentHistory.length > 0) {
            conversationHistory = recentHistory.map((item) => {
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
        const aiAnswer = yield (0, awsService_1.sendChatToBedrock)(fullMessageToAI, analysisContextString);
        // 7. Lưu câu trả lời của AI
        yield (0, dynamoService_1.saveChatMessage)(sessionId, "assistant", aiAnswer);
        // 8. Trả về Frontend
        return res.json({ answer: aiAnswer });
    }
    catch (error) {
        console.error("Chat Error:", error);
        return res.status(500).json({ error: "Chat failed" });
    }
});
exports.chatWithContract = chatWithContract;
// ==========================================================
// 3. GENERATE CONTRACT -> TRẢ VỀ LINK DOWNLOAD
// ==========================================================
const generateContractAPI = (req, res) => __awaiter(void 0, void 0, void 0, function* () {
    try {
        const user = req.user;
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
        const result = yield (0, awsService_1.invokeChatLambda)(lambdaArn, payload);
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
        // Clean tên file một chút để tránh ký tự lạ
        const safeTitle = contractTitle.replace(/[^a-zA-Z0-9\u00C0-\u1EF9 ]/g, "_");
        const fileName = `${safeTitle}_${Date.now()}.doc`;
        // Upload S3
        const s3Key = yield (0, s3Service_1.uploadToS3)(docBuffer, fileName, 'application/msword', 'generated-monthly-user', user.id);
        // 🔥 [UPDATE QUAN TRỌNG] LƯU VÀO DB ĐỂ HIỆN TRONG LỊCH SỬ (TAB INSPECTIONS)
        // -------------------------------------------------------------------------
        // Hàm createChatSession mặc định set status là 'UPLOADED'.
        // Bên Frontend sẽ hiểu 'UPLOADED' (hoặc thiếu score) là "Chưa phân tích" -> Đúng ý bạn.
        const sessionId = yield (0, dynamoService_1.createChatSession)(user.id, contractTitle, // Tên hiển thị trong danh sách
        s3Key);
        console.log(`[Generate] Saved to ChatSessions history with ID: ${sessionId}`);
        // -------------------------------------------------------------------------
        // Tạo Presigned URL
        const downloadUrl = yield (0, s3Service_1.getDownloadUrl)(s3Key);
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
    }
    catch (error) {
        console.error("Generate Error:", error);
        return res.status(500).json({ error: "Internal Server Error" });
    }
});
exports.generateContractAPI = generateContractAPI;
// Lấy danh sách inspections + drafts cho user, kèm URL mới cho mỗi file
const getUserDashboard = (req, res) => __awaiter(void 0, void 0, void 0, function* () {
    try {
        const user = req.user;
        if (!user) {
            return res.status(401).json({ error: "Unauthorized" });
        }
        // Lấy từ Dynamo (hoặc DB của bạn)
        const [rawInspections, drafts] = yield Promise.all([
            (0, dynamoService_2.getUserInspections)(user.id),
            (0, dynamoService_2.getUserDrafts)(user.id),
        ]);
        // Với mỗi inspection, nếu có s3Key thì generate lại presigned URL mới
        const inspections = yield Promise.all((rawInspections || []).map((item) => __awaiter(void 0, void 0, void 0, function* () {
            // --- 1. Lấy S3 key từ bản ghi Dynamo ---
            // Tùy schema DB của bạn mà chỉnh các field bên dưới cho đúng
            const s3Key = item.s3Key ||
                item.s3_key ||
                item.fileKey ||
                item.originalS3Key ||
                item.original_s3_key;
            let fileUrl;
            if (s3Key) {
                try {
                    // Tạo presigned URL mới, hạn 1h (getDownloadUrl đã set expiresIn = 3600)
                    fileUrl = yield (0, s3Service_1.getDownloadUrl)(s3Key);
                }
                catch (err) {
                    console.error("Generate presigned URL error for inspection:", item.id, err);
                }
            }
            // --- 2. Đoán loại file (pdf/docx/...) ---
            const rawType = item.fileType ||
                item.file_type ||
                item.extension ||
                item.originalExtension;
            let fileType = rawType;
            if (!fileType && item.name) {
                const ext = path_1.default.extname(item.name).replace(".", "").toLowerCase();
                if (ext)
                    fileType = ext;
            }
            // --- 3. Trả về object đã “bổ sung” field cho FE ---
            return Object.assign(Object.assign({}, item), { fileUrl, // 👈 FE sẽ dùng inspection.fileUrl
                fileType });
        })));
        return res.json({
            success: true,
            inspections,
            drafts: drafts || [],
        });
    }
    catch (error) {
        console.error("Dashboard Error:", error);
        return res.status(500).json({ error: "Failed to load dashboard" });
    }
});
exports.getUserDashboard = getUserDashboard;
const saveUserDrafts = (req, res) => __awaiter(void 0, void 0, void 0, function* () {
    try {
        const user = req.user;
        const { templates } = req.body;
        yield (0, dynamoService_2.updateUserDrafts)(user.id, templates);
        res.json({ success: true });
    }
    catch (error) {
        console.error("Save Draft Error:", error);
        res.status(500).json({ error: "Failed to save drafts" });
    }
});
exports.saveUserDrafts = saveUserDrafts;
// [HÀM XÓA] Giữ lại hàm xóa mà bạn đã thêm ở bước trước
const deleteContract = (req, res) => __awaiter(void 0, void 0, void 0, function* () {
    try {
        const { id } = req.params;
        const user = req.user;
        if (!id) {
            return res.status(400).json({ error: "Thiếu Session ID" });
        }
        yield (0, dynamoService_1.deleteSession)(id, user.id);
        res.json({ success: true, message: "Đã xóa hợp đồng thành công" });
    }
    catch (error) {
        console.error("Delete Error:", error);
        res.status(500).json({ error: "Không thể xóa hợp đồng" });
    }
});
exports.deleteContract = deleteContract;
// 4. AI WRITER ASSIST (API MỚI CHO EDITOR)
// ==========================================================
const aiWriterAssist = (req, res) => __awaiter(void 0, void 0, void 0, function* () {
    try {
        const { prompt } = req.body;
        if (!prompt) {
            return res.status(400).json({ error: "Thiếu nội dung prompt" });
        }
        // Gọi trực tiếp Bedrock, không cần check Session DB
        const content = yield (0, awsService_2.generateLegalText)(prompt);
        return res.json({ answer: content });
    }
    catch (error) {
        console.error("AI Writer Error:", error);
        return res.status(500).json({ error: "Failed to generate text" });
    }
});
exports.aiWriterAssist = aiWriterAssist;
