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
Object.defineProperty(exports, "__esModule", { value: true });
exports.generateLegalText = exports.sendChatToBedrock = exports.invokeChatLambda = exports.invokeLambda = void 0;
const client_lambda_1 = require("@aws-sdk/client-lambda");
const client_bedrock_runtime_1 = require("@aws-sdk/client-bedrock-runtime");
/**
 * KHỞI TẠO CLIENTS
 * Region mặc định là ap-southeast-1 nếu không có env
 */
const lambdaClient = new client_lambda_1.LambdaClient({
    region: process.env.AWS_REGION || "ap-southeast-1",
});
const bedrockClient = new client_bedrock_runtime_1.BedrockRuntimeClient({
    region: process.env.AWS_REGION || "ap-southeast-1"
});
/**
 * ---------------------------------------------------------------
 * 1. invokeLambda (ASYNC)
 * Dùng cho các tác vụ nền (Background Jobs) không cần chờ kết quả.
 * ---------------------------------------------------------------
 */
const invokeLambda = (arn, payload) => __awaiter(void 0, void 0, void 0, function* () {
    try {
        // Chuyển Object -> JSON String -> Buffer (Bắt buộc cho AWS SDK v3)
        const payloadBuffer = Buffer.from(JSON.stringify(payload));
        const command = new client_lambda_1.InvokeCommand({
            FunctionName: arn,
            InvocationType: "Event", // Fire and forget
            Payload: payloadBuffer,
        });
        yield lambdaClient.send(command);
        console.log(`[Lambda] Invoked async: ${arn}`);
    }
    catch (err) {
        console.error(`[Lambda] Async invoke error:`, err);
        throw err;
    }
});
exports.invokeLambda = invokeLambda;
/**
 * ---------------------------------------------------------------
 * 2. invokeChatLambda (SYNC)
 * Dùng cho Upload, Generate Contract (Cần chờ kết quả trả về)
 * ---------------------------------------------------------------
 */
const invokeChatLambda = (arn, payload) => __awaiter(void 0, void 0, void 0, function* () {
    try {
        // 🔥 FIX QUAN TRỌNG: Chuyển payload thành Buffer
        const payloadBuffer = Buffer.from(JSON.stringify(payload));
        const command = new client_lambda_1.InvokeCommand({
            FunctionName: arn,
            InvocationType: "RequestResponse", // Wait for response
            Payload: payloadBuffer,
        });
        const { Payload } = yield lambdaClient.send(command);
        if (!Payload) {
            console.error("[Lambda] Empty response payload");
            return null;
        }
        // Decode kết quả từ Uint8Array về String
        const jsonString = new TextDecoder().decode(Payload);
        // Parse JSON an toàn (Xử lý trường hợp double-encoded từ API Gateway)
        let parsed;
        try {
            parsed = JSON.parse(jsonString);
            // Nếu body bên trong vẫn là string (do Python trả về json.dumps trong body), parse tiếp lần nữa
            if (parsed.body && typeof parsed.body === 'string') {
                try {
                    parsed.body = JSON.parse(parsed.body);
                }
                catch (e) { /* Ignore parsing error */ }
            }
        }
        catch (_a) {
            console.warn("[Lambda] Payload is not valid JSON, returning raw string");
            return jsonString;
        }
        return parsed;
    }
    catch (err) {
        console.error(`[Lambda] Chat invoke error:`, err);
        // Trả về object lỗi chuẩn để Controller xử lý (thay vì crash app)
        return {
            statusCode: 500,
            body: JSON.stringify({ error: "Lambda invocation failed", details: String(err) })
        };
    }
});
exports.invokeChatLambda = invokeChatLambda;
/**
 * ---------------------------------------------------------------
 * 3. sendChatToBedrock (CHAT TỰ DO)
 * Gọi trực tiếp Bedrock để trả lời câu hỏi pháp lý
 * ---------------------------------------------------------------
 */
const sendChatToBedrock = (message, context) => __awaiter(void 0, void 0, void 0, function* () {
    try {
        const prompt = `
      Bạn là một Cố vấn Pháp lý Cấp cao (Senior Legal Counsel).
      Nhiệm vụ: Trả lời câu hỏi của người dùng dựa trên thông tin hợp đồng đã phân tích.

      DỮ LIỆU HỢP ĐỒNG:
      ${context}

      CÂU HỎI: "${message}"

      YÊU CẦU:
      - Trả lời tự nhiên, chi tiết, chuyên nghiệp bằng tiếng Việt.
      - Phân tích sâu về rủi ro hoặc lợi ích pháp lý.
      - Sử dụng định dạng Markdown (in đậm, gạch đầu dòng) để trình bày đẹp.
      
      Trả lời:
    `;
        const payload = {
            anthropic_version: "bedrock-2023-05-31",
            max_tokens: 4000,
            temperature: 0.1, // Giảm độ sáng tạo để câu trả lời chính xác hơn
            messages: [{ role: "user", content: [{ type: "text", text: prompt }] }]
        };
        const command = new client_bedrock_runtime_1.InvokeModelCommand({
            modelId: "anthropic.claude-3-haiku-20240307-v1:0",
            contentType: "application/json",
            accept: "application/json",
            body: JSON.stringify(payload)
        });
        const response = yield bedrockClient.send(command);
        const responseBody = JSON.parse(new TextDecoder().decode(response.body));
        return responseBody.content[0].text;
    }
    catch (error) {
        console.error("[Bedrock] Chat Error:", error);
        return "Xin lỗi, hiện tại tôi đang gặp sự cố kết nối với hệ thống AI. Vui lòng thử lại sau.";
    }
});
exports.sendChatToBedrock = sendChatToBedrock;
const generateLegalText = (prompt) => __awaiter(void 0, void 0, void 0, function* () {
    try {
        const systemPrompt = `
      Bạn là một Luật sư chuyên soạn thảo hợp đồng chuyên nghiệp.
      Nhiệm vụ: Viết một điều khoản hợp đồng hoặc nội dung pháp lý dựa trên yêu cầu của người dùng.
      
      YÊU CẦU OUTPUT:
      - Chỉ trả về nội dung văn bản (có thể dùng HTML tags cơ bản như <p>, <ul>, <li>, <strong> để định dạng).
      - Không rào đón, không giải thích thừa (như "Đây là điều khoản...").
      - Ngôn ngữ: Tiếng Việt chuẩn pháp lý, chặt chẽ.
    `;
        const payload = {
            anthropic_version: "bedrock-2023-05-31",
            max_tokens: 2000,
            temperature: 0.5, // Tăng nhẹ để viết sáng tạo hơn chút
            messages: [
                { role: "user", content: [{ type: "text", text: `${systemPrompt}\n\nYêu cầu: ${prompt}` }] }
            ]
        };
        const command = new client_bedrock_runtime_1.InvokeModelCommand({
            modelId: "anthropic.claude-3-haiku-20240307-v1:0",
            contentType: "application/json",
            accept: "application/json",
            body: JSON.stringify(payload)
        });
        const response = yield bedrockClient.send(command);
        const responseBody = JSON.parse(new TextDecoder().decode(response.body));
        return responseBody.content[0].text;
    }
    catch (error) {
        console.error("[Bedrock] Generate Text Error:", error);
        return "<p>Xin lỗi, hệ thống đang bận. Vui lòng thử lại sau.</p>";
    }
});
exports.generateLegalText = generateLegalText;
