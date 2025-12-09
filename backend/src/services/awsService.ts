import { LambdaClient, InvokeCommand } from "@aws-sdk/client-lambda";
import { BedrockRuntimeClient, InvokeModelCommand } from "@aws-sdk/client-bedrock-runtime";


const lambdaClient = new LambdaClient({
  region: process.env.AWS_REGION || "ap-southeast-1",
});

const bedrockClient = new BedrockRuntimeClient({ 
  region: process.env.AWS_REGION || "ap-southeast-1" 
});

/**
 * ---------------------------------------------------------------
 * 1. invokeLambda (ASYNC)
 * Dùng cho các tác vụ nền (Background Jobs) không cần chờ kết quả.
 */
export const invokeLambda = async (arn: string, payload: any) => {
  try {
    const payloadBuffer = Buffer.from(JSON.stringify(payload));

    const command = new InvokeCommand({
      FunctionName: arn,
      InvocationType: "Event", 
      Payload: payloadBuffer, 
    });

    await lambdaClient.send(command);
    console.log(`[Lambda] Invoked async: ${arn}`);

  } catch (err) {
    console.error(`[Lambda] Async invoke error:`, err);
    throw err;
  }
};

/**
 * ---------------------------------------------------------------
 * 2. invokeChatLambda (SYNC)
 * Dùng cho Upload, Generate Contract (Cần chờ kết quả trả về)
 * ---------------------------------------------------------------
 */
export const invokeChatLambda = async (arn: string, payload: any) => {
  try {
    const payloadBuffer = Buffer.from(JSON.stringify(payload));

    const command = new InvokeCommand({
      FunctionName: arn,
      InvocationType: "RequestResponse", 
      Payload: payloadBuffer, 
    });

    const { Payload } = await lambdaClient.send(command);

    if (!Payload) {
      console.error("[Lambda] Empty response payload");
      return null;
    }

    const jsonString = new TextDecoder().decode(Payload);
    let parsed;
    try {
      parsed = JSON.parse(jsonString);
      
      if (parsed.body && typeof parsed.body === 'string') {
          try {
              parsed.body = JSON.parse(parsed.body);
          } catch (e) { /* Ignore parsing error */ }
      }
    } catch {
      console.warn("[Lambda] Payload is not valid JSON, returning raw string");
      return jsonString;
    }
    
    return parsed;

  } catch (err) {
    console.error(`[Lambda] Chat invoke error:`, err);
    return {
      statusCode: 500,
      body: JSON.stringify({ error: "Lambda invocation failed", details: String(err) })
    };
  }
};

/**
 * ---------------------------------------------------------------
 * 3. sendChatToBedrock (CHAT TỰ DO)
 * Gọi trực tiếp Bedrock để trả lời câu hỏi pháp lý
 * ---------------------------------------------------------------
 */
export const sendChatToBedrock = async (message: string, context: string) => {
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
      temperature: 0.1, 
      messages: [{ role: "user", content: [{ type: "text", text: prompt }] }]
    };

    const command = new InvokeModelCommand({
      modelId: "anthropic.claude-3-haiku-20240307-v1:0",
      contentType: "application/json",
      accept: "application/json",
      body: JSON.stringify(payload)
    });

    const response = await bedrockClient.send(command);
    const responseBody = JSON.parse(new TextDecoder().decode(response.body));
    
    return responseBody.content[0].text;

  } catch (error) {
    console.error("[Bedrock] Chat Error:", error);
    return "Xin lỗi, hiện tại tôi đang gặp sự cố kết nối với hệ thống AI. Vui lòng thử lại sau.";
  }
};

export const generateLegalText = async (prompt: string) => {
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
      temperature: 0.5, 
      messages: [
        { role: "user", content: [{ type: "text", text: `${systemPrompt}\n\nYêu cầu: ${prompt}` }] }
      ]
    };

    const command = new InvokeModelCommand({
      modelId: "anthropic.claude-3-haiku-20240307-v1:0",
      contentType: "application/json",
      accept: "application/json",
      body: JSON.stringify(payload)
    });

    const response = await bedrockClient.send(command);
    const responseBody = JSON.parse(new TextDecoder().decode(response.body));
    
    return responseBody.content[0].text;

  } catch (error) {
    console.error("[Bedrock] Generate Text Error:", error);
    return "<p>Xin lỗi, hệ thống đang bận. Vui lòng thử lại sau.</p>";
  }
};