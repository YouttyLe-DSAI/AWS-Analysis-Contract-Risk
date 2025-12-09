import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { 
    DynamoDBDocumentClient, 
    PutCommand, 
    QueryCommand, 
    ScanCommand, 
    UpdateCommand, 
    GetCommand,
    DeleteCommand     
} from "@aws-sdk/lib-dynamodb";
import { v4 as uuidv4 } from 'uuid';

const client = new DynamoDBClient({ 
    region: process.env.AWS_REGION 
});
const docClient = DynamoDBDocumentClient.from(client);

export interface User {
    id: string;
    email: string;
    password_hash: string;
    plan: string;
    created_at: string;
}

// ==========================================
// 1. AUTH SERVICE
// ==========================================

export const createUser = async (email: string, hash: string) => {
  const userId = uuidv4();
  const command = new PutCommand({
    TableName: process.env.DYNAMO_TABLE_USERS,
    Item: {
      user_id: userId,
      email,
      password_hash: hash,
      plan: 'free',
      created_at: new Date().toISOString()
    }
  });
  await docClient.send(command);
  
  return { 
      id: userId, 
      email, 
      password_hash: hash, 
      plan: 'free', 
      created_at: new Date().toISOString() 
  } as User;
};

export const findUserByEmail = async (email: string): Promise<User | null> => {
    if (!email) {
        console.warn("findUserByEmail called with empty email");
        return null; 
    }

    const command = new ScanCommand({
        TableName: process.env.DYNAMO_TABLE_USERS,
        FilterExpression: "email = :email",
        ExpressionAttributeValues: { ":email": email }
    });
    const response = await docClient.send(command);
    
    if (response.Items && response.Items.length > 0) {
        const item = response.Items[0];
        return {
            id: item.user_id,
            email: item.email,
            password_hash: item.password_hash,
            plan: item.plan,
            created_at: item.created_at
        } as User;
    }
    return null;
};

// ==========================================
// 2. CHAT SESSION SERVICE
// ==========================================

export const createChatSession = async (userId: string, fileName: string, s3Key: string) => {
    const sessionId = uuidv4();
    await docClient.send(new PutCommand({
        TableName: process.env.DYNAMO_TABLE_SESSIONS,
        Item: {
            session_id: sessionId,
            user_id: userId,
            file_name: fileName,
            s3_key: s3Key,
            status: 'UPLOADED',
            created_at: new Date().toISOString()
        }
    }));
    return sessionId;
};

export const getSessionById = async (sessionId: string) => {
    const command = new GetCommand({
        TableName: process.env.DYNAMO_TABLE_SESSIONS,
        Key: { session_id: sessionId }
    });
    const response = await docClient.send(command);
    return response.Item;
};

export const updateSessionWithAnalysis = async (sessionId: string, analysisData: any) => {
    const riskSummary = analysisData.summary || analysisData.risk_summary || "No summary";
    const risks = analysisData.risk_items || analysisData.risks || [];
    const score = analysisData.overall_risk_level || analysisData.overall_score || "UNKNOWN";

    const command = new UpdateCommand({
        TableName: process.env.DYNAMO_TABLE_SESSIONS,
        Key: { session_id: sessionId },
        UpdateExpression: "set #status = :s, summary = :sum, risks = :r, overall_score = :score, last_updated = :t, analysis_json = :json",
        ExpressionAttributeNames: {
            "#status": "status"
        },
        ExpressionAttributeValues: {
            ":s": "ANALYZED",
            ":sum": riskSummary,
            ":r": risks,
            ":score": score,
            ":t": new Date().toISOString(),
            ":json": analysisData 
        }
    });
    
    await docClient.send(command);
    console.log(`[DynamoDB] Updated session ${sessionId} with analysis json`);
};

// ==========================================
// 3. CHAT MESSAGE SERVICE
// ==========================================

export const saveChatMessage = async (sessionId: string, role: 'user' | 'assistant', content: string) => {
    const messageId = uuidv4();
    await docClient.send(new PutCommand({
        TableName: process.env.DYNAMO_TABLE_MESSAGES,
        Item: {
            session_id: sessionId, 
            timestamp: new Date().toISOString(),
            message_id: messageId,
            role,
            content
        }
    }));
};

export const getChatHistory = async (sessionId: string) => {
    const command = new QueryCommand({
        TableName: process.env.DYNAMO_TABLE_MESSAGES,
        KeyConditionExpression: "session_id = :sid",
        ExpressionAttributeValues: { ":sid": sessionId },
        ScanIndexForward: true 
    });
    const response = await docClient.send(command);
    return response.Items;
};

// 4. USER DATA (INSPECTIONS & DRAFTS) - MỚI
// ==========================================

// A. Lấy danh sách Inspection (Đã phân tích)

export const getUserInspections = async (userId: string) => {
    console.log(`[DynamoDB] Getting inspections for user: ${userId}`);

    try {
        let items: any[] = [];
        let lastEvaluatedKey: Record<string, any> | undefined = undefined;

        // VÒNG LẶP QUÉT TOÀN BỘ DỮ LIỆU
        do {
            const command: ScanCommand = new ScanCommand({
                TableName: process.env.DYNAMO_TABLE_SESSIONS,
                FilterExpression: "user_id = :uid",
                ExpressionAttributeValues: {
                    ":uid": userId
                },
                ExclusiveStartKey: lastEvaluatedKey 
            });
            
            const response = await docClient.send(command);
            
            if (response.Items) {
                items = items.concat(response.Items);
            }
            
            // Cập nhật key để quét trang tiếp theo
            lastEvaluatedKey = response.LastEvaluatedKey;
            
        } while (lastEvaluatedKey); 

        console.log(`[DynamoDB] Found total ${items.length} items for user.`);

        // Map dữ liệu trả về Frontend
        return items.map(item => {
            let finalAnalysis = item.analysis_json || item.analysis;

            if (!finalAnalysis) {
                 finalAnalysis = {
                     summary: item.summary || "Đang xử lý...",
                     overall_risk_level: item.overall_score || "UNKNOWN",
                     risk_items: item.risks || [],
                 };
            }

            return {
                id: item.session_id,
                name: item.file_name || "Hợp đồng không tên",
                content: "", 
                s3_key: item.s3_key,
                score: item.overall_score === 'LOW' ? 85 : 
                       item.overall_score === 'MEDIUM' ? 60 : 
                       item.overall_score === 'HIGH' ? 30 : -1,
                
                status: item.status || 'processed',
                createdAt: item.created_at,
                analysisData: finalAnalysis
            };
        });

    } catch (error) {
        console.error("[DynamoDB] Error in getUserInspections:", error);
        return [];
    }
};

// B. Lấy danh sách Draft (Bản nháp)
export const getUserDrafts = async (userId: string) => {
    const command = new GetCommand({
        TableName: process.env.DYNAMO_TABLE_USERS,
        Key: { user_id: userId }
    });
    
    const response = await docClient.send(command);
    return response.Item?.drafts || [];
};

// C. Lưu danh sách Draft (Cập nhật đè mảng drafts trong bảng Users)
export const updateUserDrafts = async (userId: string, drafts: any[]) => {
    const command = new UpdateCommand({
        TableName: process.env.DYNAMO_TABLE_USERS,
        Key: { user_id: userId },
        UpdateExpression: "set drafts = :d",
        ExpressionAttributeValues: {
            ":d": drafts
        }
    });
    await docClient.send(command);
};

export const updateUserProfile = async (userId: string, data: any) => {    
    let updateExp = "set updated_at = :t";
    const expValues: any = { ":t": new Date().toISOString() };
    const expNames: any = {};

    Object.keys(data).forEach((key, index) => {
        if (key === 'id' || key === 'email' || data[key] === undefined) return;

        const attrName = `#attr${index}`;
        const attrVal = `:val${index}`;

        updateExp += `, ${attrName} = ${attrVal}`;
        expNames[attrName] = key;
        expValues[attrVal] = data[key];
    });

    const command = new UpdateCommand({
        TableName: process.env.DYNAMO_TABLE_USERS,
        Key: { user_id: userId },
        UpdateExpression: updateExp,
        ExpressionAttributeNames: expNames,
        ExpressionAttributeValues: expValues,
        ReturnValues: "ALL_NEW" 
    });

    const response = await docClient.send(command);
    return response.Attributes;
};

export const getUserById = async (userId: string) => {
    const command = new GetCommand({
        TableName: process.env.DYNAMO_TABLE_USERS,
        Key: { user_id: userId }
    });
    const response = await docClient.send(command);
    if (response.Item) {
        delete response.Item.password_hash;
    }
    return response.Item;
};

export const deleteSession = async (sessionId: string, userId: string) => {    
    const command = new DeleteCommand({
        TableName: process.env.DYNAMO_TABLE_SESSIONS,
        Key: { session_id: sessionId },
        ConditionExpression: "user_id = :uid",
        ExpressionAttributeValues: {
            ":uid": userId
        }
    });

    try {
        await docClient.send(command);
        return true;
    } catch (error) {
        console.error("DynamoDB Delete Error:", error);
        throw error;
    }
};