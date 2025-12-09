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
exports.deleteSession = exports.getUserById = exports.updateUserProfile = exports.updateUserDrafts = exports.getUserDrafts = exports.getUserInspections = exports.getChatHistory = exports.saveChatMessage = exports.updateSessionWithAnalysis = exports.getSessionById = exports.createChatSession = exports.findUserByEmail = exports.createUser = void 0;
const client_dynamodb_1 = require("@aws-sdk/client-dynamodb");
const lib_dynamodb_1 = require("@aws-sdk/lib-dynamodb");
const uuid_1 = require("uuid");
const client = new client_dynamodb_1.DynamoDBClient({
    region: process.env.AWS_REGION
});
const docClient = lib_dynamodb_1.DynamoDBDocumentClient.from(client);
// ==========================================
// 1. AUTH SERVICE
// ==========================================
const createUser = (email, hash) => __awaiter(void 0, void 0, void 0, function* () {
    const userId = (0, uuid_1.v4)();
    const command = new lib_dynamodb_1.PutCommand({
        TableName: process.env.DYNAMO_TABLE_USERS,
        Item: {
            user_id: userId,
            email,
            password_hash: hash,
            plan: 'free',
            created_at: new Date().toISOString()
        }
    });
    yield docClient.send(command);
    return {
        id: userId,
        email,
        password_hash: hash,
        plan: 'free',
        created_at: new Date().toISOString()
    };
});
exports.createUser = createUser;
const findUserByEmail = (email) => __awaiter(void 0, void 0, void 0, function* () {
    // FIX: Check null để tránh lỗi ValidationException
    if (!email) {
        console.warn("findUserByEmail called with empty email");
        return null;
    }
    const command = new lib_dynamodb_1.ScanCommand({
        TableName: process.env.DYNAMO_TABLE_USERS,
        FilterExpression: "email = :email",
        ExpressionAttributeValues: { ":email": email }
    });
    const response = yield docClient.send(command);
    if (response.Items && response.Items.length > 0) {
        const item = response.Items[0];
        return {
            id: item.user_id,
            email: item.email,
            password_hash: item.password_hash,
            plan: item.plan,
            created_at: item.created_at
        };
    }
    return null;
});
exports.findUserByEmail = findUserByEmail;
// ==========================================
// 2. CHAT SESSION SERVICE
// ==========================================
const createChatSession = (userId, fileName, s3Key) => __awaiter(void 0, void 0, void 0, function* () {
    const sessionId = (0, uuid_1.v4)();
    yield docClient.send(new lib_dynamodb_1.PutCommand({
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
});
exports.createChatSession = createChatSession;
const getSessionById = (sessionId) => __awaiter(void 0, void 0, void 0, function* () {
    const command = new lib_dynamodb_1.GetCommand({
        TableName: process.env.DYNAMO_TABLE_SESSIONS,
        Key: { session_id: sessionId }
    });
    const response = yield docClient.send(command);
    return response.Item;
});
exports.getSessionById = getSessionById;
const updateSessionWithAnalysis = (sessionId, analysisData) => __awaiter(void 0, void 0, void 0, function* () {
    // 1. Map các field lẻ để query/sort (Giữ nguyên logic cũ)
    const riskSummary = analysisData.summary || analysisData.risk_summary || "No summary";
    const risks = analysisData.risk_items || analysisData.risks || [];
    const score = analysisData.overall_risk_level || analysisData.overall_score || "UNKNOWN";
    const command = new lib_dynamodb_1.UpdateCommand({
        TableName: process.env.DYNAMO_TABLE_SESSIONS,
        Key: { session_id: sessionId },
        // 👇 [QUAN TRỌNG] Thêm analysis_json = :json vào câu lệnh update
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
            ":json": analysisData // 👇 Lưu nguyên cục JSON vào đây
        }
    });
    yield docClient.send(command);
    console.log(`[DynamoDB] Updated session ${sessionId} with analysis json`);
});
exports.updateSessionWithAnalysis = updateSessionWithAnalysis;
// ==========================================
// 3. CHAT MESSAGE SERVICE
// ==========================================
const saveChatMessage = (sessionId, role, content) => __awaiter(void 0, void 0, void 0, function* () {
    const messageId = (0, uuid_1.v4)();
    yield docClient.send(new lib_dynamodb_1.PutCommand({
        TableName: process.env.DYNAMO_TABLE_MESSAGES,
        Item: {
            session_id: sessionId,
            timestamp: new Date().toISOString(),
            message_id: messageId,
            role,
            content
        }
    }));
});
exports.saveChatMessage = saveChatMessage;
const getChatHistory = (sessionId) => __awaiter(void 0, void 0, void 0, function* () {
    const command = new lib_dynamodb_1.QueryCommand({
        TableName: process.env.DYNAMO_TABLE_MESSAGES,
        KeyConditionExpression: "session_id = :sid",
        ExpressionAttributeValues: { ":sid": sessionId },
        ScanIndexForward: true
    });
    const response = yield docClient.send(command);
    return response.Items;
});
exports.getChatHistory = getChatHistory;
// 4. USER DATA (INSPECTIONS & DRAFTS) - MỚI
// ==========================================
// A. Lấy danh sách Inspection (Đã phân tích)
const getUserInspections = (userId) => __awaiter(void 0, void 0, void 0, function* () {
    console.log(`[DynamoDB] Getting inspections for user: ${userId}`);
    try {
        let items = [];
        let lastEvaluatedKey = undefined;
        // VÒNG LẶP QUÉT TOÀN BỘ DỮ LIỆU
        do {
            const command = new lib_dynamodb_1.ScanCommand({
                TableName: process.env.DYNAMO_TABLE_SESSIONS,
                FilterExpression: "user_id = :uid",
                ExpressionAttributeValues: {
                    ":uid": userId
                },
                ExclusiveStartKey: lastEvaluatedKey
            });
            const response = yield docClient.send(command);
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
    }
    catch (error) {
        console.error("[DynamoDB] Error in getUserInspections:", error);
        return [];
    }
});
exports.getUserInspections = getUserInspections;
// B. Lấy danh sách Draft (Bản nháp)
const getUserDrafts = (userId) => __awaiter(void 0, void 0, void 0, function* () {
    var _a;
    const command = new lib_dynamodb_1.GetCommand({
        TableName: process.env.DYNAMO_TABLE_USERS,
        Key: { user_id: userId }
    });
    const response = yield docClient.send(command);
    return ((_a = response.Item) === null || _a === void 0 ? void 0 : _a.drafts) || [];
});
exports.getUserDrafts = getUserDrafts;
// C. Lưu danh sách Draft (Cập nhật đè mảng drafts trong bảng Users)
const updateUserDrafts = (userId, drafts) => __awaiter(void 0, void 0, void 0, function* () {
    const command = new lib_dynamodb_1.UpdateCommand({
        TableName: process.env.DYNAMO_TABLE_USERS,
        Key: { user_id: userId },
        UpdateExpression: "set drafts = :d",
        ExpressionAttributeValues: {
            ":d": drafts
        }
    });
    yield docClient.send(command);
});
exports.updateUserDrafts = updateUserDrafts;
const updateUserProfile = (userId, data) => __awaiter(void 0, void 0, void 0, function* () {
    // data bao gồm: { phone, birthdate, gender, avatar_url, ... }
    // Tạo UpdateExpression động dựa trên dữ liệu gửi lên
    let updateExp = "set updated_at = :t";
    const expValues = { ":t": new Date().toISOString() };
    const expNames = {};
    Object.keys(data).forEach((key, index) => {
        // Bỏ qua các field không được sửa hoặc rỗng
        if (key === 'id' || key === 'email' || data[key] === undefined)
            return;
        const attrName = `#attr${index}`;
        const attrVal = `:val${index}`;
        updateExp += `, ${attrName} = ${attrVal}`;
        expNames[attrName] = key;
        expValues[attrVal] = data[key];
    });
    const command = new lib_dynamodb_1.UpdateCommand({
        TableName: process.env.DYNAMO_TABLE_USERS, // Hoặc 'Users'
        Key: { user_id: userId },
        UpdateExpression: updateExp,
        ExpressionAttributeNames: expNames,
        ExpressionAttributeValues: expValues,
        ReturnValues: "ALL_NEW" // Trả về data mới sau khi update
    });
    const response = yield docClient.send(command);
    return response.Attributes;
});
exports.updateUserProfile = updateUserProfile;
// --- [MỚI] Hàm lấy chi tiết User theo ID ---
const getUserById = (userId) => __awaiter(void 0, void 0, void 0, function* () {
    const command = new lib_dynamodb_1.GetCommand({
        TableName: process.env.DYNAMO_TABLE_USERS,
        Key: { user_id: userId }
    });
    const response = yield docClient.send(command);
    // Xóa password_hash trước khi trả về
    if (response.Item) {
        delete response.Item.password_hash;
    }
    return response.Item;
});
exports.getUserById = getUserById;
const deleteSession = (sessionId, userId) => __awaiter(void 0, void 0, void 0, function* () {
    const command = new lib_dynamodb_1.DeleteCommand({
        TableName: process.env.DYNAMO_TABLE_SESSIONS,
        Key: { session_id: sessionId },
        ConditionExpression: "user_id = :uid",
        ExpressionAttributeValues: {
            ":uid": userId
        }
    });
    try {
        yield docClient.send(command);
        return true;
    }
    catch (error) {
        console.error("DynamoDB Delete Error:", error);
        throw error;
    }
});
exports.deleteSession = deleteSession;
