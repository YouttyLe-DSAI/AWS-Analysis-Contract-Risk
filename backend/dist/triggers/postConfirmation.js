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
exports.handler = void 0;
const client_dynamodb_1 = require("@aws-sdk/client-dynamodb");
const lib_dynamodb_1 = require("@aws-sdk/lib-dynamodb");
const client = new client_dynamodb_1.DynamoDBClient({ region: process.env.AWS_REGION });
const docClient = lib_dynamodb_1.DynamoDBDocumentClient.from(client);
const handler = (event) => __awaiter(void 0, void 0, void 0, function* () {
    // Chỉ chạy khi user xác nhận đăng ký
    if (event.triggerSource === "PostConfirmation_ConfirmSignUp") {
        // Cognito gửi về thông tin user đã xác thực
        const { sub, email } = event.request.userAttributes;
        // CHÚ Ý: Trong event này KHÔNG bao giờ có password
        const newUserItem = {
            // 1. user_id: Lấy từ Cognito ID (sub) để đồng bộ giữa 2 bên
            user_id: sub,
            // 2. email: Lưu lại để hiển thị/liên lạc
            email: email,
            // 3. created_at: Thời điểm tạo
            created_at: new Date().toISOString(),
            // 4. plan & credits: Thông tin nghiệp vụ (Business Logic)
            plan: 'free',
            credits_left: 5,
            // 5. password_hash: KHÔNG LƯU NỮA
        };
        const command = new lib_dynamodb_1.PutCommand({
            TableName: process.env.DYNAMO_TABLE_USERS || 'Users',
            Item: newUserItem,
            ConditionExpression: "attribute_not_exists(user_id)"
        });
        try {
            yield docClient.send(command);
            console.log(`[Sync] Đã tạo user ${email} vào DynamoDB`);
        }
        catch (error) {
            console.error("Lỗi sync DynamoDB:", error);
            // Không throw error để user vẫn đăng nhập được dù sync thất bại (có thể sync lại sau)
        }
    }
    return event;
});
exports.handler = handler;
