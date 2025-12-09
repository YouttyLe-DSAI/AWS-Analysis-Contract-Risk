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
exports.protect = void 0;
const aws_jwt_verify_1 = require("aws-jwt-verify");
// 1. Khởi tạo Verifier
// Verifier này sẽ tự động tải Public Key từ AWS về để kiểm tra chữ ký Token
const verifier = aws_jwt_verify_1.CognitoJwtVerifier.create({
    userPoolId: process.env.COGNITO_USER_POOL_ID || "",
    tokenUse: "access", // API thường dùng Access Token
    clientId: process.env.COGNITO_CLIENT_ID || "",
});
const protect = (req, res, next) => __awaiter(void 0, void 0, void 0, function* () {
    try {
        // 2. Lấy header Authorization
        const authHeader = req.headers.authorization;
        if (!authHeader || !authHeader.startsWith('Bearer ')) {
            return res.status(401).json({ message: 'Missing Authorization header' });
        }
        // 3. Lấy token ra khỏi chuỗi "Bearer <token>"
        const token = authHeader.split(' ')[1];
        // 4. Verify token (Online/Offline check)
        // Nếu token hết hạn hoặc giả mạo, hàm này sẽ throw error ngay
        const payload = yield verifier.verify(token);
        // 5. Gắn thông tin User vào request
        // 'sub' chính là UUID của user trong Cognito -> Dùng làm User ID trong hệ thống
        req.user = {
            id: payload.sub,
            username: payload.username
        };
        console.log(`[Auth] User ${payload.sub} authenticated`);
        next(); // Cho phép đi tiếp vào Controller
    }
    catch (error) {
        console.error("[Auth] Token verification failed:", error);
        return res.status(401).json({ message: 'Token không hợp lệ hoặc đã hết hạn' });
    }
});
exports.protect = protect;
