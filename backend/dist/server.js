"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.handler = void 0;
// src/server.ts
const app_1 = __importDefault(require("./app"));
const serverless_http_1 = __importDefault(require("serverless-http"));
// Logic thông minh: Tự nhận biết môi trường
// 1. Nếu có biến AWS_LAMBDA... tức là đang chạy trên mây -> Dùng Serverless handler
if (process.env.AWS_LAMBDA_FUNCTION_VERSION) {
    console.log("🚀 Running on AWS Lambda");
}
// 2. Nếu không có -> Đang chạy Local trên máy tính -> Mở Port 3001
else {
    const PORT = process.env.PORT || 3001;
    app_1.default.listen(PORT, () => {
        console.log(`🚀 Local Server running on http://localhost:${PORT}`);
    });
}
// Xuất ra handler để AWS Lambda sử dụng (Quan trọng nhất)
exports.handler = (0, serverless_http_1.default)(app_1.default);
