"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
// backend/src/app.ts
const express_1 = __importDefault(require("express"));
const cors_1 = __importDefault(require("cors"));
const dotenv_1 = __importDefault(require("dotenv"));
// Import các routes
const contractRoutes_1 = __importDefault(require("./routes/contractRoutes"));
const adminRoutes_1 = __importDefault(require("./routes/adminRoutes"));
const newsRoutes_1 = __importDefault(require("./routes/newsRoutes"));
const authRoutes_1 = __importDefault(require("./routes/authRoutes"));
dotenv_1.default.config();
const app = (0, express_1.default)();
// --- 👇 BẮT ĐẦU SỬA: Tăng giới hạn Upload & Cấu hình CORS 👇 ---
// 1. Tăng giới hạn kích thước Body lên 50MB (Khắc phục lỗi 413 Payload Too Large)
app.use(express_1.default.json({ limit: '50mb' }));
app.use(express_1.default.urlencoded({ limit: '50mb', extended: true }));
// 2. Cấu hình CORS chặt chẽ hơn để tránh lỗi Preflight
app.use((0, cors_1.default)({
    origin: '*',
    credentials: true,
    methods: ['GET', 'POST', 'PUT', 'DELETE', 'OPTIONS'],
    allowedHeaders: ['Content-Type', 'Authorization']
}));
// --- 👆 KẾT THÚC SỬA 👆 ---
app.get('/', (req, res) => {
    res.json({ message: "AI Contract Backend is running!" });
});
// 1. Gom tất cả routes vào một Router chung
const apiRouter = express_1.default.Router();
apiRouter.use('/contracts', contractRoutes_1.default);
apiRouter.use('/admin', adminRoutes_1.default);
apiRouter.use('/news', newsRoutes_1.default);
apiRouter.use('/auth', authRoutes_1.default);
// 2. Mount Router này cho cả 2 trường hợp:
// Trường hợp 1: Chạy Local (http://localhost:3000/api/...)
app.use('/api', apiRouter);
// Trường hợp 2: Chạy trên AWS Lambda (https://.../dev/api/...)
app.use('/dev/api', apiRouter);
app.use('/prod/api', apiRouter);
exports.default = app;
