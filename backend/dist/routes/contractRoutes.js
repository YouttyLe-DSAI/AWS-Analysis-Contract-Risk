"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const express_1 = require("express");
const multer_1 = __importDefault(require("multer"));
const contractController_1 = require("../controllers/contractController");
const authMiddleware_1 = require("../middlewares/authMiddleware"); // <--- Import Middleware
const router = (0, express_1.Router)();
const storage = multer_1.default.memoryStorage();
const upload = (0, multer_1.default)({ storage: storage });
// Thêm 'protect' vào trước các hàm xử lý
router.post('/upload', authMiddleware_1.protect, upload.single('file'), contractController_1.uploadContract);
router.post('/chat', authMiddleware_1.protect, contractController_1.chatWithContract);
router.post('/generate', authMiddleware_1.protect, contractController_1.generateContractAPI);
router.get('/dashboard', authMiddleware_1.protect, contractController_1.getUserDashboard); // Lấy dữ liệu khi login
router.post('/drafts', authMiddleware_1.protect, contractController_1.saveUserDrafts);
router.delete('/:id', authMiddleware_1.protect, contractController_1.deleteContract);
router.post('/assist', authMiddleware_1.protect, contractController_1.aiWriterAssist);
exports.default = router;
