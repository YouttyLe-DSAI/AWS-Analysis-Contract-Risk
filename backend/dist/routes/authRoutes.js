"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const express_1 = require("express");
const multer_1 = __importDefault(require("multer")); // 👈 Import Multer
const authController_1 = require("../controllers/authController");
const authMiddleware_1 = require("../middlewares/authMiddleware");
const router = (0, express_1.Router)();
const upload = (0, multer_1.default)({ storage: multer_1.default.memoryStorage() }); // 👈 Cấu hình bộ nhớ tạm
router.post('/register', authController_1.registerUser);
router.post('/login', authController_1.loginUser);
router.get('/profile', authMiddleware_1.protect, authController_1.getProfile);
router.put('/profile', authMiddleware_1.protect, authController_1.updateProfile);
// 👇 [MỚI] Route upload avatar (nhận field tên là 'avatar')
router.post('/avatar', authMiddleware_1.protect, upload.single('avatar'), authController_1.uploadAvatar);
exports.default = router;
