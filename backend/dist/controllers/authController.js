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
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.uploadAvatar = exports.updateProfile = exports.getProfile = exports.loginUser = exports.registerUser = void 0;
const bcryptjs_1 = __importDefault(require("bcryptjs"));
const jsonwebtoken_1 = __importDefault(require("jsonwebtoken"));
const dynamoService_1 = require("../services/dynamoService");
const s3Service_1 = require("../services/s3Service");
const generateToken = (id) => {
    return jsonwebtoken_1.default.sign({ id }, process.env.JWT_SECRET || "secret_mac_dinh", { expiresIn: '30d' });
};
// 1. Đăng ký
const registerUser = (req, res) => __awaiter(void 0, void 0, void 0, function* () {
    const { email, password, name } = req.body;
    // Check trùng email
    const userExists = yield (0, dynamoService_1.findUserByEmail)(email);
    if (userExists)
        return res.status(400).json({ message: 'Email đã tồn tại' });
    // Mã hóa mật khẩu
    const salt = yield bcryptjs_1.default.genSalt(10);
    const hashedPassword = yield bcryptjs_1.default.hash(password, salt);
    // Lưu vào DB
    const newUser = yield (0, dynamoService_1.createUser)(email, hashedPassword);
    res.status(201).json({
        id: newUser.id,
        email: newUser.email,
        token: generateToken(newUser.id)
    });
});
exports.registerUser = registerUser;
// 2. Đăng nhập
const loginUser = (req, res) => __awaiter(void 0, void 0, void 0, function* () {
    const { email, password } = req.body;
    // Tìm user
    const user = yield (0, dynamoService_1.findUserByEmail)(email); // Lúc này user là User | null
    // Bước kiểm tra quan trọng để TypeScript biết user không phải null
    if (!user) {
        return res.status(400).json({ message: 'Sai email hoặc mật khẩu' });
    }
    // Bây giờ truy cập user.password_hash sẽ KHÔNG bị lỗi nữa
    const isMatch = yield bcryptjs_1.default.compare(password, user.password_hash);
    if (!isMatch) {
        return res.status(400).json({ message: 'Sai email hoặc mật khẩu' });
    }
    res.json({
        id: user.id,
        email: user.email, // user.email cũng OK luôn
        token: generateToken(user.id)
    });
});
exports.loginUser = loginUser;
const getProfile = (req, res) => __awaiter(void 0, void 0, void 0, function* () {
    try {
        const userId = req.user.id;
        const user = yield (0, dynamoService_1.getUserById)(userId);
        if (!user)
            return res.status(404).json({ message: "User not found" });
        // 🔥 LOGIC MỚI: Nếu avatar là S3 Key (không phải link http), tạo Presigned URL
        if (user.avatar && !user.avatar.startsWith('http') && !user.avatar.startsWith('data:')) {
            try {
                user.avatar = yield (0, s3Service_1.getDownloadUrl)(user.avatar);
            }
            catch (e) {
                console.warn("Lỗi tạo link avatar:", e);
                user.avatar = ""; // Nếu lỗi thì để rỗng để hiện ảnh mặc định
            }
        }
        res.json(user);
    }
    catch (error) {
        res.status(500).json({ message: "Lỗi server" });
    }
});
exports.getProfile = getProfile;
// [MỚI] API Cập nhật Profile
const updateProfile = (req, res) => __awaiter(void 0, void 0, void 0, function* () {
    try {
        const userId = req.user.id;
        const updateData = req.body; // { phone, birthdate, gender, ... }
        const updatedUser = yield (0, dynamoService_1.updateUserProfile)(userId, updateData);
        res.json({ success: true, user: updatedUser });
    }
    catch (error) {
        console.error(error);
        res.status(500).json({ message: "Update failed" });
    }
});
exports.updateProfile = updateProfile;
const uploadAvatar = (req, res) => __awaiter(void 0, void 0, void 0, function* () {
    try {
        const file = req.file;
        const userId = req.user.id;
        if (!file)
            return res.status(400).json({ message: "Chưa chọn file ảnh" });
        // 1. Upload lên S3 (Folder 'user-avatar')
        const s3Key = yield (0, s3Service_1.uploadToS3)(file.buffer, file.originalname, file.mimetype, 'user-avatar', userId);
        // 2. Lưu s3Key vào DynamoDB (Thay vì lưu base64 dài ngoằng)
        yield (0, dynamoService_1.updateUserProfile)(userId, { avatar: s3Key });
        // 3. Tạo ngay cái link để trả về cho Frontend hiển thị luôn
        const avatarUrl = yield (0, s3Service_1.getDownloadUrl)(s3Key);
        res.json({ success: true, avatarUrl });
    }
    catch (error) {
        console.error("Upload avatar error:", error);
        res.status(500).json({ message: "Lỗi upload ảnh" });
    }
});
exports.uploadAvatar = uploadAvatar;
