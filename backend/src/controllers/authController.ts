import { Request, Response } from 'express';
import bcrypt from 'bcryptjs';
import jwt from 'jsonwebtoken';
import { createUser, findUserByEmail, getUserById, updateUserProfile } from '../services/dynamoService'; 
import { getDownloadUrl, uploadToS3 } from '../services/s3Service'; 
const generateToken = (id: string) => {
    return jwt.sign({ id }, process.env.JWT_SECRET || "secret_mac_dinh", { expiresIn: '30d' });
};

// 1. Đăng ký
export const registerUser = async (req: Request, res: Response) => {
    const { email, password, name } = req.body;

    // Check trùng email
    const userExists = await findUserByEmail(email);
    if (userExists) return res.status(400).json({ message: 'Email đã tồn tại' });

    // Mã hóa mật khẩu
    const salt = await bcrypt.genSalt(10);
    const hashedPassword = await bcrypt.hash(password, salt);

    // Lưu vào DB
    const newUser = await createUser(email, hashedPassword); 

    res.status(201).json({
        id: newUser.id,
        email: newUser.email,
        token: generateToken(newUser.id)
    });
};

// 2. Đăng nhập
export const loginUser = async (req: Request, res: Response) => {
    const { email, password } = req.body;

    // Tìm user
    const user = await findUserByEmail(email); 

    if (!user) {
        return res.status(400).json({ message: 'Sai email hoặc mật khẩu' });
    }

    const isMatch = await bcrypt.compare(password, user.password_hash);
    
    if (!isMatch) {
        return res.status(400).json({ message: 'Sai email hoặc mật khẩu' });
    }

    res.json({
        id: user.id,
        email: user.email, 
        token: generateToken(user.id)
    });
};
export const getProfile = async (req: Request, res: Response) => {
    try {
        const userId = (req as any).user.id;
        const user = await getUserById(userId);
        if (!user) return res.status(404).json({ message: "User not found" });

        if (user.avatar && !user.avatar.startsWith('http') && !user.avatar.startsWith('data:')) {
            try {
                user.avatar = await getDownloadUrl(user.avatar);
            } catch (e) {
                console.warn("Lỗi tạo link avatar:", e);
                user.avatar = ""; 
            }
        }

        res.json(user);
    } catch (error) {
        res.status(500).json({ message: "Lỗi server" });
    }
};

export const updateProfile = async (req: Request, res: Response) => {
    try {
        const userId = (req as any).user.id;
        const updateData = req.body; 
        
        const updatedUser = await updateUserProfile(userId, updateData);
        res.json({ success: true, user: updatedUser });
    } catch (error) {
        console.error(error);
        res.status(500).json({ message: "Update failed" });
    }
};

export const uploadAvatar = async (req: Request, res: Response) => {
    try {
        const file = (req as any).file;
        const userId = (req as any).user.id;

        if (!file) return res.status(400).json({ message: "Chưa chọn file ảnh" });

        // 1. Upload lên S3 (Folder 'user-avatar')
        const s3Key = await uploadToS3(
            file.buffer,
            file.originalname,
            file.mimetype,
            'user-avatar',
            userId
        );

        // 2. Lưu s3Key vào DynamoDB (Thay vì lưu base64 dài ngoằng)
        await updateUserProfile(userId, { avatar: s3Key });

        // 3. Tạo ngay cái link để trả về cho Frontend hiển thị luôn
        const avatarUrl = await getDownloadUrl(s3Key);

        res.json({ success: true, avatarUrl });

    } catch (error) {
        console.error("Upload avatar error:", error);
        res.status(500).json({ message: "Lỗi upload ảnh" });
    }
};


