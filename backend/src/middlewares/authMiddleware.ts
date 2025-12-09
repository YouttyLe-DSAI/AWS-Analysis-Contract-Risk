import { Request, Response, NextFunction } from 'express';
import { CognitoJwtVerifier } from "aws-jwt-verify";

// 1. Khởi tạo Verifier
const verifier = CognitoJwtVerifier.create({
  userPoolId: process.env.COGNITO_USER_POOL_ID || "", 
  tokenUse: "access", // API thường dùng Access Token
  clientId: process.env.COGNITO_CLIENT_ID || "", 
});

export const protect = async (req: Request, res: Response, next: NextFunction) => {
    try {
        // 2. Lấy header Authorization
        const authHeader = req.headers.authorization;
        
        if (!authHeader || !authHeader.startsWith('Bearer ')) {
            return res.status(401).json({ message: 'Missing Authorization header' });
        }

        // 3. Lấy token ra khỏi chuỗi "Bearer <token>"
        const token = authHeader.split(' ')[1];

        // 4. Verify token (Online/Offline check)
        const payload = await verifier.verify(token);

        // 5. Gắn thông tin User vào request
        (req as any).user = {
            id: payload.sub,
            username: payload.username 
        };

        console.log(`[Auth] User ${payload.sub} authenticated`);
        
        next(); 

    } catch (error) {
        console.error("[Auth] Token verification failed:", error);
        return res.status(401).json({ message: 'Token không hợp lệ hoặc đã hết hạn' });
    }
};