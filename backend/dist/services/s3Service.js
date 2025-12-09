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
exports.getDownloadUrl = exports.uploadToS3 = void 0;
const client_s3_1 = require("@aws-sdk/client-s3");
const s3_request_presigner_1 = require("@aws-sdk/s3-request-presigner");
// Khởi tạo S3 Client
const s3Client = new client_s3_1.S3Client({
    region: process.env.AWS_REGION
});
const uploadToS3 = (fileBuffer, fileName, mimeType, folderType, userId) => __awaiter(void 0, void 0, void 0, function* () {
    let keyPrefix = '';
    // Lấy thời gian hiện tại để chia folder
    const now = new Date();
    const year = now.getFullYear();
    const month = String(now.getMonth() + 1).padStart(2, '0');
    switch (folderType) {
        // === 1. LEGAL CORPUS ===
        case 'legal-corpus-raw':
            keyPrefix = 'legal-corpus/original-docs/';
            break;
        case 'legal-corpus-embeddings':
            keyPrefix = 'legal-corpus/embeddings/';
            break;
        // === 2. USER DATA (Logic cũ) ===
        case 'user-document':
            keyPrefix = userId ? `user-data/${userId}/documents/` : 'user-data/anonymous/documents/';
            break;
        case 'user-processed':
            keyPrefix = userId ? `user-data/${userId}/processed/` : 'user-data/anonymous/processed/';
            break;
        case 'user-generated':
            keyPrefix = userId ? `user-data/${userId}/generated-templates/` : 'user-data/anonymous/generated-templates/';
            break;
        // 🔥 CASE MỚI: CẤU TRÚC PHÂN CẤP THEO NĂM/THÁNG/USER
        case 'generated-monthly-user':
            if (!userId)
                throw new Error("Folder generated-monthly-user requires userId");
            // Kết quả: generated/contracts/2025/12/{USER_ID}/
            keyPrefix = `user-data/${userId}/generated-templates/${year}/${month}/`;
            break;
        case 'user-avatar':
            keyPrefix = userId ? `user-data/${userId}/avatar/` : 'misc/avatar/';
            break;
        default:
            keyPrefix = 'misc/';
            break;
    }
    // Tạo Key (Đường dẫn đầy đủ)
    // Thêm Date.now() để đảm bảo tên file là duy nhất, tránh ghi đè
    const key = `${keyPrefix}${Date.now()}_${fileName}`;
    yield s3Client.send(new client_s3_1.PutObjectCommand({
        Bucket: process.env.S3_BUCKET_RAW,
        Key: key,
        Body: fileBuffer,
        ContentType: mimeType,
    }));
    return key;
});
exports.uploadToS3 = uploadToS3;
const getDownloadUrl = (s3Key) => __awaiter(void 0, void 0, void 0, function* () {
    const command = new client_s3_1.GetObjectCommand({
        Bucket: process.env.S3_BUCKET_RAW,
        Key: s3Key
    });
    return (0, s3_request_presigner_1.getSignedUrl)(s3Client, command, { expiresIn: 3600 });
});
exports.getDownloadUrl = getDownloadUrl;
