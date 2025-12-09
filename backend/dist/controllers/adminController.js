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
exports.uploadLegalCorpus = void 0;
const s3Service_1 = require("../services/s3Service");
const awsService_1 = require("../services/awsService");
const uploadLegalCorpus = (req, res) => __awaiter(void 0, void 0, void 0, function* () {
    try {
        // 1. Ép kiểu req để lấy file
        const file = req.file;
        // 2. Kiểm tra an toàn
        if (!file) {
            return res.status(400).json({ error: "Chưa chọn file luật để upload" });
        }
        // 3. Upload lên S3
        // Type 'legal-corpus-raw' sẽ đưa file vào folder: legal-corpus/original-docs/
        // Không cần truyền userId (tham số thứ 5) vì đây là dữ liệu chung
        const s3Key = yield (0, s3Service_1.uploadToS3)(file.buffer, file.originalname, file.mimetype, 'legal-corpus-raw');
        // 4. Trigger Lambda Ingestion (Để AI học luật mới này)
        // Giả sử Lambda Template/Ingestion đảm nhận việc này
        if (process.env.LAMBDA_TEMPLATE_ARN) {
            yield (0, awsService_1.invokeLambda)(process.env.LAMBDA_TEMPLATE_ARN, {
                action: "ingest_legal_doc",
                s3_key: s3Key
            });
        }
        res.json({
            message: "Đã upload văn bản luật & kích hoạt AI học.",
            s3_key: s3Key
        });
    }
    catch (error) {
        console.error("Admin Upload Error:", error);
        res.status(500).json({ error: "Upload failed" });
    }
});
exports.uploadLegalCorpus = uploadLegalCorpus;
