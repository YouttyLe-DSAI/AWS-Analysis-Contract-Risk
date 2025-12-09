import { Request, Response } from 'express';
import { uploadToS3 } from '../services/s3Service';
import { invokeLambda } from '../services/awsService';

export const uploadLegalCorpus = async (req: Request, res: Response) => {
    try {
        // 1. Ép kiểu req để lấy file
        const file = (req as any).file;

        // 2. Kiểm tra an toàn
        if (!file) {
            return res.status(400).json({ error: "Chưa chọn file luật để upload" });
        }

        // 3. Upload lên S3
        const s3Key = await uploadToS3(
            file.buffer, 
            file.originalname, 
            file.mimetype, 
            'legal-corpus-raw' 
        );

        // 4. Trigger Lambda Ingestion (Để AI học luật mới này)
        if (process.env.LAMBDA_TEMPLATE_ARN) {
            await invokeLambda(process.env.LAMBDA_TEMPLATE_ARN, { 
                action: "ingest_legal_doc",
                s3_key: s3Key
            });
        }

        res.json({ 
            message: "Đã upload văn bản luật & kích hoạt AI học.", 
            s3_key: s3Key 
        });

    } catch (error) {
        console.error("Admin Upload Error:", error);
        res.status(500).json({ error: "Upload failed" });
    }
};