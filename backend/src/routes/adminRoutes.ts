import { Router } from 'express';
import multer from 'multer';
import { uploadLegalCorpus } from '../controllers/adminController';

const router = Router();
const upload = multer({ storage: multer.memoryStorage() });

router.post('/upload-law', upload.single('file'), uploadLegalCorpus);

export default router;