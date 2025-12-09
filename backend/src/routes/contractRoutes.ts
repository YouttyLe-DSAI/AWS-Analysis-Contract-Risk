import { Router } from 'express';
import multer from 'multer';
import { uploadContract, chatWithContract, generateContractAPI, getUserDashboard, saveUserDrafts, deleteContract, aiWriterAssist } from '../controllers/contractController';
import { protect } from '../middlewares/authMiddleware'; 

const router = Router();
const storage = multer.memoryStorage();
const upload = multer({ storage: storage });

router.post('/upload', protect, upload.single('file'), uploadContract); 
router.post('/chat', protect, chatWithContract);
router.post('/generate', protect, generateContractAPI);
router.get('/dashboard', protect, getUserDashboard); 
router.post('/drafts', protect, saveUserDrafts);
router.delete('/:id', protect, deleteContract);
router.post('/assist', protect, aiWriterAssist);
export default router;