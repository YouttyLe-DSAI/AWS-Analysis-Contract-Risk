import { Router } from 'express';
import multer from 'multer'; 
import { registerUser, loginUser, getProfile, updateProfile, uploadAvatar } from '../controllers/authController';
import { protect } from '../middlewares/authMiddleware';

const router = Router();
const upload = multer({ storage: multer.memoryStorage() }); 

router.post('/register', registerUser);
router.post('/login', loginUser);

router.get('/profile', protect, getProfile);
router.put('/profile', protect, updateProfile);

router.post('/avatar', protect, upload.single('avatar'), uploadAvatar);

export default router;