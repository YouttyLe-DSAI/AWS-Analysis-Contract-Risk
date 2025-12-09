import express from 'express';
import cors from 'cors';
import dotenv from 'dotenv';

// Import các routes
import contractRoutes from './routes/contractRoutes';
import adminRoutes from './routes/adminRoutes';
import newsRoutes from './routes/newsRoutes'; 
import authRoutes from './routes/authRoutes';

dotenv.config();

const app = express();

app.use(express.json({ limit: '50mb' }));
app.use(express.urlencoded({ limit: '50mb', extended: true }));
app.use(cors({ 
    origin: '*', 
    credentials: true,
    methods: ['GET', 'POST', 'PUT', 'DELETE', 'OPTIONS'],
    allowedHeaders: ['Content-Type', 'Authorization']
}));


app.get('/', (req, res) => {
  res.json({ message: "AI Contract Backend is running!" });
});

const apiRouter = express.Router();

apiRouter.use('/contracts', contractRoutes);
apiRouter.use('/admin', adminRoutes);
apiRouter.use('/news', newsRoutes);
apiRouter.use('/auth', authRoutes); 
app.use('/api', apiRouter);
app.use('/dev/api', apiRouter); 
app.use('/prod/api', apiRouter); 

export default app;